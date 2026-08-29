import type { Bus } from '../bus/index.js'
import {
  PRESENCE_BUCKET,
  sessionVarKey,
  VARS_BUCKET,
  varKey,
} from '../extension/index.js'
import {
  CH,
  ConfigSetSchema,
  ConfigSnapshotSchema,
  type ExtensionConfigItem,
  type ExtensionManifest,
  ExtensionManifestSchema,
  ExtensionVariableValueSchema,
  type HookEvent,
  HookResponseSchema,
  type InterruptSignal,
  MAILBOX_CONSUME,
  MailboxMessageSchema,
  type ObjectRef,
  ToolResultSchema,
} from '../protocol/index.js'
import { type AgentConnect, connectBus } from '../transport/index.js'

export interface ToolResultResolved {
  content?: string
  data?: unknown
  object?: ObjectRef
  error?: { code: string; message: string }
  metadata?: unknown
}

export interface MailboxMessageResolved {
  id: string
  sessionName: string
  type: string
  payload?: unknown
}

export interface ConfigError {
  code: 'invalid_argument' | 'not_found' | 'retryable'
  message: string
}

/** Options for Agent.serveConfig(). */
export interface ServeConfigOptions {
  /** Fire-and-forget deliveries skip the extension ack round-trip. */
  defaultAck?: boolean
}

const CONFIG_KV_BUCKET = 'cfg'

function kvKey(
  extId: string,
  scope: string,
  sessionName: string,
  name: string,
): string {
  return scope === 'session'
    ? `${extId}.${sessionName}.${name}`
    : `${extId}.${name}`
}

/**
 * Agent-side config authority. Values live in memory (serving snapshot reqs),
 * mirror into the KV `cfg` bucket when the transport supports KV, and are
 * delivered to extensions as 1:1 `req`s with an optional ack.
 */
class ConfigAuthority {
  /** extId -> declared items (from the last discovered manifest). */
  private declarations = new Map<string, ExtensionConfigItem[]>()
  /** extId -> (config name -> [revision, value]). */
  private global = new Map<string, Map<string, [number, unknown]>>()
  /** extId -> (session -> (name -> [revision, value])). */
  private sessions = new Map<
    string,
    Map<string, Map<string, [number, unknown]>>
  >()
  private unsub?: () => Promise<void>

  constructor(
    private readonly bus: Bus,
    private readonly defaultAck: boolean,
  ) {}

  async start(): Promise<void> {
    // Recover persisted values.
    for (const extId of this.declarations.keys()) {
      await this.recover(extId)
    }
    // Snapshot serving moved to the cfg KV bucket (0.2): extensions
    // recover state by reading/watching it; no abc.config.get serving.
    this.unsub = async () => {}
  }

  async stop(): Promise<void> {
    await this.unsub?.()
  }

  /** Record declarations from a manifest; recovers persisted state once. */
  declare(manifest: ExtensionManifest): void {
    this.declarations.set(manifest.id, manifest.config ?? [])
    void this.recover(manifest.id)
  }

  private async recover(extId: string): Promise<void> {
    const items = this.declarations.get(extId) ?? []
    for (const item of items) {
      const raw = await this.bus.kvGet(
        CONFIG_KV_BUCKET,
        kvKey(extId, 'global', '', item.name),
      )
      if (raw !== null) {
        // envelope {r, v} with a bare-value fallback (pre-0.2 entries)
        const parsed = JSON.parse(raw) as { r?: number; v?: unknown }
        if (parsed !== null && typeof parsed === 'object' && 'v' in parsed) {
          this.global
            .get(extId)
            ?.set(item.name, [Number(parsed.r ?? 0), parsed.v])
        } else {
          this.global.get(extId)?.set(item.name, [0, parsed])
        }
      }
    }
    // Session overrides are recovered lazily via kvList-like pattern; v1
    // keeps recovery global-only (documented) because KV lacks listing here.
  }

  private snapshot(extId: string): z_infer_ConfigSnapshot {
    const g: Record<string, unknown> = {}
    for (const [name, [, value]] of this.global.get(extId) ?? [])
      g[name] = value
    const sessions: Record<string, Record<string, unknown>> = {}
    for (const [sess, vals] of this.sessions.get(extId) ?? []) {
      const rec: Record<string, unknown> = {}
      for (const [name, [, value]] of vals) rec[name] = value
      sessions[sess] = rec
    }
    return { global: g, sessions }
  }

  /**
   * Validate against the manifest declaration, then deliver. Throws
   * ConfigRejected when the extension refuses (ack path) or the declaration
   * is violated.
   */
  async set(
    manifest: ExtensionManifest,
    name: string,
    value: unknown,
    sessionName?: string,
    ack?: boolean,
  ): Promise<void> {
    const items = this.declarations.get(manifest.id)
    const item = items?.find(c => c.name === name)
    if (item === undefined) {
      const err: ConfigError = {
        code: 'not_found',
        message: `config ${name} not declared by ${manifest.id}`,
      }
      throw err
    }
    const scope = item.scope
    if (scope === 'session' && sessionName === undefined) {
      throw {
        code: 'invalid_argument',
        message: `config ${name} requires a sessionName (scope=session)`,
      } satisfies ConfigError
    }
    const vErr = validateValue(item, value)
    if (vErr !== null)
      throw { code: 'invalid_argument', message: vErr } satisfies ConfigError

    const useAck = ack ?? this.defaultAck ?? true

    // Bump revision (per ext/scope/session/name key).
    let revision: number
    if (scope === 'global') {
      const m = this.global.get(manifest.id) ?? new Map()
      const prev = m.get(name)?.[0] ?? 0
      revision = prev + 1
      m.set(name, [revision, value])
      this.global.set(manifest.id, m)
    } else {
      const sess = this.sessions.get(manifest.id) ?? new Map()
      const vals = sess.get(sessionName as string) ?? new Map()
      const prev = vals.get(name)?.[0] ?? 0
      revision = prev + 1
      vals.set(name, [revision, value])
      sess.set(sessionName as string, vals)
      this.sessions.set(manifest.id, sess)
    }

    // Persist first (crash-safe): the cfg KV bucket is the source of truth;
    // the revision rides along so a restarted agent restores counters.
    await this.bus.kvPut(
      CONFIG_KV_BUCKET,
      kvKey(manifest.id, scope, sessionName ?? '', name),
      JSON.stringify({ r: revision, v: value }),
      0,
    )

    // Deliver as 1:1 req with optional ack.
    const reqOpts: import('../bus/index.js').RequestOpts = {
      timeoutMs: useAck ? 5000 : 300,
    }
    if (sessionName !== undefined) reqOpts.sessionName = sessionName
    const reply = await this.bus
      .request(
        CH.config(manifest.id),
        {
          name,
          value,
          revision,
          scope,
          ...(sessionName !== undefined ? { session_name: sessionName } : {}),
          ack: useAck,
        },
        reqOpts,
      )
      .catch(() => null)
    if (!useAck) return
    if (reply === null) {
      // Delivery is best-effort in the 0.2 model: the value is committed to
      // the cfg KV bucket; an offline extension recovers via its KV watch.
      return
    }
    const parsed = HookResponseSchema.safeParse(reply.payload)
    if (parsed.success && !parsed.data.ok) {
      // Roll back memory + KV.
      if (scope === 'global') {
        const m = this.global.get(manifest.id)
        const prev = m?.get(name)
        if (prev !== undefined && prev[0] === revision) m?.delete(name)
      } else {
        const vals = this.sessions.get(manifest.id)?.get(sessionName as string)
        const prev = vals?.get(name)
        if (prev !== undefined && prev[0] === revision) vals?.delete(name)
      }
      await this.bus
        .kvDelete(
          CONFIG_KV_BUCKET,
          kvKey(manifest.id, scope, sessionName ?? '', name),
        )
        .catch(() => {})
      throw {
        code: 'retryable',
        message:
          parsed.data.error?.message ?? 'extension rejected the config change',
      } satisfies ConfigError
    }
  }

  /** Drop session overrides when a session ends. */
  async dropSession(manifestId: string, sessionName: string): Promise<void> {
    const vals = this.sessions.get(manifestId)?.get(sessionName)
    if (vals === undefined) return
    for (const name of vals.keys()) {
      await this.bus
        .kvDelete(
          CONFIG_KV_BUCKET,
          kvKey(manifestId, 'session', sessionName, name),
        )
        .catch(() => {})
    }
    this.sessions.get(manifestId)?.delete(sessionName)
  }
}

function validateValue(
  item: ExtensionConfigItem,
  value: unknown,
): string | null {
  switch (item.type) {
    case 'string':
      return typeof value === 'string'
        ? null
        : `expected string, got ${typeof value}`
    case 'number':
      return typeof value === 'number'
        ? null
        : `expected number, got ${typeof value}`
    case 'boolean':
      return typeof value === 'boolean'
        ? null
        : `expected boolean, got ${typeof value}`
    case 'enum':
      if (typeof value !== 'string')
        return `expected enum string, got ${typeof value}`
      if (item.enum_values !== undefined && !item.enum_values.includes(value)) {
        return `value ${value} not in [${item.enum_values.join(', ')}]`
      }
      return null
    case 'json':
      return null
    default:
      return `unknown type ${String(item.type)}`
  }
}

type z_infer_ConfigSnapshot = {
  global: Record<string, unknown>
  sessions: Record<string, Record<string, unknown>>
}
void ConfigSnapshotSchema
void ConfigSetSchema

/** Returned by a mailbox handler to terminate a message: delivery stops
 * and (unless noDLQ) the message is copied to the dead-letter stream. */
export class TermError extends Error {
  constructor(readonly noDLQ = false) {
    super('terminated by consumer')
  }
}

export class Agent {
  /** Transport ownership handle (inproc/ws hub) when Agent.connect started it. */
  hub?: unknown
  private configAuthority?: ConfigAuthority
  private manifestCache = new Map<string, ExtensionManifest>()

  constructor(private readonly bus: Bus) {}

  /** Wire the NATS transport and return a ready Agent. */
  static async connect(opts: AgentConnect): Promise<Agent> {
    const { bus } = await connectBus(opts)
    return new Agent(bus)
  }

  /**
   * Turn this agent into the config authority: it serves startup snapshot
   * requests, persists values into the KV `cfg` bucket, and
   * delivers validated changes to extensions. Call once before setConfig.
   */
  async serveConfig(opts: ServeConfigOptions = {}): Promise<void> {
    if (this.configAuthority === undefined) {
      this.configAuthority = new ConfigAuthority(
        this.bus,
        opts.defaultAck ?? true,
      )
      await this.configAuthority.start()
    }
  }

  /**
   * Set a config value on an extension. Validates against the manifest
   * declaration (so discover() must have run, or pass the manifest), persists
   * via the KV mirror, and delivers with an ack; a rejection from the
   * extension rolls the value back and throws ConfigRejected.
   */
  async setConfig(
    extId: string,
    name: string,
    value: unknown,
    sessionName?: string,
    opts: { ack?: boolean; manifest?: ExtensionManifest } = {},
  ): Promise<void> {
    const manifest = opts.manifest ?? this.manifestCache.get(extId)
    if (manifest === undefined) {
      throw {
        code: 'not_found',
        message: `no manifest for ${extId}; run discover() first`,
      } satisfies ConfigError
    }
    this.configAuthority?.declare(manifest)
    if (this.configAuthority === undefined) {
      this.configAuthority = new ConfigAuthority(this.bus, true)
      this.configAuthority.declare(manifest)
      await this.configAuthority.start()
    }
    await this.configAuthority.set(manifest, name, value, sessionName, opts.ack)
  }

  /** Drop per-session config overrides when a session ends. */
  async dropSessionConfig(extId: string, sessionName: string): Promise<void> {
    await this.configAuthority?.dropSession(extId, sessionName)
  }

  get rawBus(): Bus {
    return this.bus
  }

  async discover(maxWaitMs = 500): Promise<ExtensionManifest[]> {
    // Presence-first: extensions heartbeat manifests into the abc-presence
    // KV bucket; the watcher keeps the cache live (offline extensions drop
    // out via key TTL). Only a cold cache falls back to the broadcast.
    await this.ensurePresence()
    if (this.manifestCache.size > 0) {
      return [...this.manifestCache.values()]
    }
    const replies = await this.bus.requestMany(CH.DISCOVER, {}, { maxWaitMs })
    const out: ExtensionManifest[] = []
    const seen = new Set<string>()
    for (const r of replies) {
      const m = ExtensionManifestSchema.safeParse(r.payload)
      if (!m.success || seen.has(m.data.id)) continue
      seen.add(m.data.id)
      this.manifestCache.set(m.data.id, m.data)
      out.push(m.data)
    }
    return out
  }

  private presenceStarted = false
  private presenceStops: Array<() => Promise<void>> = []

  private async ensurePresence(): Promise<void> {
    if (this.presenceStarted) return
    this.presenceStarted = true
    try {
      const { stream, stop } = await this.bus.kvWatch(PRESENCE_BUCKET, '>')
      this.presenceStops.push(async () => {
        await stop()
      })
      void (async () => {
        for await (const ev of stream) {
          if (ev.deleted) {
            this.manifestCache.delete(ev.key)
            continue
          }
          const m = ExtensionManifestSchema.safeParse(JSON.parse(ev.value))
          if (m.success) this.manifestCache.set(m.data.id, m.data)
        }
      })()
    } catch {
      this.presenceStarted = false // retry on the next discover
    }
  }

  async callTool(
    sessionName: string,
    extId: string,
    tool: string,
    callId: string,
    args: Record<string, unknown>,
  ): Promise<ToolResultResolved> {
    const reply = await this.bus.request(
      CH.toolCall(extId, tool),
      { call_id: callId, arguments: args },
      { timeoutMs: 0, sessionName },
    )
    const parsed = ToolResultSchema.safeParse(reply.payload)
    if (!parsed.success) return {}
    const p = parsed.data
    const out: ToolResultResolved = {}
    if (p.content !== undefined) out.content = p.content
    if (p.data !== undefined) out.data = p.data
    if (p.object !== undefined) out.object = p.object
    if (p.error !== undefined) out.error = p.error
    if (p.metadata !== undefined) out.metadata = p.metadata
    return out
  }

  /**
   * Subscribe to in-flight progress telemetry for a specific tool call. The
   * returned subscription yields progress envelopes; the orchestration layer
   * uses these for liveness/UI, never for the LLM context.
   */
  async subscribeProgress(callId: string) {
    return this.bus.subscribe(CH.toolProgress(callId))
  }

  async resolveVariable(
    provider: string,
    name: string,
    sessionName?: string,
  ): Promise<string | null> {
    // KV-first: extensions cache resolved values in the vars bucket, so a
    // cached hit avoids the lazy resolver round trip entirely.
    if (sessionName !== undefined && sessionName !== '') {
      const cached = await this.bus
        .kvGet(VARS_BUCKET, sessionVarKey(provider, sessionName, name))
        .catch(() => null)
      if (cached !== null && cached !== '') return cached
    }
    const cachedGlobal = await this.bus
      .kvGet(VARS_BUCKET, varKey(provider, name))
      .catch(() => null)
    if (cachedGlobal !== null && cachedGlobal !== '') return cachedGlobal
    try {
      const reply = await this.bus.request(
        CH.variable(provider, name),
        { name },
        sessionName === undefined ? {} : { sessionName },
      )
      const parsed = ExtensionVariableValueSchema.safeParse(reply.payload)
      return parsed.success ? parsed.data.value : null
    } catch {
      return null
    }
  }

  async publishMailbox(
    sessionName: string,
    type: string,
    payload: unknown,
  ): Promise<void> {
    const id = crypto.randomUUID()
    await this.bus.inboxPublish(
      CH.mailbox(sessionName),
      { id, type, payload },
      { id, sessionName },
    )
  }

  async consumeMailbox(
    handler: (msg: MailboxMessageResolved) => void | Promise<void>,
  ): Promise<() => Promise<void>> {
    const sub = await this.bus.inboxConsume({ subject: MAILBOX_CONSUME })
    void (async () => {
      for await (const msg of sub) {
        const parsed = MailboxMessageSchema.safeParse(msg.payload)
        if (!parsed.success) {
          await msg.ack()
          continue
        }
        const p = parsed.data
        const sessionName = msg.session_name ?? ''
        if (sessionName === '' || typeof msg.id !== 'string' || msg.id === '') {
          await msg.ack()
          continue
        }
        try {
          await handler({
            id: msg.id,
            sessionName,
            type: p.type ?? 'event',
            payload: p.payload,
          })
          await msg.ack()
        } catch (err: unknown) {
          const e = err as Error | TermError
          if (e instanceof TermError) {
            if (e.noDLQ) await msg.termNoDLQ()
            else await msg.term()
            continue
          }
          await msg.nak(5000)
        }
      }
    })()
    return () => sub.close()
  }

  /** Fire a sync call-hook; returns false when it failed. */
  async callHook(
    sessionName: string,
    extId: string,
    hook: string,
    args?: Record<string, unknown>,
  ): Promise<{
    ok: boolean
    error?: { code: string; message: string }
    data?: unknown
  }> {
    const reply = await this.bus.request(CH.hookCall(extId, hook), {
      hook,
      session_name: sessionName,
      arguments: args,
    })
    const parsed = HookResponseSchema.safeParse(reply.payload)
    if (!parsed.success) {
      return { ok: false, error: { code: 'internal', message: 'no reply' } }
    }
    const out: {
      ok: boolean
      error?: { code: string; message: string }
      data?: unknown
    } = { ok: parsed.data.ok }
    if (parsed.data.error !== undefined) out.error = parsed.data.error
    if (parsed.data.data !== undefined) out.data = parsed.data.data
    return out
  }

  /** Fire an async event-hook (best-effort). */
  async publishEventHook(
    sessionName: string,
    hook: string,
    payload?: unknown,
  ): Promise<void> {
    const ev: HookEvent = { hook, session_name: sessionName, payload }
    await this.bus.publish(CH.hookEvent(hook), ev)
  }

  /** Ask an extension to interrupt in-flight work for a session. */
  async interrupt(
    extId: string,
    sessionName?: string,
    reason?: string,
  ): Promise<void> {
    const sig: InterruptSignal = {}
    if (sessionName !== undefined) sig.session_name = sessionName
    if (reason !== undefined) sig.reason = reason
    await this.bus.publish(CH.interrupt(extId), sig)
  }

  /**
   * Announce a session lifecycle change on abc.session.lifecycle.<kind>
   * (created / forked / renamed / deleted). forked carries parent, renamed
   * carries from/to. Extensions that declared the kind receive it; on
   * "deleted" this also drops the session's config overrides for every
   * known extension.
   */
  async publishLifecycleEvent(
    kind: 'created' | 'forked' | 'renamed' | 'deleted',
    sessionName: string,
    opts: {
      parent?: string
      from?: string
      to?: string
      payload?: unknown
    } = {},
  ): Promise<void> {
    const body: Record<string, unknown> = {
      kind,
      session_name: sessionName,
    }
    if (kind === 'forked' && opts.parent !== undefined)
      body.parent = opts.parent
    if (kind === 'renamed') {
      if (opts.from !== undefined) body.from = opts.from
      if (opts.to !== undefined) body.to = opts.to
    }
    if (opts.payload !== undefined) body.payload = opts.payload
    await this.bus.publish(`abc.session.lifecycle.${kind}`, body)
    if (kind === 'deleted') {
      for (const extId of this.manifestCache.keys()) {
        await this.dropSessionConfig(extId, sessionName).catch(() => {})
      }
    }
  }

  async consumeDLQ(
    handler: (msg: MailboxMessageResolved) => void | Promise<void>,
  ): Promise<() => Promise<void>> {
    const sub = await this.bus.inboxConsume({ subject: 'abc.dlq.>' })
    void (async () => {
      for await (const msg of sub) {
        const parsed = MailboxMessageSchema.safeParse(msg.payload)
        const sessionName = msg.session_name ?? ''
        if (!parsed.success || sessionName === '') {
          await msg.termNoDLQ()
          continue
        }
        try {
          await handler({
            id: msg.id ?? '',
            sessionName,
            type: parsed.data.type ?? 'event',
            payload: parsed.data.payload,
          })
          await msg.ack()
        } catch {
          await msg.nak(5000)
        }
      }
    })()
    return () => sub.close()
  }

  async putObject(name: string, data: Uint8Array): Promise<void> {
    return this.bus.objectPut(name, data)
  }

  async getObject(name: string): Promise<Uint8Array | null> {
    return this.bus.objectGet(name)
  }

  /**
   * Replay the retained session events for a session (abc.session.events.
   * <token>), oldest first. Returns the raw `{event, params?, eid?}` items;
   * replay window/retention is a channel property (24h on NATS), and how
   * much of the replay to show is the consumer's policy.
   */
  async replayEvents(
    sessionName: string,
  ): Promise<Array<{ event: string; params?: unknown; eid?: string }>> {
    const out: Array<{ event: string; params?: unknown; eid?: string }> = []
    const envelopes = await this.bus.replay(CH.sessionEvents(sessionName))
    for (const env of envelopes) {
      const p = env.payload as {
        event?: string
        params?: unknown
        eid?: string
      }
      if (typeof p?.event === 'string') {
        const item: { event: string; params?: unknown; eid?: string } = {
          event: p.event,
        }
        if (p.params !== undefined) item.params = p.params
        if (p.eid !== undefined) item.eid = p.eid
        out.push(item)
      }
    }
    return out
  }

  async close(): Promise<void> {
    for (const stop of this.presenceStops) await stop().catch(() => {})
    this.presenceStops = []
    return this.bus.close()
  }
}
