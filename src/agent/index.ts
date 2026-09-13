import type { Bus, PublishOpts } from '../bus/index.js'
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
  DLQ_CONSUME,
  MailboxMessageSchema,
  type ObjectRef,
  ToolResultSchema,
} from '../protocol/index.js'
import { escapeKVSegment } from '../protocol/kv-escaping.js'
import {
  GLOBAL_TENANT,
  subjectTenant,
  tenantKVKey,
  tenantObjectName,
} from '../protocol/tenant.js'
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
  tenant: string
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

/**
 * Tenant-scoped config KV key. Mirrors the extension SDK's key layout:
 *   global  -> t.<tenant>.<extId>.<name>
 *   session -> t.<tenant>.<extId>.<escapedSession>.<name>
 */
function kvKey(
  tenant: string,
  extId: string,
  scope: string,
  sessionName: string,
  name: string,
): string {
  const rest =
    scope === 'session'
      ? `${extId}.${escapeKVSegment(sessionName)}.${name}`
      : `${extId}.${name}`
  return tenantKVKey(tenant, rest)
}

/**
 * Agent-side config authority. Values live in memory (serving snapshot reqs),
 * mirror into the KV `cfg` bucket when the transport supports KV, and are
 * delivered to extensions as 1:1 `req`s with an optional ack. State is keyed
 * by tenant → extId → config name.
 */
class ConfigAuthority {
  /** tenant -> extId -> declared items (from the last discovered manifest). */
  private declarations = new Map<string, Map<string, ExtensionConfigItem[]>>()
  /** tenant -> extId -> (config name -> [revision, value]). */
  private global = new Map<string, Map<string, Map<string, [number, unknown]>>>()
  /** tenant -> extId -> (session -> (name -> [revision, value])). */
  private sessions = new Map<
    string,
    Map<string, Map<string, Map<string, [number, unknown]>>>
  >()
  private unsub?: () => Promise<void>

  constructor(
    private readonly bus: Bus,
    private readonly defaultAck: boolean,
  ) {}

  async start(): Promise<void> {
    for (const [tenant, extMap] of this.declarations) {
      for (const extId of extMap.keys()) {
        await this.recover(tenant, extId)
      }
    }
    // Snapshot serving moved to the cfg KV bucket (0.2): extensions
    // recover state by reading/watching it; no abc.config.get serving.
    this.unsub = async () => {}
  }

  async stop(): Promise<void> {
    await this.unsub?.()
  }

  /** Record declarations from a manifest; recovers persisted state once. */
  declare(tenant: string, manifest: ExtensionManifest): void {
    let extMap = this.declarations.get(tenant)
    if (extMap === undefined) {
      extMap = new Map()
      this.declarations.set(tenant, extMap)
    }
    extMap.set(manifest.id, manifest.config ?? [])
    void this.recover(tenant, manifest.id)
  }

  private async recover(tenant: string, extId: string): Promise<void> {
    const items = this.declarations.get(tenant)?.get(extId) ?? []
    for (const item of items) {
      const raw = await this.bus.kvGet(
        CONFIG_KV_BUCKET,
        kvKey(tenant, extId, 'global', '', item.name),
      )
      if (raw !== null) {
        // envelope {r, v} with a bare-value fallback (pre-0.2 entries)
        const parsed = JSON.parse(raw) as { r?: number; v?: unknown }
        if (parsed !== null && typeof parsed === 'object' && 'v' in parsed) {
          this.globalFor(tenant, extId).set(item.name, [
            Number(parsed.r ?? 0),
            parsed.v,
          ])
        } else {
          this.globalFor(tenant, extId).set(item.name, [0, parsed])
        }
      }
    }
    // Session overrides are recovered lazily via kvList-like pattern; v1
    // keeps recovery global-only (documented) because KV lacks listing here.
  }

  private globalFor(
    tenant: string,
    extId: string,
  ): Map<string, [number, unknown]> {
    let extMap = this.global.get(tenant)
    if (extMap === undefined) {
      extMap = new Map()
      this.global.set(tenant, extMap)
    }
    let m = extMap.get(extId)
    if (m === undefined) {
      m = new Map()
      extMap.set(extId, m)
    }
    return m
  }

  private sessionsFor(
    tenant: string,
    extId: string,
  ): Map<string, Map<string, [number, unknown]>> {
    let extMap = this.sessions.get(tenant)
    if (extMap === undefined) {
      extMap = new Map()
      this.sessions.set(tenant, extMap)
    }
    let m = extMap.get(extId)
    if (m === undefined) {
      m = new Map()
      extMap.set(extId, m)
    }
    return m
  }

  private snapshot(tenant: string, extId: string): z_infer_ConfigSnapshot {
    const g: Record<string, unknown> = {}
    for (const [name, [, value]] of this.globalFor(tenant, extId)) g[name] = value
    const sessions: Record<string, Record<string, unknown>> = {}
    for (const [sess, vals] of this.sessionsFor(tenant, extId)) {
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
    tenant: string,
    name: string,
    value: unknown,
    sessionName?: string,
    ack?: boolean,
  ): Promise<void> {
    const items = this.declarations.get(tenant)?.get(manifest.id)
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

    // Bump revision (per tenant/ext/scope/session/name key).
    let revision: number
    if (scope === 'global') {
      const m = this.globalFor(tenant, manifest.id)
      const prev = m.get(name)?.[0] ?? 0
      revision = prev + 1
      m.set(name, [revision, value])
    } else {
      const sess = this.sessionsFor(tenant, manifest.id)
      const vals = sess.get(sessionName as string) ?? new Map()
      const prev = vals.get(name)?.[0] ?? 0
      revision = prev + 1
      vals.set(name, [revision, value])
      sess.set(sessionName as string, vals)
    }

    // Persist first (crash-safe): the cfg KV bucket is the source of truth;
    // the revision rides along so a restarted agent restores counters.
    await this.bus.kvPut(
      CONFIG_KV_BUCKET,
      kvKey(tenant, manifest.id, scope, sessionName ?? '', name),
      JSON.stringify({ r: revision, v: value }),
      0,
    )

    // Deliver as 1:1 req with optional ack.
    const reqOpts: import('../bus/index.js').RequestOpts = {
      timeoutMs: useAck ? 5000 : 300,
      tenant,
    }
    if (sessionName !== undefined) reqOpts.sessionName = sessionName
    const reply = await this.bus
      .request(
        CH.config(tenant, manifest.id),
        {
          name,
          value,
          revision,
          scope,
          tenant,
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
        const m = this.globalFor(tenant, manifest.id)
        const prev = m.get(name)
        if (prev !== undefined && prev[0] === revision) m.delete(name)
      } else {
        const vals = this.sessionsFor(tenant, manifest.id).get(
          sessionName as string,
        )
        const prev = vals?.get(name)
        if (prev !== undefined && prev[0] === revision) vals?.delete(name)
      }
      await this.bus
        .kvDelete(
          CONFIG_KV_BUCKET,
          kvKey(tenant, manifest.id, scope, sessionName ?? '', name),
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
  async dropSession(
    tenant: string,
    manifestId: string,
    sessionName: string,
  ): Promise<void> {
    const vals = this.sessionsFor(tenant, manifestId).get(sessionName)
    if (vals === undefined) return
    for (const name of vals.keys()) {
      await this.bus
        .kvDelete(
          CONFIG_KV_BUCKET,
          kvKey(tenant, manifestId, 'session', sessionName, name),
        )
        .catch(() => {})
    }
    this.sessionsFor(tenant, manifestId).delete(sessionName)
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
/** Default poison-message escalation: after this many nak redeliveries the
 * consumer terms the message into the dead-letter stream. */
export const DEFAULT_MAX_NAKS_BEFORE_TERM = 5

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
   * Set a tenant's config value on an extension. Validates against the
   * manifest declaration (so discover() must have run, or pass the manifest),
   * persists via the KV mirror, and delivers with an ack; a rejection from the
   * extension rolls the value back and throws ConfigRejected.
   */
  async setConfig(
    tenant: string,
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
    this.configAuthority?.declare(tenant, manifest)
    if (this.configAuthority === undefined) {
      this.configAuthority = new ConfigAuthority(this.bus, true)
      this.configAuthority.declare(tenant, manifest)
      await this.configAuthority.start()
    }
    await this.configAuthority.set(
      manifest,
      tenant,
      name,
      value,
      sessionName,
      opts.ack,
    )
  }

  /** Drop a tenant's per-session config overrides when a session ends. */
  async dropSessionConfig(
    tenant: string,
    extId: string,
    sessionName: string,
  ): Promise<void> {
    await this.configAuthority?.dropSession(tenant, extId, sessionName)
  }

  get rawBus(): Bus {
    return this.bus
  }

  async discover(maxWaitMs = 500): Promise<ExtensionManifest[]> {
    // Presence-first: extensions heartbeat manifests into the abc-presence
    // KV bucket; the watcher keeps the cache live (offline extensions drop
    // out via key TTL). Only a cold cache falls back to the broadcast.
    // Discovery is GLOBAL: every extension serves every tenant.
    await this.ensurePresence()
    if (this.manifestCache.size > 0) {
      return [...this.manifestCache.values()]
    }
    const replies = await this.bus.requestMany(CH.DISCOVER, {}, {
      maxWaitMs,
      tenant: GLOBAL_TENANT,
    })
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

  /** Whether a discovered extension advertises a protocol feature
   * (absent = pre-0.3 baseline). */
  extSupports(extId: string, feature: string): boolean {
    const m = this.manifestCache.get(extId)
    return m?.features?.includes(feature) ?? false
  }

  async callTool(
    tenant: string,
    sessionName: string,
    extId: string,
    tool: string,
    callId: string,
    args: Record<string, unknown>,
  ): Promise<ToolResultResolved> {
    const reply = await this.bus.request(
      CH.toolCall(tenant, extId, tool),
      { call_id: callId, arguments: args },
      { timeoutMs: 0, sessionName, tenant },
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
  async subscribeProgress(tenant: string, callId: string) {
    return this.bus.subscribe(CH.toolProgress(tenant, callId))
  }

  async resolveVariable(
    tenant: string,
    provider: string,
    name: string,
    sessionName?: string,
  ): Promise<string | null> {
    // KV-first: extensions cache resolved values in the vars bucket, so a
    // cached hit avoids the lazy resolver round trip entirely.
    if (sessionName !== undefined && sessionName !== '') {
      const cached = await this.bus
        .kvGet(VARS_BUCKET, sessionVarKey(tenant, provider, sessionName, name))
        .catch(() => null)
      if (cached !== null && cached !== '') return cached
    }
    const cachedGlobal = await this.bus
      .kvGet(VARS_BUCKET, varKey(tenant, provider, name))
      .catch(() => null)
    if (cachedGlobal !== null && cachedGlobal !== '') return cachedGlobal
    try {
      const reply = await this.bus.request(
        CH.variable(tenant, provider, name),
        { name },
        sessionName === undefined ? { tenant } : { sessionName, tenant },
      )
      const parsed = ExtensionVariableValueSchema.safeParse(reply.payload)
      return parsed.success ? parsed.data.value : null
    } catch {
      return null
    }
  }

  async publishMailbox(
    tenant: string,
    sessionName: string,
    type: string,
    payload: unknown,
  ): Promise<void> {
    const id = crypto.randomUUID()
    await this.bus.inboxPublish(
      CH.mailbox(tenant, sessionName),
      { id, type, payload },
      { id, sessionName, tenant },
    )
  }

  async consumeMailbox(
    handler: (msg: MailboxMessageResolved) => void | Promise<void>,
    maxNaksBeforeTerm = DEFAULT_MAX_NAKS_BEFORE_TERM,
    nakDelayMs = 5000,
  ): Promise<() => Promise<void>> {
    const sub = await this.bus.inboxConsume({ subject: MAILBOX_CONSUME })
    const naks = new Map<string, number>()
    void (async () => {
      for await (const msg of sub) {
        const parsed = MailboxMessageSchema.safeParse(msg.payload)
        if (!parsed.success) {
          await msg.ack()
          continue
        }
        const p = parsed.data
        const sessionName = msg.session_name ?? ''
        const tenant = msg.tenant ?? subjectTenant(msg.ch ?? '') ?? ''
        const id = typeof msg.id === 'string' ? msg.id : ''
        if (sessionName === '' || id === '' || tenant === '') {
          await msg.ack()
          continue
        }
        try {
          await handler({
            id,
            tenant,
            sessionName,
            type: p.type ?? 'event',
            payload: p.payload,
          })
          naks.delete(id)
          await msg.ack()
        } catch (err: unknown) {
          const e = err as Error | TermError
          if (e instanceof TermError) {
            naks.delete(id)
            if (e.noDLQ) await msg.termNoDLQ()
            else await msg.term()
            continue
          }
          const n = (naks.get(id) ?? 0) + 1
          naks.set(id, n)
          if (n >= maxNaksBeforeTerm) {
            // poison: park it in the DLQ instead of nak-looping forever
            naks.delete(id)
            await msg.term()
            continue
          }
          await msg.nak(nakDelayMs)
        }
      }
    })()
    return () => sub.close()
  }

  /** Requeue one dead-lettered message (by original id) onto its session's
   * mailbox with a fresh id, acking the dead-letter copy. The return path
   * for triage: fix the consumer, then requeue the parked payloads. */
  async requeueDLQ(id: string, timeoutMs = 10_000): Promise<boolean> {
    const sub = await this.bus.inboxConsume({ subject: DLQ_CONSUME })
    const deadline = Date.now() + timeoutMs
    try {
      for await (const msg of sub) {
        if (Date.now() >= deadline) return false
        const parsed = MailboxMessageSchema.safeParse(msg.payload)
        const sessionName = msg.session_name ?? ''
        const tenant = msg.tenant ?? subjectTenant(msg.ch ?? '') ?? ''
        const mid = typeof msg.id === 'string' ? msg.id : ''
        if (!parsed.success || sessionName === '' || mid !== id || tenant === '') {
          await msg.nak(500).catch(() => {})
          continue
        }
        await this.publishMailbox(
          tenant,
          sessionName,
          parsed.data.type ?? 'event',
          parsed.data.payload,
        )
        await msg.ack()
        return true
      }
    } catch {
      await sub.close()
    }
    return false
  }

  /** Fire a sync call-hook; returns false when it failed. */
  async callHook(
    tenant: string,
    sessionName: string,
    extId: string,
    hook: string,
    args?: Record<string, unknown>,
  ): Promise<{
    ok: boolean
    error?: { code: string; message: string }
    data?: unknown
  }> {
    const reply = await this.bus.request(
      CH.hookCall(tenant, extId, hook),
      {
        hook,
        tenant,
        session_name: sessionName,
        arguments: args,
      },
      { tenant },
    )
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
    tenant: string,
    sessionName: string,
    hook: string,
    payload?: unknown,
  ): Promise<void> {
    const ev: HookEvent = { hook, session_name: sessionName, payload }
    await this.bus.publish(CH.hookEvent(tenant, hook), ev, { tenant })
  }

  /** Ask an extension to interrupt in-flight work for a session. */
  async interrupt(
    tenant: string,
    extId: string,
    sessionName?: string,
    reason?: string,
  ): Promise<void> {
    const sig: InterruptSignal = {}
    if (sessionName !== undefined) sig.session_name = sessionName
    if (reason !== undefined) sig.reason = reason
    await this.bus.publish(CH.interrupt(tenant, extId), sig, { tenant })
  }

  /**
   * Announce a session lifecycle change on
   * abc.<tenant>.session.lifecycle.<kind> (created / forked / renamed /
   * deleted). forked carries parent, renamed carries from/to. Extensions that
   * declared the kind receive it; on "deleted" this also drops the session's
   * config overrides for every known extension.
   */
  async publishLifecycleEvent(
    tenant: string,
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
      tenant,
      session_name: sessionName,
    }
    if (kind === 'forked' && opts.parent !== undefined)
      body.parent = opts.parent
    if (kind === 'renamed') {
      if (opts.from !== undefined) body.from = opts.from
      if (opts.to !== undefined) body.to = opts.to
    }
    if (opts.payload !== undefined) body.payload = opts.payload
    await this.bus.publish(CH.lifecycle(tenant, kind), body, { tenant })
    if (kind === 'deleted') {
      for (const extId of this.manifestCache.keys()) {
        await this.dropSessionConfig(tenant, extId, sessionName).catch(() => {})
      }
    }
  }

  async consumeDLQ(
    handler: (msg: MailboxMessageResolved) => void | Promise<void>,
  ): Promise<() => Promise<void>> {
    const sub = await this.bus.inboxConsume({ subject: DLQ_CONSUME })
    void (async () => {
      for await (const msg of sub) {
        const parsed = MailboxMessageSchema.safeParse(msg.payload)
        const sessionName = msg.session_name ?? ''
        const tenant = msg.tenant ?? subjectTenant(msg.ch ?? '') ?? ''
        if (!parsed.success || sessionName === '' || tenant === '') {
          await msg.termNoDLQ()
          continue
        }
        try {
          await handler({
            id: msg.id ?? '',
            tenant,
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

  async putObject(
    tenant: string,
    name: string,
    data: Uint8Array,
  ): Promise<void> {
    return this.bus.objectPut(tenantObjectName(tenant, name), data)
  }

  async getObject(tenant: string, name: string): Promise<Uint8Array | null> {
    return this.bus.objectGet(tenantObjectName(tenant, name))
  }

  /**
   * Stream a session's events (abc.<tenant>.session.events.<token>) over ONE
   * ordered subscription: first the retained history from `startTimeMs` (or
   * from now when omitted), then live events — no polling, no replay/live
   * handover race. Yields raw `{event, params?, eid?}` items.
   *
   * CATCH-UP COALESCING: while the consumer is behind (`envelope.pending > 0`)
   * consecutive deltas of the same part (`reasoning-delta` / `text-delta`, same
   * run_id + part id) are merged into a single event, so a long turn's tens of
   * thousands of token deltas replay as one (or a few) events instead of
   * thousands. Structural events (`*-start/end`, `step-start`, `tool-*`,
   * `turn-complete`) force a flush first. Once caught up (`pending == 0`) the
   * deltas are streamed one-by-one so live typing stays incremental.
   */
  async *streamEvents(
    tenant: string,
    sessionName: string,
    opts?: { startTimeMs?: number },
  ): AsyncGenerator<{ event: string; params?: unknown; eid?: string }> {
    const sub = await this.bus.subscribeStream(
      CH.sessionEvents(tenant, sessionName),
      opts,
    )
    const isDelta = (e: string) => e === 'reasoning-delta' || e === 'text-delta'
    const buffer = new Map<
      string,
      { event: string; params: Record<string, unknown>; eid?: string }
    >()
    const order: string[] = []
    const flush = function* () {
      for (const k of order) {
        const v = buffer.get(k)
        if (v !== undefined) yield v
      }
      buffer.clear()
      order.length = 0
    }
    try {
      for await (const env of sub) {
        const p = env.payload as {
          event?: string
          params?: unknown
          eid?: string
        }
        if (typeof p?.event !== 'string') continue
        const event = p.event
        const params = (
          p.params !== null && typeof p.params === 'object' ? p.params : {}
        ) as Record<string, unknown>
        const live = env.pending === 0
        if (isDelta(event)) {
          if (live && buffer.size === 0) {
            const item: { event: string; params?: unknown; eid?: string } = {
              event,
              params,
            }
            if (p.eid !== undefined) item.eid = p.eid
            yield item
            continue
          }
          const key = `${String(params.run_id ?? '')}|${event}|${String(
            params.id ?? '',
          )}`
          const existing = buffer.get(key)
          if (existing === undefined) {
            const item: { event: string; params: Record<string, unknown>; eid?: string } = {
              event,
              params,
            }
            if (p.eid !== undefined) item.eid = p.eid
            buffer.set(key, item)
            order.push(key)
          } else {
            existing.params = {
              ...existing.params,
              text: `${String(existing.params.text ?? '')}${String(
                params.text ?? '',
              )}`,
            }
          }
          if (live) yield* flush()
          continue
        }
        // Structural event: emit buffered deltas first, then the event itself.
        if (buffer.size > 0) yield* flush()
        const item: { event: string; params?: unknown; eid?: string } = {
          event,
          params,
        }
        if (p.eid !== undefined) item.eid = p.eid
        yield item
      }
    } finally {
      await sub.close().catch(() => {})
    }
  }

  async close(): Promise<void> {
    for (const stop of this.presenceStops) await stop().catch(() => {})
    this.presenceStops = []
    return this.bus.close()
  }
}
