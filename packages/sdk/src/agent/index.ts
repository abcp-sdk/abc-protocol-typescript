import type { Bus } from '../bus/index.js'
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
import { VARS_BUCKET, sessionVarKey, varKey } from '../extension/index.js'
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
    // Recover persisted values (caps.kv transports).
    if (this.bus.caps.kv) {
      for (const extId of this.declarations.keys()) {
        await this.recover(extId)
      }
    }
    // Serve snapshot requests for every declared extension.
    const sub = await this.bus.subscribe(CH.configWildcard())
    void (async () => {
      for await (const env of sub) {
        const extId = env.ch.slice('abc.config.get.'.length)
        if (env.reply_to === undefined) continue
        const snap = this.snapshot(extId)
        await this.bus.publish(env.reply_to, snap)
      }
    })()
    this.unsub = async () => {
      await sub.close()
    }
  }

  async stop(): Promise<void> {
    await this.unsub?.()
  }

  /** Record declarations from a manifest; recovers persisted state once. */
  declare(manifest: ExtensionManifest): void {
    this.declarations.set(manifest.id, manifest.config ?? [])
    if (this.bus.caps.kv) {
      void this.recover(manifest.id)
    }
  }

  private async recover(extId: string): Promise<void> {
    const items = this.declarations.get(extId) ?? []
    for (const item of items) {
      const raw = await this.bus.kvGet(
        CONFIG_KV_BUCKET,
        kvKey(extId, 'global', '', item.name),
      )
      if (raw !== null) {
        try {
          this.global.get(extId)?.set(item.name, [0, JSON.parse(raw)])
        } catch {
          /* ignore malformed */
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

    // Persist first (crash-safe): KV mirror, when supported.
    if (this.bus.caps.kv) {
      await this.bus.kvPut(
        CONFIG_KV_BUCKET,
        kvKey(manifest.id, scope, sessionName ?? '', name),
        JSON.stringify(value),
        0,
      )
    }

    // Deliver as 1:1 req with optional ack.
    const reqOpts: import('../bus/index.js').RequestOpts = {
      timeoutMs: useAck ? 5000 : 300,
    }
    if (sessionName !== undefined) reqOpts.sessionName = sessionName
    const reply = await this.bus.request(
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
    ).catch(() => null)
    if (!useAck) return
    if (reply === null) {
      throw {
        code: 'retryable',
        message: 'extension did not answer the config change',
      } satisfies ConfigError
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
      if (this.bus.caps.kv) {
        await this.bus
          .kvDelete(
            CONFIG_KV_BUCKET,
            kvKey(manifest.id, scope, sessionName ?? '', name),
          )
          .catch(() => {})
      }
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
    if (this.bus.caps.kv) {
      for (const name of vals.keys()) {
        await this.bus
          .kvDelete(
            CONFIG_KV_BUCKET,
            kvKey(manifestId, 'session', sessionName, name),
          )
          .catch(() => {})
      }
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

export class Agent {
  /** Transport ownership handle (inproc/ws hub) when Agent.connect started it. */
  hub?: unknown
  private closeHandle?: () => void | Promise<void>
  private configAuthority?: ConfigAuthority
  private manifestCache = new Map<string, ExtensionManifest>()

  constructor(private readonly bus: Bus) {}

  /** Wire a transport and return a ready Agent (no manual createBus). */
  static async connect(opts: AgentConnect): Promise<Agent> {
    const { bus, hub, closeHandle } = await connectBus(opts)
    const a = new Agent(bus)
    a.hub = hub
    if (closeHandle !== undefined) a.closeHandle = closeHandle
    return a
  }

  /**
   * Turn this agent into the config authority: it serves startup snapshot
   * requests, persists values into the KV `cfg` bucket (when caps.kv), and
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

  caps() {
    return this.bus.caps
  }

  get rawBus(): Bus {
    return this.bus
  }

  async discover(maxWaitMs = 500): Promise<ExtensionManifest[]> {
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
    type: 'user_prompt' | 'interrupt' | 'event',
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
        } catch {
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
   * (created / forked / renamed / deleted). Extensions that declared the
   * kind receive it; on "deleted" this also drops the session's config
   * overrides for every known extension.
   */
  async publishLifecycleEvent(
    kind: 'created' | 'forked' | 'renamed' | 'deleted',
    sessionName: string,
    payload?: unknown,
  ): Promise<void> {
    await this.bus.publish(`abc.session.lifecycle.${kind}`, {
      kind,
      session_name: sessionName,
      payload,
    })
    if (kind === 'deleted') {
      for (const extId of this.manifestCache.keys()) {
        await this.dropSessionConfig(extId, sessionName).catch(() => {})
      }
    }
  }

  async putObject(name: string, data: Uint8Array): Promise<void> {
    return this.bus.objectPut(name, data)
  }

  async getObject(name: string): Promise<Uint8Array | null> {
    return this.bus.objectGet(name)
  }

  async close(): Promise<void> {
    await this.closeHandle?.()
    return this.bus.close()
  }
}
