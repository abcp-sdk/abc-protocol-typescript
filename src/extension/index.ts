import type { Bus, KvEvent } from '../bus/index.js'
import {
  CH,
  ConfigSetSchema,
  ConfigSnapshotSchema,
  type ErrorPayload,
  type ExtensionConfigItem,
  type ExtensionManifest,
  type ExtensionTool,
  type ExtensionVariable,
  HookCallSchema,
  HookEventSchema,
  InterruptSignalSchema,
  type LifecycleEvent,
  LifecycleEventSchema,
  MAILBOX_WILDCARD,
  sessionToken,
  ToolCallEnvelopeSchema,
  type ToolResult,
} from '../protocol/index.js'
import { validateJsonSchema } from '../protocol/jsonschema.js'
import { unescapeKVSegment } from '../protocol/kv-escaping.js'
import { connectBus, type ExtensionConnect } from '../transport/index.js'

const OFFLOAD_THRESHOLD = 256 * 1024

export interface ToolSpec {
  description: string
  /** Localized descriptions (locale → text); `description` is the fallback. */
  descriptions?: Record<string, string>
  inputSchema?: Record<string, unknown>
  /** Config names whose value this tool requires to run (a tool may share a
   * required config with sibling tools). Absent = no config is gated. */
  requiredConfig?: string[]
  /** Optional 4th parameter: aborted when this call is interrupted. The
   * await resolves immediately with an `interrupted` error either way; the
   * signal lets handlers clean up (close handles, cancel side effects). */
  execute(
    args: Record<string, unknown>,
    callId: string,
    sessionName: string,
    signal?: AbortSignal,
  ): Promise<ToolResultData | undefined>
}

export type ToolResultData = {
  content?: string
  data?: unknown
  object?: { id: string; content_type?: string }
}

export interface VariableSpec {
  description?: string
  descriptions?: Record<string, string>
  scope?: 'global' | 'session'
  resolve?: (sessionName?: string) => Promise<string> | string
}

export interface ConfigSpec {
  description?: string
  /** Value type validated agent-side; also the shape passed to the callback. */
  type: 'string' | 'number' | 'boolean' | 'enum' | 'json'
  /** Allowed values when type is enum. */
  enumValues?: string[]
  /** Applied until the first set. */
  default?: unknown
  scope?: 'global' | 'session'
}

export interface ExtensionConfig {
  id: string
  version: string
  tools?: Record<string, ToolSpec>
  variables?: Record<string, VariableSpec>
  config?: Record<string, ConfigSpec>
  callHooks?: string[]
  eventHooks?: string[]
  /** Per-hook JSON-schema (subset) for the hook payloads; the extension
   * validates incoming payloads against these before dispatch. */
  hookSchemas?: {
    call?: Record<string, Record<string, unknown>>
    event?: Record<string, Record<string, unknown>>
  }
  /** Session lifecycle kinds this extension reacts to. */
  lifecycle?: Array<'created' | 'forked' | 'renamed' | 'deleted'>
  /** Sync call-hook (req); returning an error aborts the enclosing op. */
  onCallHook?: (
    hook: string,
    sessionName: string,
    args?: Record<string, unknown>,
  ) => Promise<{ ok: boolean; error?: ErrorPayload; data?: unknown }>
  /** Async event-hook (pub); best-effort. */
  onEventHook?: (
    hook: string,
    sessionName: string,
    payload?: unknown,
  ) => void | Promise<void>
  /** Dedicated interrupt callback: called after in-flight awaits are
   * aborted. When absent, interrupts fall back to onEventHook('interrupt').
   */
  onInterrupt?: (sessionName: string, reason?: string) => void | Promise<void>
  /**
   * Session lifecycle callback. On "deleted" the SDK also deletes the
   * session-scoped variables (KV) before calling this.
   */
  onLifecycle?: (ev: LifecycleEvent) => void | Promise<void>
  /**
   * Applied config change. Returning an error (or throwing) REJECTS the
   * change: the agent keeps the old value and the revision does not advance.
   * `get(name, sessionName?)` reads the effective value inside the callback.
   */
  onConfigChange?: (
    name: string,
    value: unknown,
    sessionName: string | undefined,
    get: (name: string, sessionName?: string) => unknown,
  ) => void | Promise<void>
}

export class Extension {
  readonly manifest: ExtensionManifest
  private unsubs: Array<() => void> = []
  private presenceTimer: ReturnType<typeof setInterval> | undefined
  // In-flight tool calls per session (AbortController per call), so an
  // interrupt signal aborts that session's awaits (real cancel semantics:
  // JS cannot kill the running handler, but the call resolves immediately).
  private inflight = new Map<string, Set<AbortController>>()
  private globalConfig = new Map<string, unknown>()
  private sessionConfig = new Map<string, Map<string, unknown>>()

  /** Wire a transport and return a ready Extension (no manual createBus). */
  static async connect(
    opts: ExtensionConnect,
    cfg: ExtensionConfig,
  ): Promise<Extension> {
    const { bus } = await connectBus(opts)
    return new Extension(bus, cfg)
  }

  constructor(
    private readonly bus: Bus,
    private readonly cfg: ExtensionConfig,
  ) {
    const tools: ExtensionTool[] = Object.entries(cfg.tools ?? {})
      .map(([name, t]) => ({
        name,
        description: t.description,
        ...(t.descriptions !== undefined
          ? { descriptions: t.descriptions }
          : {}),
        input_schema: t.inputSchema,
        ...(t.requiredConfig !== undefined ? { required_config: t.requiredConfig } : {}),
      }))
      .sort((a, b) => a.name.localeCompare(b.name))
    const variables: ExtensionVariable[] = Object.entries(cfg.variables ?? {})
      .map(([name, v]) => ({
        name,
        description: v.description,
        ...(v.descriptions !== undefined
          ? { descriptions: v.descriptions }
          : {}),
        scope: v.scope ?? 'global',
      }))
      .sort((a, b) => a.name.localeCompare(b.name))
    const config: ExtensionConfigItem[] = Object.entries(cfg.config ?? {})
      .map(([name, c]) => ({
        name,
        type: c.type,
        ...(c.enumValues !== undefined ? { enum_values: c.enumValues } : {}),
        ...(c.default !== undefined ? { default: c.default } : {}),
        ...(c.description !== undefined ? { description: c.description } : {}),
        scope: c.scope ?? 'global',
      }))
      .sort((a, b) => a.name.localeCompare(b.name))

    const capabilities: Array<'tools' | 'prompt'> = []
    if (tools.length > 0) capabilities.push('tools')
    if (variables.length > 0) capabilities.push('prompt')

    this.manifest = {
      id: cfg.id,
      version: cfg.version,
      capabilities,
      // the cooperative feature set this SDK build speaks
      features: [
        'dlq',
        'config-kv',
        'presence',
        'kv-escaping',
        'interrupt-abort',
        'progress',
      ],
      ...(tools.length > 0 ? { tools } : {}),
      ...(variables.length > 0 ? { prompt: { variables } } : {}),
      ...(config.length > 0 ? { config } : {}),
      ...(cfg.callHooks !== undefined || cfg.eventHooks !== undefined
        ? {
            hooks: {
              ...(cfg.callHooks !== undefined ? { call: cfg.callHooks } : {}),
              ...(cfg.eventHooks !== undefined
                ? { event: cfg.eventHooks }
                : {}),
            },
          }
        : {}),
    }
  }

  async serve(): Promise<void> {
    this.startPresence()
    await this.subscribeDiscovery()
    await this.subscribeConfig()
    await this.subscribeTools()
    await this.subscribeVariables()
    await this.subscribeCallHooks()
    await this.subscribeEventHooks()
    await this.subscribeLifecycle()
    await this.subscribeInterrupt()
  }

  /**
   * Effective config value: session override > global set > manifest default.
   * Available inside onConfigChange via the injected `get`.
   */
  getConfig(name: string, sessionName?: string): unknown {
    if (sessionName !== undefined) {
      const override = this.sessionConfig.get(sessionName)?.get(name)
      if (override !== undefined) return override
    }
    const set = this.globalConfig.get(name)
    if (set !== undefined) return set
    return this.cfg.config?.[name]?.default
  }

  private async subscribeConfig(): Promise<void> {
    if (this.cfg.config === undefined) return
    // Recover state from the cfg KV bucket (0.2): the watch delivers the
    // snapshot at startup and live updates afterwards — no agent needs to
    // be online. Live sets still arrive (and may be rejected) via the req.
    try {
      const { stream, stop } = await this.bus.kvWatch(
        CONFIG_BUCKET,
        `${this.cfg.id}.>`,
      )
      this.unsubs.push(() => void stop())
      void (async () => {
        for await (const ev of stream) this.applyConfigKV(ev)
      })()
    } catch {
      // Bucket missing yet — defaults apply until the first set.
    }
    const sub = await this.bus.subscribe(CH.config(this.cfg.id))
    this.unsubs.push(() => void sub.close())
    void (async () => {
      for await (const env of sub) {
        if (!env.reply_to) continue
        const replyTo: string = env.reply_to
        const parsed = ConfigSetSchema.safeParse(env.payload)
        if (!parsed.success) continue
        const set = parsed.data
        let rejected: { code: 'internal'; message: string } | undefined
        if (set.scope === 'global') {
          this.globalConfig.set(set.name, set.value)
        } else if (set.session_name !== undefined) {
          const m = this.sessionConfig.get(set.session_name) ?? new Map()
          m.set(set.name, set.value)
          this.sessionConfig.set(set.session_name, m)
        }
        const handler = this.cfg.onConfigChange
        if (handler !== undefined) {
          try {
            await handler(
              set.name,
              set.value,
              set.session_name,
              (name, sessionName) => this.getConfig(name, sessionName),
            )
          } catch (e) {
            rejected = { code: 'internal', message: String(e) }
          }
        }
        // Roll back local state on rejection so the old value stays effective.
        if (rejected !== undefined) {
          if (set.scope === 'global') this.globalConfig.delete(set.name)
          else if (set.session_name !== undefined) {
            this.sessionConfig.get(set.session_name)?.delete(set.name)
          }
        }
        if (set.ack) {
          await this.bus.publish(
            replyTo,
            rejected !== undefined
              ? { ok: false, error: rejected }
              : { ok: true },
          )
        }
      }
    })()
  }

  /** Liveness heartbeat: manifest into the abc-presence KV bucket with a
   * TTL, refreshed every interval. Agents watching presence see extensions
   * arrive and (via TTL) disappear. */
  startPresence(): void {
    const put = () =>
      void this.bus
        .kvPut(
          PRESENCE_BUCKET,
          this.cfg.id,
          JSON.stringify(this.manifest),
          PRESENCE_TTL_MS,
        )
        .catch(() => {})
    put()
    this.presenceTimer = setInterval(put, 5000)
    this.unsubs.push(() => {
      if (this.presenceTimer !== undefined) clearInterval(this.presenceTimer)
      void this.bus.kvDelete(PRESENCE_BUCKET, this.cfg.id).catch(() => {})
    })
  }

  async close(): Promise<void> {
    for (const u of this.unsubs) {
      await Promise.resolve(u()).catch(() => {})
    }
    this.unsubs = []
    await this.bus.close()
  }

  /**
   * Report in-flight progress for a tool call. A one-way `pub` on
   * `abc.tool.progress.<callId>`, consumed by the agent's orchestration layer
   * (never the LLM context). Emitting it implicitly signals "still running".
   */
  async reportProgress(
    callId: string,
    progress: {
      phase?: string
      progress?: number
      text?: string
      metadata?: unknown
    },
  ): Promise<void> {
    await this.bus.publish(CH.toolProgress(callId), {
      call_id: callId,
      ...progress,
    })
  }

  private async subscribeDiscovery() {
    const sub = await this.bus.subscribe(CH.DISCOVER)
    this.unsubs.push(() => void sub.close())
    void (async () => {
      for await (const env of sub) {
        if (env.reply_to) await this.bus.publish(env.reply_to, this.manifest)
      }
    })()
  }

  private async subscribeTools() {
    for (const [name, spec] of Object.entries(this.cfg.tools ?? {})) {
      const sub = await this.bus.subscribe(CH.toolCall(this.cfg.id, name), {
        queue: this.cfg.id,
      })
      this.unsubs.push(() => void sub.close())
      void (async () => {
        for await (const env of sub) {
          if (!env.reply_to) continue
          const replyTo = env.reply_to
          const parsed = ToolCallEnvelopeSchema.safeParse(env.payload)
          if (!parsed.success) continue
          const p = parsed.data
          const sessionName = env.session_name ?? ''
          const respond = async (
            result: ToolResultData | undefined,
            error?: ErrorPayload,
          ) => {
            const res: ToolResult = { call_id: p.call_id, tool: name }
            if (error !== undefined) res.error = error
            else if (result !== undefined) {
              if (result.content !== undefined) {
                if (result.content.length > OFFLOAD_THRESHOLD) {
                  const objName = `${p.call_id}.data`
                  await this.bus.objectPut(objName, Buffer.from(result.content))
                  res.object = { id: objName, content_type: 'text/plain' }
                  res.content = result.content.slice(0, 400)
                } else {
                  res.content = result.content
                }
              }
              if (result.data !== undefined) res.data = result.data
              if (result.object !== undefined) res.object = result.object
            }
            await this.bus.publish(replyTo, res)
          }
          const ac = new AbortController()
          if (sessionName !== '') this.trackInflight(sessionName, ac)
          let settled = false
          const aborted = new Promise<never>((_, reject) => {
            ac.signal.addEventListener('abort', () => {
              if (!settled) reject(new Error('interrupted'))
            })
          })
          try {
            const result = await Promise.race([
              spec.execute(
                p.arguments ?? {},
                p.call_id,
                sessionName,
                ac.signal,
              ),
              aborted,
            ])
            settled = true
            await respond(result)
          } catch (e) {
            settled = true
            await respond(undefined, {
              code: 'internal',
              message: String(e),
            })
          } finally {
            this.untrackInflight(sessionName, ac)
          }
        }
      })()
    }
  }

  private async subscribeVariables() {
    for (const [name, spec] of Object.entries(this.cfg.variables ?? {})) {
      const sub = await this.bus.subscribe(CH.variable(this.cfg.id, name), {
        queue: this.cfg.id,
      })
      this.unsubs.push(() => void sub.close())
      void (async () => {
        for await (const env of sub) {
          if (!env.reply_to) continue
          const resolver = spec.resolve
          if (resolver === undefined) continue
          const sessionName = env.session_name
          try {
            const value = await resolver(sessionName)
            await this.bus.publish(env.reply_to, { name, value })
          } catch {
            // leave unanswered -> caller keeps the literal placeholder
          }
        }
      })()
    }
  }

  private async subscribeCallHooks() {
    for (const hook of this.cfg.callHooks ?? []) {
      const sub = await this.bus.subscribe(CH.hookCall(this.cfg.id, hook), {
        queue: this.cfg.id,
      })
      this.unsubs.push(() => void sub.close())
      void (async () => {
        for await (const env of sub) {
          if (!env.reply_to) continue
          const parsed = HookCallSchema.safeParse(env.payload)
          const p = parsed.success ? parsed.data : undefined
          const sessionName = p?.session_name ?? env.session_name ?? ''
          const badArgs = validateJsonSchema(
            this.cfg.hookSchemas?.call?.[hook],
            p?.arguments,
          )
          const handler = this.cfg.onCallHook
          let res: { ok: boolean; error?: ErrorPayload; data?: unknown }
          if (badArgs !== null) {
            res = {
              ok: false,
              error: { code: 'invalid_argument', message: badArgs },
            }
          } else if (handler === undefined) {
            res = {
              ok: false,
              error: {
                code: 'not_found',
                message: `no handler for call hook ${hook}`,
              },
            }
          } else {
            try {
              res = await handler(hook, sessionName, p?.arguments)
            } catch (e) {
              res = {
                ok: false,
                error: { code: 'internal', message: String(e) },
              }
            }
          }
          await this.bus.publish(env.reply_to, res)
        }
      })()
    }
  }

  private async subscribeEventHooks() {
    for (const hook of this.cfg.eventHooks ?? []) {
      const sub = await this.bus.subscribe(CH.hookEvent(hook), {
        queue: this.cfg.id,
      })
      this.unsubs.push(() => void sub.close())
      void (async () => {
        for await (const env of sub) {
          const parsed = HookEventSchema.safeParse(env.payload)
          const ev = parsed.success ? parsed.data : undefined
          const sessionName = ev?.session_name ?? env.session_name ?? ''
          const bad = validateJsonSchema(
            this.cfg.hookSchemas?.event?.[hook],
            ev?.payload,
          )
          if (bad !== null) continue // invalid event payload: drop (best-effort)
          if (this.cfg.onEventHook !== undefined) {
            await this.cfg.onEventHook(hook, sessionName, ev?.payload)
          }
        }
      })()
    }
  }

  private async subscribeLifecycle(): Promise<void> {
    const kinds = this.cfg.lifecycle ?? []
    if (kinds.length === 0 || this.cfg.onLifecycle === undefined) return
    const sub = await this.bus.subscribe('abc.session.lifecycle.>', {
      queue: this.cfg.id,
    })
    this.unsubs.push(() => void sub.close())
    void (async () => {
      for await (const env of sub) {
        const kind = env.ch.slice('abc.session.lifecycle.'.length)
        if (!kinds.includes(kind as 'created')) continue
        const parsed = LifecycleEventSchema.safeParse(env.payload)
        if (!parsed.success) continue
        if (kind === 'deleted') {
          await this.deleteSessionVariables(parsed.data.session_name).catch(
            () => {},
          )
        }
        await this.cfg.onLifecycle?.(parsed.data)
      }
    })()
  }

  /** Delete every session-scoped variable of a session (KV). */
  private async deleteSessionVariables(sessionName: string): Promise<void> {
    for (const [name, spec] of Object.entries(this.cfg.variables ?? {})) {
      if (spec.scope !== 'session') continue
      await this.bus
        .kvDelete(VARS_BUCKET, sessionVarKey(this.cfg.id, sessionName, name))
        .catch(() => {})
    }
  }

  /** Apply a cfg-bucket watch entry. Key layout: <extId>.<name> (global)
   * or <extId>.<session>.<name> (session). Envelope {r,v} with a
   * bare-value fallback for pre-0.2 entries. */
  private applyConfigKV(ev: KvEvent): void {
    // Key layout: <extId>.<name> (global) or <extId>.<escapedSession>.<name>.
    // The session segment is escaped (v0.2.2+); legacy colon-style session
    // names parse identically (their segments carry no dots).
    const rest = ev.key.slice(this.cfg.id.length + 1)
    const i = rest.indexOf('.')
    if (i === -1) {
      if (!ev.deleted) this.applyConfigValue('', rest, ev.value)
      else this.globalConfig.delete(rest)
      return
    }
    const session = unescapeKVSegment(rest.slice(0, i))
    const name = rest.slice(i + 1)
    if (ev.deleted) {
      this.sessionConfig.get(session)?.delete(name)
      return
    }
    let v: unknown
    try {
      const parsed = JSON.parse(ev.value) as { r?: number; v?: unknown }
      if (parsed !== null && typeof parsed === 'object' && 'v' in parsed) {
        v = parsed.v
      } else {
        v = parsed
      }
    } catch {
      return
    }
    if (v === undefined || v === null) return
    const m = this.sessionConfig.get(session) ?? new Map()
    m.set(name, v)
    this.sessionConfig.set(session, m)
  }

  private applyConfigValue(session: string, name: string, value: string): void {
    let v: unknown
    try {
      const parsed = JSON.parse(value) as { r?: number; v?: unknown }
      if (parsed !== null && typeof parsed === 'object' && 'v' in parsed) {
        v = parsed.v
      } else {
        v = parsed
      }
    } catch {
      return
    }
    if (v === undefined || v === null) return
    if (session === '') this.globalConfig.set(name, v)
    else {
      const m = this.sessionConfig.get(session) ?? new Map()
      m.set(name, v)
      this.sessionConfig.set(session, m)
    }
  }

  private async subscribeInterrupt() {
    const sub = await this.bus.subscribe(CH.interrupt(this.cfg.id), {
      queue: this.cfg.id,
    })
    this.unsubs.push(() => void sub.close())
    void (async () => {
      for await (const env of sub) {
        const parsed = InterruptSignalSchema.safeParse(env.payload)
        const sig = parsed.success ? parsed.data : undefined
        const sessionName = sig?.session_name ?? env.session_name ?? ''
        // Abort in-flight awaits first: session-scoped signal cancels that
        // session only; a signal without a session is a broadcast.
        this.abortInflight(sessionName === '' ? undefined : sessionName)
        if (this.cfg.onInterrupt !== undefined) {
          await this.cfg.onInterrupt(sessionName, sig?.reason)
        } else if (this.cfg.onEventHook !== undefined) {
          await this.cfg.onEventHook('interrupt', sessionName, sig?.reason)
        }
      }
    })()
  }

  private trackInflight(session: string, ac: AbortController): void {
    let set = this.inflight.get(session)
    if (set === undefined) {
      set = new Set()
      this.inflight.set(session, set)
    }
    set.add(ac)
  }

  private untrackInflight(session: string, ac: AbortController): void {
    if (session === '') return
    const set = this.inflight.get(session)
    if (set === undefined) return
    set.delete(ac)
    if (set.size === 0) this.inflight.delete(session)
  }

  /** Abort in-flight awaits; undefined session = broadcast (all). */
  private abortInflight(session: string | undefined): void {
    if (session === undefined) {
      const all = this.inflight
      this.inflight = new Map()
      for (const set of all.values()) for (const ac of set) ac.abort()
      return
    }
    const set = this.inflight.get(session)
    this.inflight.delete(session)
    if (set !== undefined) for (const ac of set) ac.abort()
  }
}

export const VARS_BUCKET = 'vars'
export function varKey(provider: string, name: string): string {
  return `${provider}.${name}`
}
export function sessionVarKey(
  provider: string,
  sessionName: string,
  name: string,
): string {
  return `${provider}.${sessionToken(sessionName)}.${name}`
}

function _mailboxWildcard(): string {
  return MAILBOX_WILDCARD
}
void _mailboxWildcard

// ---------------------------------------------------------------------------
// Session-facing helpers (session events, mailbox, variables, objects).

/**
 * Push one SSE event onto the session's durable event stream
 * (`abc.session.events.<token>`) — the channel the agent's SSE handler
 * replays and live-tails. Use for UI-facing side-effect notices.
 */
export async function publishSessionEvent(
  bus: Bus,
  sessionName: string,
  event: string,
  params?: unknown,
): Promise<void> {
  const id = crypto.randomUUID()
  await bus.inboxPublish(
    CH.sessionEvents(sessionName),
    { event, params, eid: id },
    { id, sessionName },
  )
}

/** Publish an event into a session's durable mailbox. */
export async function publishMailboxEvent(
  bus: Bus,
  sessionName: string,
  eventType = 'event',
  payload?: unknown,
): Promise<void> {
  const id = crypto.randomUUID()
  await bus.inboxPublish(
    CH.mailbox(sessionName),
    { id, type: eventType, payload },
    { id, sessionName },
  )
}

/** Store an object (large tool results, …). */
export function putObject(
  bus: Bus,
  name: string,
  data: Uint8Array,
): Promise<void> {
  return bus.objectPut(name, data)
}

/** Store a global variable (vars.<extId>.<name>). */
export function setVariable(
  bus: Bus,
  extId: string,
  name: string,
  value: string,
): Promise<void> {
  return bus.kvPut(VARS_BUCKET, varKey(extId, name), value, 0)
}

/** Store a session variable (vars.<extId>.<token>.<name>) — the KV cache. */
export function setSessionVariable(
  bus: Bus,
  extId: string,
  sessionName: string,
  name: string,
  value: string,
): Promise<void> {
  return bus.kvPut(
    VARS_BUCKET,
    sessionVarKey(extId, sessionName, name),
    value,
    0,
  )
}

// trackInflight registers a call's AbortController under its session.

// CONFIG_BUCKET is the cfg KV bucket name (source of truth for config).
const CONFIG_BUCKET = 'cfg'

/** Presence bucket: key = extId, value = manifest, TTL refreshed by the
 * extension heartbeat (extension/index.ts). */
export const PRESENCE_BUCKET = 'abc-presence'
export const PRESENCE_TTL_MS = 15_000
