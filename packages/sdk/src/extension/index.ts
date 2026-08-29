import type { Bus } from '../bus/index.js'
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
  type InterruptSignal,
  InterruptSignalSchema,
  type LifecycleEvent,
  LifecycleEventSchema,
  MAILBOX_WILDCARD,
  sessionToken,
  ToolCallEnvelopeSchema,
  type ToolResult,
} from '../protocol/index.js'
import { connectBus, type ExtensionConnect } from '../transport/index.js'

const OFFLOAD_THRESHOLD = 256 * 1024

export interface ToolSpec {
  description: string
  inputSchema?: Record<string, unknown>
  execute(
    args: Record<string, unknown>,
    callId: string,
    sessionName: string,
  ): Promise<ToolResultData | undefined>
}

export type ToolResultData = {
  content?: string
  data?: unknown
  object?: { id: string; content_type?: string }
}

export interface VariableSpec {
  description?: string
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
  /**
   * Session lifecycle callback. On "deleted" the SDK also deletes the
   * session-scoped variables (KV) before calling this.
   */
  onLifecycle?: (
    ev: LifecycleEvent,
  ) => void | Promise<void>
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
        input_schema: t.inputSchema,
      }))
      .sort((a, b) => a.name.localeCompare(b.name))
    const variables: ExtensionVariable[] = Object.entries(cfg.variables ?? {})
      .map(([name, v]) => ({
        name,
        description: v.description,
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
    // Pull the startup snapshot first (pre-serve changes), then subscribe.
    try {
      const reply = await this.bus.request(CH.configGet(this.cfg.id), {})
      const parsed = ConfigSnapshotSchema.safeParse(reply.payload)
      if (parsed.success) {
        for (const [name, value] of Object.entries(parsed.data.global)) {
          this.globalConfig.set(name, value)
        }
        for (const [sess, vals] of Object.entries(parsed.data.sessions)) {
          const m = this.sessionConfig.get(sess) ?? new Map()
          for (const [name, value] of Object.entries(vals)) m.set(name, value)
          this.sessionConfig.set(sess, m)
        }
      }
    } catch {
      // No authority serving snapshots yet; defaults apply.
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
          try {
            const result = await spec.execute(
              p.arguments ?? {},
              p.call_id,
              sessionName,
            )
            await respond(result)
          } catch (e) {
            await respond(undefined, {
              code: 'internal',
              message: String(e),
            })
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
          const handler = this.cfg.onCallHook
          let res: { ok: boolean; error?: ErrorPayload; data?: unknown }
          if (handler === undefined) {
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
        .kvDelete(
          VARS_BUCKET,
          sessionVarKey(this.cfg.id, sessionName, name),
        )
        .catch(() => {})
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
        if (this.cfg.onEventHook !== undefined) {
          await this.cfg.onEventHook('interrupt', sessionName, sig?.reason)
        }
      }
    })()
  }
}

// re-export helpers used by manifest-era callers
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
  return bus.kvPut(VARS_BUCKET, sessionVarKey(extId, sessionName, name), value, 0)
}
