import { randomUUID } from 'node:crypto'
import {
  type Bus,
  type Caps,
  type Envelope,
  EnvelopeSchema,
  type InboxConsumeOpts,
  type InboxMsg,
  type InboxPublishOpts,
  type InboxSubscription,
  type RequestOpts,
  type SubscribeOpts,
  type Subscription,
} from '@abc-protocol/sdk'
import type { RawData } from 'ws'
import { WebSocket, WebSocketServer } from 'ws'
import { z } from 'zod'

const CAPS: Caps = {
  requestReply: true,
  broadcast: true,
  pubSub: true,
  durableInbox: true,
  object: false,
  kv: false,
}

interface RpcRequest {
  jsonrpc: '2.0'
  id: string | number
  method: string
  params?: unknown
}
interface RpcResponse {
  jsonrpc: '2.0'
  id: string | number | null
  result?: unknown
  error?: { code: number; message: string }
}
interface RpcNotification {
  jsonrpc: '2.0'
  method: string
  params?: unknown
}

const RpcRequestSchema = z.object({
  jsonrpc: z.literal('2.0'),
  id: z.union([z.string(), z.number()]),
  method: z.string(),
  params: z.unknown().optional(),
})
const RpcNotificationSchema = z.object({
  jsonrpc: z.literal('2.0'),
  method: z.string(),
  params: z.unknown().optional(),
})
const RpcMessageSchema = z.union([RpcRequestSchema, RpcNotificationSchema])
type RpcMessage = z.infer<typeof RpcMessageSchema>

const PublishParamsSchema = z.object({
  channel: z.string(),
  payload: z.unknown(),
  reply_to: z.string().optional(),
  session_name: z.string().optional(),
})
const SubscribeParamsSchema = z.object({
  channel: z.string(),
  queue: z.string().optional(),
})
const InboxPublishParamsSchema = z.object({
  id: z.string(),
  channel: z.string(),
  payload: z.unknown(),
  session_name: z.string().optional(),
})
const InboxAckParamsSchema = z.object({
  id: z.string(),
  action: z.string(),
  delay_ms: z.number().optional(),
})
const UnsubscribeParamsSchema = z.object({ channel: z.string() })
const InboxConsumeParamsSchema = z.object({ channel: z.string().optional() })

/** subjectMatch implements NATS-style `*` / `>` channel matching. */
function subjectMatch(pattern: string, subject: string): boolean {
  const p = pattern.split('.')
  const s = subject.split('.')
  let pi = 0
  for (let si = 0; si < s.length; si++) {
    if (pi >= p.length) return false
    const pt = p[pi] as string
    if (pt === '>') return true
    if (pt === '*') {
      pi++
      continue
    }
    if (pt !== s[si]) return false
    pi++
  }
  return pi === p.length
}

interface InboxRow {
  id: string
  channel: string
  payload: string
  sessionName?: string
}

class InboxStore {
  private rows = new Map<string, InboxRow>()
  private leased = new Map<string, number>()

  inboxPublish(
    id: string,
    channel: string,
    payload: string,
    sessionName?: string,
  ): void {
    if (!this.rows.has(id)) {
      const row: InboxRow = { id, channel, payload }
      if (sessionName !== undefined) row.sessionName = sessionName
      this.rows.set(id, row)
    }
  }
  inboxConsume(leaseMs: number): InboxRow | null {
    const now = Date.now()
    for (const [id, row] of this.rows) {
      const lease = this.leased.get(id) ?? 0
      if (lease <= now) {
        this.leased.set(id, now + leaseMs)
        return row
      }
    }
    return null
  }
  inboxAck(id: string): void {
    this.rows.delete(id)
    this.leased.delete(id)
  }
  inboxNak(id: string, delayMs: number): void {
    this.leased.set(id, Date.now() + delayMs)
  }
  inboxTerm(id: string): void {
    this.rows.delete(id)
    this.leased.delete(id)
  }
}

/**
 * WS transport: an agent-embedded JSON-RPC 2.0 hub. The agent side runs the
 * hub (single-replica); extension services connect as clients. The durable
 * inbox is backed by an in-memory store (single-replica by design; use NATS
 * for multi-replica).
 */
export class WsHub {
  private wss: WebSocketServer
  private sessions = new Map<WebSocket, string>()
  private subs = new Map<string, Set<WebSocket>>()
  private inboxConsumers = new Map<WebSocket, string>()
  private inboxOrder: Array<{ ws: WebSocket; channel: string }> = []
  private store = new InboxStore()
  private pumpTimer: NodeJS.Timeout

  constructor(port: number) {
    this.wss = new WebSocketServer({ port })
    this.wss.on('connection', ws => this.onConnection(ws))
    this.pumpTimer = setInterval(() => this.pumpInbox(), 10)
    this.pumpTimer.unref()
  }

  /** The actual bound port (useful when constructed with port 0). */
  get port(): number {
    const a = this.wss.address()
    return typeof a === 'object' && a !== null ? a.port : 0
  }

  close() {
    clearInterval(this.pumpTimer)
    this.wss.close()
  }

  private onConnection(ws: WebSocket) {
    this.sessions.set(ws, 'extension')
    ws.on('message', (raw: RawData) => {
      const parsed = RpcMessageSchema.safeParse(JSON.parse(raw.toString()))
      if (!parsed.success) return
      const m = parsed.data
      void this.handle(ws, m as RpcRequest | RpcNotification)
    })
    ws.on('close', () => {
      this.sessions.delete(ws)
      this.inboxConsumers.delete(ws)
      this.inboxOrder = this.inboxOrder.filter(e => e.ws !== ws)
      for (const set of this.subs.values()) set.delete(ws)
    })
  }

  private send(ws: WebSocket, m: unknown) {
    if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(m))
  }

  private reply(m: RpcRequest, ws: WebSocket, result?: unknown) {
    this.send(ws, { jsonrpc: '2.0', id: m.id, result } satisfies RpcResponse)
  }

  private async handle(ws: WebSocket, m: RpcMessage) {
    if ('id' in m) {
      switch (m.method) {
        case 'abep.ctl.hello':
          this.reply(m, ws, { caps: CAPS })
          return
        case 'abep.ctl.subscribe': {
          const parsed = SubscribeParamsSchema.safeParse(m.params ?? {})
          if (!parsed.success) return
          this.subscribe(ws, parsed.data.channel, parsed.data.queue ?? '')
          this.reply(m, ws, {})
          return
        }
        case 'abep.ctl.unsubscribe': {
          const parsed = UnsubscribeParamsSchema.safeParse(m.params ?? {})
          if (!parsed.success) return
          this.unsubscribe(ws, parsed.data.channel)
          this.reply(m, ws, {})
          return
        }
        case 'abep.ctl.publish': {
          const parsed = PublishParamsSchema.safeParse(m.params ?? {})
          if (!parsed.success) return
          this.publishChannel(
            parsed.data.channel,
            parsed.data.payload,
            parsed.data.reply_to,
            parsed.data.session_name,
          )
          this.reply(m, ws, {})
          return
        }
        case 'abep.ctl.inbox.publish': {
          const parsed = InboxPublishParamsSchema.safeParse(m.params ?? {})
          if (!parsed.success) return
          this.store.inboxPublish(
            parsed.data.id,
            parsed.data.channel,
            JSON.stringify({
              v: 1,
              ch: parsed.data.channel,
              kind: 'queue',
              id: parsed.data.id,
              ...(parsed.data.session_name !== undefined
                ? { session_name: parsed.data.session_name }
                : {}),
              payload: parsed.data.payload,
            }),
            parsed.data.session_name,
          )
          this.reply(m, ws, {})
          return
        }
        case 'abep.ctl.inbox.consume': {
          const parsed = InboxConsumeParamsSchema.safeParse(m.params ?? {})
          const channel = parsed.success
            ? (parsed.data.channel ?? 'abc.mailbox.>')
            : 'abc.mailbox.>'
          this.inboxConsumers.set(ws, channel)
          this.inboxOrder.push({ ws, channel })
          this.reply(m, ws, {})
          return
        }
        case 'abep.ctl.inbox.ack': {
          const parsed = InboxAckParamsSchema.safeParse(m.params ?? {})
          if (!parsed.success) return
          this.ackInbox(
            parsed.data.id,
            parsed.data.action,
            parsed.data.delay_ms ?? 0,
          )
          this.reply(m, ws, {})
          return
        }
        default:
          this.reply(m, ws, {
            error: { code: -32601, message: `unknown ${m.method}` },
          })
          return
      }
    }
    const parsed = EnvelopeSchema.safeParse(m.params ?? {})
    if (!parsed.success) return
    const e = parsed.data
    this.publishChannel(e.ch, e.payload, e.reply_to, e.session_name)
  }

  private subscribe(ws: WebSocket, channel: string, queue: string) {
    if (queue !== '') {
      let set = this.subs.get(`${queue}\u0000${channel}`)
      if (!set) {
        set = new Set()
        this.subs.set(`${queue}\u0000${channel}`, set)
      }
      set.add(ws)
      return
    }
    let set = this.subs.get(channel)
    if (!set) {
      set = new Set()
      this.subs.set(channel, set)
    }
    set.add(ws)
  }

  private unsubscribe(ws: WebSocket, channel: string) {
    this.subs.get(channel)?.delete(ws)
    for (const [key, set] of this.subs) {
      if (key.endsWith(`\u0000${channel}`)) set.delete(ws)
    }
  }

  private publishChannel(
    channel: string,
    payload: unknown,
    replyTo?: string,
    sessionName?: string,
  ) {
    const env: Envelope = { v: 1, ch: channel, kind: 'pub', payload }
    if (replyTo !== undefined) env.reply_to = replyTo
    if (sessionName !== undefined) env.session_name = sessionName
    const targets = new Set<WebSocket>()
    for (const [pattern, set] of this.subs) {
      for (const ws of set) {
        const parsed = pattern.split('\u0000')
        const pat = parsed.length === 2 ? (parsed[1] as string) : pattern
        if (pat === channel || subjectMatch(pat, channel)) targets.add(ws)
      }
    }
    for (const ws of targets) {
      this.send(ws, {
        jsonrpc: '2.0',
        method: channel,
        params: env,
      } satisfies RpcNotification)
    }
  }

  private ackInbox(id: string, action: string, delayMs: number) {
    if (action === 'ack') this.store.inboxAck(id)
    else if (action === 'term') this.store.inboxTerm(id)
    else this.store.inboxNak(id, delayMs || 1000)
  }

  private pumpInbox() {
    const row = this.store.inboxConsume(30000)
    if (row === null) return
    const payload = EnvelopeSchema.safeParse(JSON.parse(row.payload))
    if (!payload.success) {
      this.store.inboxTerm(row.id)
      return
    }
    const env: Envelope = {
      v: 1,
      ch: payload.data.ch || row.channel,
      kind: 'queue',
      id: payload.data.id ?? row.id,
      payload: payload.data.payload,
    }
    if (payload.data.session_name !== undefined) {
      env.session_name = payload.data.session_name
    } else if (row.sessionName !== undefined) {
      env.session_name = row.sessionName
    }
    const eligible = this.inboxOrder.filter(e =>
      subjectMatch(e.channel, row.channel),
    )
    if (eligible.length === 0) {
      this.store.inboxNak(row.id, 1000)
      return
    }
    const first = eligible[0]
    if (first === undefined) return
    this.send(first.ws, {
      jsonrpc: '2.0',
      method: row.channel,
      params: env,
    } satisfies RpcNotification)
  }
}

class SubHandle implements Subscription {
  private q: Envelope[] = []
  private waiters: Array<(v: IteratorResult<Envelope>) => void> = []
  private closed = false

  push(e: Envelope) {
    if (this.closed) return
    const w = this.waiters.shift()
    if (w) w({ value: e, done: false })
    else this.q.push(e)
  }

  [Symbol.asyncIterator](): AsyncIterator<Envelope> {
    return {
      next: () => {
        if (this.q.length > 0)
          return Promise.resolve({
            value: this.q.shift() as Envelope,
            done: false,
          })
        if (this.closed)
          return Promise.resolve({ value: undefined, done: true })
        return new Promise(resolve => this.waiters.push(resolve))
      },
    }
  }

  async close() {
    if (this.closed) return
    this.closed = true
    for (const w of this.waiters.splice(0)) w({ value: undefined, done: true })
  }
}

/** WS transport client (extension side, or agent side as a hub client). */
export class WsBus implements Bus {
  readonly caps = CAPS
  private ws: WebSocket
  private nextId = 0
  private pending = new Map<string | number, (r: RpcResponse) => void>()
  private subs = new Map<string, SubHandle>()
  private ready: Promise<void>

  constructor(url: string) {
    this.ws = new WebSocket(url)
    this.ready = new Promise((resolve, reject) => {
      this.ws.once('open', () => resolve())
      this.ws.once('error', e => reject(e))
    })
    this.ws.on('message', raw => this.onMessage(raw.toString()))
  }

  private send(m: unknown) {
    this.ws.send(JSON.stringify(m))
  }

  private call(method: string, params: unknown): Promise<RpcResponse> {
    const id = ++this.nextId
    return new Promise(resolve => {
      this.pending.set(id, resolve)
      this.send({ jsonrpc: '2.0', id, method, params } satisfies RpcRequest)
    })
  }

  private onMessage(raw: string) {
    const m = JSON.parse(raw) as RpcRequest | RpcNotification | RpcResponse
    if ('id' in m && !('method' in m)) {
      const w = this.pending.get(m.id as string | number)
      if (w) {
        this.pending.delete(m.id as string | number)
        w(m)
      }
      return
    }
    if ('method' in m && 'params' in m) {
      const ch = (m as RpcNotification).method
      const params = (m as RpcNotification).params as Envelope | undefined
      for (const [pattern, handle] of this.subs) {
        if (pattern === ch || subjectMatch(pattern, ch)) {
          if (params !== undefined) handle.push(params)
        }
      }
    }
  }

  async request(
    ch: string,
    payload: unknown,
    opts: RequestOpts = {},
  ): Promise<Envelope> {
    await this.ready
    const replyCh = `abc.ws.reply.${randomUUID()}`
    const sub = await this.subscribe(replyCh)
    await this.publish(ch, payload, replyCh, opts.sessionName)
    const timeoutMs = opts.timeoutMs ?? 2000
    try {
      return await new Promise<Envelope>((resolve, reject) => {
        const timer =
          timeoutMs > 0
            ? setTimeout(
                () => reject(new Error(`request ${ch} timed out`)),
                timeoutMs,
              )
            : undefined
        void (async () => {
          for await (const env of sub) {
            if (timer !== undefined) clearTimeout(timer)
            resolve(env)
            return
          }
        })()
      })
    } finally {
      await sub.close()
      await this.unsubscribe(replyCh)
    }
  }

  async requestMany(
    ch: string,
    payload: unknown,
    opts: RequestOpts = {},
  ): Promise<Envelope[]> {
    await this.ready
    const replyCh = `abc.ws.reply.${randomUUID()}`
    const sub = await this.subscribe(replyCh)
    await this.publish(ch, payload, replyCh, opts.sessionName)
    const maxWaitMs = opts.maxWaitMs ?? 500
    const out: Envelope[] = []
    try {
      await new Promise<void>(resolve => {
        const timer = setTimeout(resolve, maxWaitMs)
        void (async () => {
          for await (const env of sub) out.push(env)
          clearTimeout(timer)
          resolve()
        })()
      })
    } finally {
      await sub.close()
      await this.unsubscribe(replyCh)
    }
    return out
  }

  async publish(
    ch: string,
    payload: unknown,
    replyTo?: string,
    sessionName?: string,
  ): Promise<void> {
    await this.ready
    await this.call('abep.ctl.publish', {
      channel: ch,
      payload,
      reply_to: replyTo,
      session_name: sessionName,
    })
  }

  async subscribe(ch: string, opts?: SubscribeOpts): Promise<Subscription> {
    await this.ready
    let handle = this.subs.get(ch)
    if (handle !== undefined) return handle
    handle = new SubHandle()
    this.subs.set(ch, handle)
    await this.call('abep.ctl.subscribe', {
      channel: ch,
      queue: opts?.queue ?? '',
    })
    return handle
  }

  async unsubscribe(ch: string): Promise<void> {
    this.subs.delete(ch)
    await this.call('abep.ctl.unsubscribe', { channel: ch })
  }

  async inboxPublish(
    ch: string,
    payload: unknown,
    opts: InboxPublishOpts,
  ): Promise<void> {
    await this.ready
    await this.call('abep.ctl.inbox.publish', {
      id: opts.id,
      channel: ch,
      payload,
      session_name: opts.sessionName,
    })
  }

  async inboxConsume(opts?: InboxConsumeOpts): Promise<InboxSubscription> {
    await this.ready
    const ch = opts?.subject ?? 'abc.mailbox.>'
    const handle = (await this.subscribe(ch)) as SubHandle
    await this.call('abep.ctl.inbox.consume', { channel: ch })
    const self = this
    return {
      [Symbol.asyncIterator]() {
        return inboxIter(handle, self)
      },
      async close() {
        await handle.close()
      },
    }
  }

  async objectPut(_name: string, _data: Uint8Array): Promise<void> {}
  async objectGet(_name: string): Promise<Uint8Array | null> {
    return null
  }
  async ackInbox(id: string, action: string, delayMs: number): Promise<void> {
    await this.call('abep.ctl.inbox.ack', { id, action, delay_ms: delayMs })
  }
  async kvCreate(): Promise<number | null> {
    return null
  }
  async kvPut(): Promise<void> {}
  async kvGet(): Promise<string | null> {
    return null
  }
  async kvCas(): Promise<number | null> {
    return null
  }
  async kvDelete(): Promise<void> {}

  async close(): Promise<void> {
    for (const h of this.subs.values()) await h.close()
    this.ws.close()
  }
}

async function* inboxIter(
  handle: SubHandle,
  bus: WsBus,
): AsyncGenerator<InboxMsg> {
  for await (const env of handle) {
    const msg = env as InboxMsg
    msg.ack = () => bus.ackInbox(env.id ?? '', 'ack', 0)
    msg.nak = (delayMs?: number) =>
      bus.ackInbox(env.id ?? '', 'nak', delayMs ?? 0)
    msg.term = () => bus.ackInbox(env.id ?? '', 'term', 0)
    yield msg
  }
}

/**
 * Symmetric WS transport provider. Unlike NATS/inproc, WS is not peer-to-peer:
 * the agent embeds a `WsHub` and extensions dial it. Calling `createBus` with
 * `{ side: 'agent', port }` starts a hub AND returns a client bus wired to it;
 * `{ side: 'extension', url }` returns a client bus dialing a remote hub.
 * Return `bus` is the identical `Bus` type across providers.
 */
export async function createWsBus(
  options: { side: 'agent'; port: number } | { side: 'extension'; url: string },
): Promise<{ bus: Bus; hub?: WsHub }> {
  if (options.side === 'agent') {
    const hub = new WsHub(options.port)
    await new Promise(r => setTimeout(r, 20))
    const bus = new WsBus(`ws://127.0.0.1:${hub.port}`)
    return { bus, hub }
  }
  return { bus: new WsBus(options.url) }
}
