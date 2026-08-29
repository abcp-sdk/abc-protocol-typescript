import { randomUUID } from 'node:crypto'
import type {
  Bus,
  Caps,
  InboxConsumeOpts,
  InboxMsg,
  InboxPublishOpts,
  InboxSubscription,
  RequestOpts,
  SubscribeOpts,
  Subscription,
} from '../../bus/index.js'
import type { Envelope } from '../../protocol/index.js'

const CAPS: Caps = {
  requestReply: true,
  broadcast: true,
  pubSub: true,
  durableInbox: true,
  object: true,
  kv: true,
}

/** subjectMatch implements NATS-style `*` / `>` channel matching. */
function subjectMatch(pattern: string, subject: string): boolean {
  const p = pattern.split('.')
  const s = subject.split('.')
  let pi = 0
  for (let si = 0; si < s.length; si++) {
    if (pi >= p.length) return false
    const pt = p[pi]
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

class Queue<T> {
  private items: T[] = []
  private waiters: Array<(v: IteratorResult<T>) => void> = []
  private closed = false

  push(v: T) {
    if (this.closed) return
    const w = this.waiters.shift()
    if (w) w({ value: v, done: false })
    else this.items.push(v)
  }

  close() {
    if (this.closed) return
    this.closed = true
    for (const w of this.waiters.splice(0)) w({ value: undefined, done: true })
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: () => {
        if (this.items.length > 0) {
          return Promise.resolve({
            value: this.items.shift() as T,
            done: false,
          })
        }
        if (this.closed)
          return Promise.resolve({ value: undefined, done: true })
        return new Promise(resolve => this.waiters.push(resolve))
      },
    }
  }
}

class Sub implements Subscription {
  private q = new Queue<Envelope>()
  push(e: Envelope) {
    this.q.push(e)
  }
  [Symbol.asyncIterator]() {
    return this.q[Symbol.asyncIterator]()
  }
  async close() {
    this.q.close()
  }
}

interface KVEntry {
  value: string
  revision: number
  expiresAt: number
}

interface InboxEntry {
  id: string
  msg: Envelope
}

/**
 * In-process bus hub: all `InprocBus` instances sharing one hub see each
 * other, mirroring a real broker. Used for tests and single-process embedding.
 */
export class InprocHub {
  private subs = new Map<string, Set<Sub>>()
  private queues = new Map<string, Sub[]>()
  private inbox: InboxEntry[] = []
  private inboxWaiters: Array<(e: InboxEntry) => void> = []
  private objects = new Map<string, Uint8Array>()
  private kv = new Map<string, Map<string, KVEntry>>()

  subscribe(ch: string): Sub {
    let set = this.subs.get(ch)
    if (!set) {
      set = new Set()
      this.subs.set(ch, set)
    }
    const s = new Sub()
    set.add(s)
    return s
  }

  subscribeQueue(ch: string): Sub {
    let q = this.queues.get(ch)
    if (!q) {
      q = []
      this.queues.set(ch, q)
    }
    const s = new Sub()
    q.push(s)
    return s
  }

  unsubscribe(ch: string, s: Sub) {
    this.subs.get(ch)?.delete(s)
    const q = this.queues.get(ch)
    if (q) {
      const idx = q.indexOf(s)
      if (idx >= 0) q.splice(idx, 1)
    }
  }

  publish(ch: string, env: Envelope) {
    const set = this.subs.get(ch)
    if (set) {
      for (const s of [...set]) s.push(env)
    }
    // Wildcard subscriptions (NATS-style `*` / `>`) also receive.
    for (const [pattern, subs] of this.subs) {
      if (pattern === ch || !(pattern.includes('*') || pattern.includes('>')))
        continue
      if (subjectMatch(pattern, ch)) {
        for (const s of [...subs]) s.push(env)
      }
    }
    const q = this.queues.get(ch)
    if (q && q.length > 0) {
      const member = q.shift() as Sub
      q.push(member)
      member.push(env)
    }
    // Queue groups on wildcard patterns also consume (round-robin).
    for (const [pattern, q] of this.queues) {
      if (pattern === ch || !(pattern.includes('*') || pattern.includes('>')))
        continue
      if (subjectMatch(pattern, ch) && q.length > 0) {
        const member = q.shift() as Sub
        q.push(member)
        member.push(env)
      }
    }
  }

  inboxPush(entry: InboxEntry) {
    const w = this.inboxWaiters.shift()
    if (w) w(entry)
    else this.inbox.push(entry)
  }

  inboxWait(): Promise<InboxEntry> {
    const e = this.inbox.shift()
    if (e) return Promise.resolve(e)
    return new Promise(resolve => this.inboxWaiters.push(resolve))
  }

  objectPut(name: string, data: Uint8Array) {
    this.objects.set(name, data)
  }

  objectGet(name: string): Uint8Array | null {
    return this.objects.get(name) ?? null
  }

  kvCreate(
    bucket: string,
    key: string,
    value: string,
    ttlMs: number,
  ): number | null {
    let b = this.kv.get(bucket)
    if (!b) {
      b = new Map()
      this.kv.set(bucket, b)
    }
    const existing = b.get(key)
    if (existing !== undefined && existing.expiresAt > Date.now()) return null
    b.set(key, { value, revision: 1, expiresAt: Date.now() + ttlMs })
    return 1
  }

  kvGet(bucket: string, key: string): string | null {
    const e = this.kv.get(bucket)?.get(key)
    if (e === undefined || e.expiresAt <= Date.now()) return null
    return e.value
  }

  kvPut(bucket: string, key: string, value: string, ttlMs: number): void {
    let b = this.kv.get(bucket)
    if (!b) {
      b = new Map()
      this.kv.set(bucket, b)
    }
    const existing = b.get(key)
    const rev = existing !== undefined ? existing.revision + 1 : 1
    const expiresAt = ttlMs > 0 ? Date.now() + ttlMs : Number.MAX_SAFE_INTEGER
    b.set(key, { value, revision: rev, expiresAt })
  }

  kvCas(
    bucket: string,
    key: string,
    value: string,
    revision: number,
  ): number | null {
    const b = this.kv.get(bucket)
    const existing = b?.get(key)
    if (existing === undefined || existing.expiresAt <= Date.now()) return null
    if (existing.revision !== revision) return null
    const next = revision + 1
    b?.set(key, { value, revision: next, expiresAt: existing.expiresAt })
    return next
  }

  kvDelete(bucket: string, key: string) {
    this.kv.get(bucket)?.delete(key)
  }
}

/** A Bus backed by an in-process hub. */
export class InprocBus implements Bus {
  readonly caps = CAPS
  constructor(private readonly hub: InprocHub) {}

  async request(
    ch: string,
    payload: unknown,
    opts: RequestOpts = {},
  ): Promise<Envelope> {
    const replyCh = `abc.inproc.reply.${randomUUID()}`
    const sub = this.hub.subscribe(replyCh)
    const timeoutMs = opts.timeoutMs ?? 2000
    const env: Envelope = {
      v: 1,
      ch,
      kind: 'req',
      id: randomUUID(),
      reply_to: replyCh,
      payload,
    }
    if (opts.sessionName !== undefined) env.session_name = opts.sessionName
    this.hub.publish(ch, env)
    try {
      return await new Promise<Envelope>((resolve, reject) => {
        const timer =
          timeoutMs > 0
            ? setTimeout(
                () => reject(new Error(`request ${ch} timed out`)),
                timeoutMs,
              )
            : undefined
        const iterator = sub[Symbol.asyncIterator]()
        void (async () => {
          const res = await iterator.next()
          if (timer !== undefined) clearTimeout(timer)
          if (res.done) reject(new Error(`request ${ch} closed`))
          else resolve(res.value)
        })()
      })
    } finally {
      this.hub.unsubscribe(replyCh, sub)
      await sub.close()
    }
  }

  async requestMany(
    ch: string,
    payload: unknown,
    opts: RequestOpts = {},
  ): Promise<Envelope[]> {
    const replyCh = `abc.inproc.reply.${randomUUID()}`
    const sub = this.hub.subscribe(replyCh)
    const maxWaitMs = opts.maxWaitMs ?? 500
    const env: Envelope = {
      v: 1,
      ch,
      kind: 'req',
      id: randomUUID(),
      reply_to: replyCh,
      payload,
    }
    if (opts.sessionName !== undefined) env.session_name = opts.sessionName
    this.hub.publish(ch, env)
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
      this.hub.unsubscribe(replyCh, sub)
      await sub.close()
    }
    return out
  }

  async publish(ch: string, payload: unknown): Promise<void> {
    this.hub.publish(ch, { v: 1, ch, kind: 'pub', payload })
  }

  async subscribe(ch: string, opts?: SubscribeOpts): Promise<Subscription> {
    if (opts?.queue !== undefined) return this.hub.subscribeQueue(ch)
    return this.hub.subscribe(ch)
  }

  async inboxPublish(
    ch: string,
    payload: unknown,
    opts: InboxPublishOpts,
  ): Promise<void> {
    const msg: Envelope = { v: 1, ch, kind: 'queue', id: opts.id, payload }
    if (opts.sessionName !== undefined) msg.session_name = opts.sessionName
    this.hub.inboxPush({
      id: opts.id,
      msg,
    })
  }

  async inboxConsume(_opts?: InboxConsumeOpts): Promise<InboxSubscription> {
    const hub = this.hub
    const q = new Queue<InboxMsg>()
    let closed = false

    void (async () => {
      while (!closed) {
        const entry = await hub.inboxWait()
        if (closed) break
        const env = entry.msg
        const msg = env as InboxMsg
        msg.ack = async () => {}
        msg.nak = async (delayMs = 100) => {
          setTimeout(() => hub.inboxPush(entry), delayMs)
        }
        msg.term = async () => {}
        q.push(msg)
      }
    })()

    return {
      [Symbol.asyncIterator]() {
        return q[Symbol.asyncIterator]()
      },
      async close() {
        closed = true
        q.close()
      },
    }
  }

  async objectPut(name: string, data: Uint8Array): Promise<void> {
    this.hub.objectPut(name, data)
  }

  async objectGet(name: string): Promise<Uint8Array | null> {
    return this.hub.objectGet(name)
  }

  async kvCreate(
    bucket: string,
    key: string,
    value: string,
    ttlMs: number,
  ): Promise<number | null> {
    return this.hub.kvCreate(bucket, key, value, ttlMs)
  }

  async kvPut(
    bucket: string,
    key: string,
    value: string,
    ttlMs: number,
  ): Promise<void> {
    this.hub.kvPut(bucket, key, value, ttlMs)
  }

  async kvGet(bucket: string, key: string): Promise<string | null> {
    return this.hub.kvGet(bucket, key)
  }

  async kvCas(
    bucket: string,
    key: string,
    value: string,
    revision: number,
  ): Promise<number | null> {
    return this.hub.kvCas(bucket, key, value, revision)
  }

  async kvDelete(bucket: string, key: string): Promise<void> {
    this.hub.kvDelete(bucket, key)
  }

  async close(): Promise<void> {}
}

/**
 * Create an inproc hub and an agent-side bus attached to it. The returned
 * `hub` is the handle: pass it to `createInprocBus(hub)` to attach the
 * extension-side (or additional) buses. All buses sharing one hub see each
 * other, mirroring a real broker.
 */
export function createInprocHub(): { bus: Bus; hub: InprocHub } {
  const hub = new InprocHub()
  return { bus: new InprocBus(hub), hub }
}

/** Attach an inproc bus to an existing hub handle. */
export function createInprocBus(hub: InprocHub): Bus {
  return new InprocBus(hub)
}

/** Create a pair of buses sharing one hub (agent side + extension side). */
export function newInprocPair(): {
  agentBus: Bus
  extensionBus: Bus
  hub: InprocHub
} {
  const hub = new InprocHub()
  return { agentBus: new InprocBus(hub), extensionBus: new InprocBus(hub), hub }
}
