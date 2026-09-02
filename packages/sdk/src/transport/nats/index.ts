import {
  type ConsumerMessages,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream'
import { Kvm } from '@nats-io/kv'
import {
  headers,
  type MsgHdrs,
  type NatsConnection,
  type Subscription as NatsSub,
} from '@nats-io/nats-core'
import { Objm } from '@nats-io/obj'
import { connect } from '@nats-io/transport-node'
import type {
  Bus,
  Envelope,
  InboxConsumeOpts,
  InboxMsg,
  InboxPublishOpts,
  InboxSubscription,
  RequestOpts,
  SubscribeOpts,
  Subscription,
} from '../../bus/index.js'
import { authHeader, type Identity, verify } from '../../identity.js'
import { EnvelopeSchema, sessionToken } from '../../protocol/index.js'

const STREAM_MAILBOX = 'ABC_MAILBOX'
const STREAM_EVENTS = 'ABC_EVENTS'
const STREAM_DLQ = 'ABC_DLQ'
const INBOX_WILDCARD = 'abc.mailbox.>'
const EVENTS_PREFIX = 'abc.session.events.'
const DLQ_PREFIX = 'abc.dlq.'
const OBJECT_BUCKET = 'ABC_TOOL'
const OBJECT_BUCKET_PERSISTENT = 'ABC_FILES'

// Two streams, two consumption models: the mailbox is a work queue
// (competing consumers, ack on done), session events are a replayable
// per-session log. Subjects are disjoint by design.
function streamFor(subject: string): string {
  if (subject.startsWith(EVENTS_PREFIX)) return STREAM_EVENTS
  if (subject.startsWith(DLQ_PREFIX)) return STREAM_DLQ
  return STREAM_MAILBOX
}

// dlqSubjectFor maps an original queue subject to its dead-letter subject.
function dlqSubjectFor(subject: string): string {
  const token = subject.substring(subject.lastIndexOf('.') + 1)
  return DLQ_PREFIX + token
}

export interface NatsConnectOptions {
  /** Stream retention ceiling; 0 = default (24h). */
  maxAgeMs?: number
  /** JetStream replica count at stream creation; 0 = server default (1).
   * Cannot change after creation. */
  replicas?: number
  /** Opt-in message authentication: outgoing messages carry abc-id/abc-sig
   * NATS headers (HMAC), incoming messages are verified. Undefined
   * (default) = zero auth overhead, everything passes. */
  identity?: Identity
}

function sameSubjects(a: string[] | undefined, b: string[]): boolean {
  if (a === undefined) return false
  if (a.length !== b.length) return false
  const set = new Set(a)
  return b.every(s => set.has(s))
}

async function ensureStreams(
  nc: NatsConnection,
  opts: NatsConnectOptions = {},
): Promise<void> {
  const jsm = await jetstreamManager(nc)
  const maxAge = opts.maxAgeMs ?? 24 * 3600 * 1_000_000_000
  const specs = [
    { name: STREAM_MAILBOX, subjects: ['abc.mailbox.>'] },
    { name: STREAM_EVENTS, subjects: ['abc.session.events.>'] },
    { name: STREAM_DLQ, subjects: ['abc.dlq.>'] },
  ]
  // Create in order; when the events stream overlaps a legacy mailbox
  // stream (pre-.2), narrow the drifted stream first and retry.
  for (const s of specs) {
    let info: Awaited<ReturnType<typeof jsm.streams.info>> | undefined
    try {
      info = await jsm.streams.info(s.name)
    } catch {
      try {
        await jsm.streams.add({
          name: s.name,
          subjects: s.subjects,
          max_age: maxAge,
          ...(opts.replicas ? { num_replicas: opts.replicas } : {}),
        })
        continue
      } catch (err) {
        if (
          s.name === STREAM_EVENTS &&
          String(err).includes('subjects overlap')
        ) {
          const mi = await jsm.streams.info(STREAM_MAILBOX)
          if (mi.config.subjects?.includes('abc.session.events.>')) {
            await jsm.streams.update(STREAM_MAILBOX, {
              subjects: ['abc.mailbox.>'],
            })
            await jsm.streams.add({
              name: s.name,
              subjects: s.subjects,
              max_age: maxAge,
              ...(opts.replicas ? { num_replicas: opts.replicas } : {}),
            })
            continue
          }
        }
        throw err
      }
    }
    // drift repair: subjects must match the desired set
    if (!sameSubjects(info.config.subjects, s.subjects)) {
      await jsm.streams.update(s.name, { subjects: s.subjects })
    }
  }
  void jsm
}

function decode(m: { data: Uint8Array }): Envelope | null {
  const raw = JSON.parse(Buffer.from(m.data).toString('utf8')) as Record<
    string,
    unknown
  >
  if (raw.v !== undefined && raw.v !== 1) {
    console.warn(
      `[abc] envelope version ${String(raw.v)} on ${String(raw.ch)} (this build speaks v1); fields may be misinterpreted`,
    )
  }
  const parsed = EnvelopeSchema.safeParse(raw)
  return parsed.success ? parsed.data : null
}

function encode(payload: unknown): Buffer {
  return Buffer.from(JSON.stringify(payload))
}

// buildEnvelope assembles the wire envelope in one place. The optional
// fields ride only when set, mirroring the zod optional() semantics.
function buildEnvelope(
  kind: string,
  ch: string,
  payload: unknown,
  opts: {
    id?: string | undefined
    sessionName?: string | undefined
    replyTo?: string | undefined
  } = {},
): Buffer {
  const body: Record<string, unknown> = { v: 1, ch, kind, payload }
  if (opts.id !== undefined && opts.id !== '') body.id = opts.id
  if (opts.sessionName !== undefined && opts.sessionName !== '')
    body.session_name = opts.sessionName
  if (opts.replyTo !== undefined && opts.replyTo !== '')
    body.reply_to = opts.replyTo
  return encode(body)
}

export class NatsBus implements Bus {
  constructor(
    private readonly nc: NatsConnection,
    private readonly idn?: Identity,
  ) {}

  private signMsg(
    msg: { subject: string; data: Uint8Array; headers?: MsgHdrs },
    kind: string,
    id: string,
  ): void {
    if (this.idn === undefined) return
    const parsed = JSON.parse(Buffer.from(msg.data).toString('utf8')) as {
      payload?: unknown
    }
    const h = authHeader(this.idn, {
      ch: msg.subject,
      kind,
      id,
      payload: parsed.payload,
    })
    if (msg.headers === undefined) msg.headers = headers()
    msg.headers.set('abc-id', h['abc-id'])
    msg.headers.set('abc-sig', h['abc-sig'])
  }

  verifyMsg(m: {
    data: Uint8Array
    subject: string
    headers?: MsgHdrs
  }): boolean {
    if (this.idn === undefined) return true
    const sig = m.headers?.get('abc-sig')
    if (sig === undefined || sig === '') return false
    const raw = JSON.parse(Buffer.from(m.data).toString('utf8')) as {
      kind?: string
      id?: string
      payload?: unknown
    }
    return verify(
      m.headers?.get('abc-id') ?? '',
      this.idn.secret,
      {
        ch: m.subject,
        kind: raw.kind ?? '',
        id: raw.id ?? '',
        payload: raw.payload,
      },
      sig,
    )
  }

  async request(
    ch: string,
    payload: unknown,
    opts: RequestOpts = {},
  ): Promise<Envelope> {
    const wire = buildEnvelope('req', ch, payload, {
      sessionName: opts.sessionName,
    })
    // timeoutMs > 0 bounds the request; 0/unset means "application bounds
    // it" — the ceiling here only guards a stuck request (nats.js would
    // otherwise apply its own short default).
    const t = opts.timeoutMs && opts.timeoutMs > 0 ? opts.timeoutMs : 600_000
    const reqOpts: { timeout: number; headers?: MsgHdrs } = { timeout: t }
    if (this.idn !== undefined) {
      const msg: { subject: string; data: Uint8Array; headers?: MsgHdrs } = {
        subject: ch,
        data: wire,
      }
      this.signMsg(msg, 'req', '')
      if (msg.headers !== undefined) reqOpts.headers = msg.headers
    }
    const m = await this.nc.request(ch, wire, reqOpts)
    if (this.idn !== undefined && !this.verifyMsg(m)) {
      throw new Error(`identity: signature verification failed on ${ch}`)
    }
    const env = decode(m)
    if (env === null) throw new Error(`invalid envelope from ${ch}`)
    return env
  }

  async requestMany(
    ch: string,
    payload: unknown,
    opts: RequestOpts = {},
  ): Promise<Envelope[]> {
    const replies = await this.nc.requestMany(
      ch,
      encode({ v: 1, ch, kind: 'req', payload }),
      {
        strategy: 'timer',
        maxWait: opts.maxWaitMs ?? 500,
      },
    )
    const out: Envelope[] = []
    for await (const m of replies) {
      const env = decode(m)
      if (env !== null) out.push(env)
    }
    return out
  }

  async publish(ch: string, payload: unknown, replyTo?: string): Promise<void> {
    const data = buildEnvelope('pub', ch, payload, { replyTo })
    if (this.idn !== undefined) {
      const msg: { subject: string; data: Uint8Array; headers?: MsgHdrs } = {
        subject: ch,
        data,
      }
      this.signMsg(msg, 'pub', '')
      const pubOpts: { reply?: string; headers?: MsgHdrs } = {}
      if (replyTo !== undefined) pubOpts.reply = replyTo
      if (msg.headers !== undefined) pubOpts.headers = msg.headers
      await this.nc.publish(ch, data, pubOpts)
    } else {
      await this.nc.publish(
        ch,
        data,
        replyTo === undefined ? {} : { reply: replyTo },
      )
    }
  }

  async subscribe(ch: string, opts?: SubscribeOpts): Promise<Subscription> {
    const sub =
      opts?.queue !== undefined
        ? this.nc.subscribe(ch, { queue: opts.queue })
        : this.nc.subscribe(ch)
    const bus = this
    return {
      [Symbol.asyncIterator]() {
        return decodeIter(sub, bus)
      },
      async close() {
        sub.unsubscribe()
      },
    }
  }

  async inboxPublish(
    ch: string,
    payload: unknown,
    opts: InboxPublishOpts,
  ): Promise<void> {
    const js = jetstream(this.nc)
    const wire = buildEnvelope('queue', ch, payload, {
      id: opts.id,
      sessionName: opts.sessionName,
    })
    await js.publish(ch, wire, { msgID: opts.id })
  }

  async inboxConsume(opts?: InboxConsumeOpts): Promise<InboxSubscription> {
    const jsm = await jetstreamManager(this.nc)
    const subject = opts?.subject ?? INBOX_WILDCARD
    // A durable consumer's filter subject is fixed at creation, so each
    // distinct subject needs its own durable name (else re-binding a shared
    // durable to a different filter silently delivers nothing).
    const durable =
      subject === INBOX_WILDCARD
        ? 'abc-mailbox-push'
        : `abc-mailbox-push-${sessionToken(subject)}`
    await jsm.consumers
      .add(streamFor(subject), {
        durable_name: durable,
        ack_policy: 'explicit',
        filter_subjects: [subject],
        ack_wait: 30_000_000_000,
        max_deliver: -1,
        max_ack_pending: 1024,
      })
      .catch(() => {})
    const js = jetstream(this.nc)
    const consumer = await js.consumers.get(streamFor(subject), durable)
    const messages = await consumer.consume()
    const nc = this.nc
    return {
      [Symbol.asyncIterator]() {
        return inboxIter(nc, messages)
      },
      async close() {
        await messages.close()
      },
    }
  }

  async objectPut(name: string, data: Uint8Array): Promise<void> {
    const os = await new Objm(this.nc).create(OBJECT_BUCKET, {
      ttl: 24 * 3600 * 1_000_000_000,
    })
    await os.putBlob({ name }, data)
  }

  async objectGet(name: string): Promise<Uint8Array | null> {
    try {
      const os = await new Objm(this.nc).open(OBJECT_BUCKET)
      return await os.getBlob(name)
    } catch {
      return null
    }
  }

  async objectPutPersistent(name: string, data: Uint8Array): Promise<void> {
    const os = await new Objm(this.nc).create(OBJECT_BUCKET_PERSISTENT, {})
    await os.putBlob({ name }, data)
  }

  async objectGetPersistent(name: string): Promise<Uint8Array | null> {
    try {
      const os = await new Objm(this.nc).open(OBJECT_BUCKET_PERSISTENT)
      return await os.getBlob(name)
    } catch {
      return null
    }
  }

  async kvCreate(
    bucket: string,
    key: string,
    value: string,
    ttlMs: number,
  ): Promise<number | null> {
    const kv = await this.openKv(bucket, ttlMs)
    try {
      return await kv.create(key, Buffer.from(value))
    } catch {
      return null
    }
  }
  async kvPut(
    bucket: string,
    key: string,
    value: string,
    ttlMs: number,
  ): Promise<void> {
    const kv = await this.openKv(bucket, ttlMs)
    await kv.put(key, Buffer.from(value))
  }
  async kvWatch(
    bucket: string,
    keys: string,
  ): Promise<{
    stream: AsyncIterable<import('../../bus/index.js').KvEvent>
    stop(): Promise<void>
  }> {
    const kv = await this.openKv(bucket, 0)
    const iter = await kv.watch({ key: keys })
    const stream: AsyncIterable<import('../../bus/index.js').KvEvent> =
      (async function* () {
        for await (const e of iter) {
          const deleted = e.operation === 'DEL' || e.operation === 'PURGE'
          yield {
            key: e.key,
            value: deleted ? '' : e.string(),
            revision: e.revision,
            deleted,
            isUpdate: e.isUpdate,
          }
        }
      })()
    return { stream, stop: async () => iter.stop() }
  }

  async kvGet(bucket: string, key: string): Promise<string | null> {
    try {
      const kv = await new Kvm(this.nc).open(bucket)
      const e = await kv.get(key)
      if (e === null || e.operation === 'DEL' || e.operation === 'PURGE') {
        return null
      }
      return Buffer.from(e.value).toString('utf8')
    } catch {
      return null
    }
  }
  async kvCas(
    bucket: string,
    key: string,
    value: string,
    revision: number,
  ): Promise<number | null> {
    try {
      const kv = await new Kvm(this.nc).open(bucket)
      return await kv.update(key, Buffer.from(value), revision)
    } catch {
      return null
    }
  }
  async kvDelete(bucket: string, key: string): Promise<void> {
    try {
      const kv = await new Kvm(this.nc).open(bucket)
      await kv.delete(key)
    } catch {
      // absent is fine
    }
  }

  /**
   * Replay the retained queue envelopes for a channel (JetStream stream
   * contents via an ephemeral ordered-ish consumer), oldest first.
   */
  /**
   * Replay the retained queue envelopes for a channel via an ephemeral
   * JetStream consumer (deliver_policy all), oldest first.
   */
  /** create-or-open: creating an existing bucket with a different config
   * (e.g. another per-call TTL) errors, so fall back to opening it. */
  private async openKv(
    bucket: string,
    ttlMs: number,
  ): Promise<Awaited<ReturnType<Kvm['create']>>> {
    try {
      return await new Kvm(this.nc).create(bucket, { ttl: ttlMs })
    } catch {
      return new Kvm(this.nc).open(bucket)
    }
  }

  async replay(ch: string): Promise<Envelope[]> {
    const out: Envelope[] = []
    const jsm = await jetstreamManager(this.nc).catch(() => null)
    if (jsm === null) return out
    // Ephemeral consumer (no durable name) over the mailbox stream,
    // filtered to the exact subject, delivering the whole retained window.
    const consumer = await jsm.consumers
      .add(streamFor(ch), {
        filter_subjects: [ch],
        ack_policy: 'explicit',
        deliver_policy: 'all',
        inactive_threshold: 60_000_000_000,
      })
      .catch(() => null)
    if (consumer === null) return out
    try {
      const js = jetstream(this.nc)
      const c = await js.consumers.get(streamFor(ch), consumer.name)
      // fetch() terminates after `expires` — bounded replay, no live tail.
      const messages = await c.fetch({ expires: 1_000, max_messages: 1024 })
      for await (const m of messages) {
        const env = decode(m)
        if (env !== null) out.push(env)
        m.ack()
      }
    } catch {
      // Stream missing / no retained messages — empty replay.
    }
    await jsm.consumers.delete(streamFor(ch), consumer.name).catch(() => {})
    return out
  }

  async close(): Promise<void> {
    // Drain may reject with ClosedConnectionError when the server already
    // tore down interest (e.g. after consumer/sub teardown races); close is
    // best-effort.
    await this.nc.drain().catch(() => {})
  }
}

async function* decodeIter(
  sub: NatsSub,
  bus?: NatsBus,
): AsyncGenerator<Envelope> {
  for await (const m of sub) {
    if (bus !== undefined && !bus.verifyMsg(m)) continue // bad sig: drop
    const env = decode(m)
    if (env === null) continue
    if (m.reply !== '') env.reply_to = m.reply
    yield env
  }
}

async function* inboxIter(
  nc: NatsConnection,
  messages: ConsumerMessages,
): AsyncGenerator<InboxMsg> {
  for await (const m of messages) {
    const env = decode(m)
    if (env === null) continue
    const msg = env as InboxMsg
    msg.ack = () => Promise.resolve(m.ack())
    msg.nak = (delayMs?: number) => Promise.resolve(m.nak(delayMs))
    // Term copies the message to the dead-letter stream first.
    msg.term = async () => {
      const js = jetstream(nc)
      const opts = env.id !== undefined ? { msgID: env.id } : {}
      await js
        .publish(dlqSubjectFor(m.subject), Buffer.from(m.data), opts)
        .catch(() => {})
      await m.term()
    }
    msg.termNoDLQ = () => Promise.resolve(m.term())
    yield msg
  }
}

export async function connectNatsBus(
  url?: string,
  opts: NatsConnectOptions = {},
): Promise<NatsBus> {
  const servers = url ?? process.env.NATS_URL ?? 'nats://127.0.0.1:4222'
  const nc = await connect({ servers })
  await ensureStreams(nc, opts)
  return new NatsBus(nc, opts.identity)
}

/**
 * Symmetric NATS transport provider. NATS is peer-to-peer: both agent and
 * extension are equal clients over an external broker. `side` is accepted for
 * signature parity with the other providers, but no authorization is derived
 * from it — any bus can call any channel that its identity is permitted to.
 * Returns the identical `Bus` type as other providers.
 */
export async function createNatsBus(
  options: { url?: string; side?: 'agent' | 'extension' } = {},
): Promise<{ bus: Bus }> {
  return { bus: await connectNatsBus(options.url) }
}
