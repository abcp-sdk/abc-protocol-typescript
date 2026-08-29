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
  sessionToken,
  type SubscribeOpts,
  type Subscription,
} from '@abc-protocol/sdk'
import {
  type ConsumerMessages,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream'
import { Kvm } from '@nats-io/kv'
import type {
  NatsConnection,
  Subscription as NatsSub,
} from '@nats-io/nats-core'
import { Objm } from '@nats-io/obj'
import { connect } from '@nats-io/transport-node'

const CAPS: Caps = {
  requestReply: true,
  broadcast: true,
  pubSub: true,
  durableInbox: true,
  object: true,
  kv: true,
}

const STREAM_MAILBOX = 'ABC_MAILBOX'
const INBOX_WILDCARD = 'abc.mailbox.>'
const OBJECT_BUCKET = 'ABC_TOOL'

async function ensureMailboxStream(nc: NatsConnection): Promise<void> {
  const jsm = await jetstreamManager(nc)
  try {
    await jsm.streams.info(STREAM_MAILBOX)
  } catch {
    await jsm.streams.add({
      name: STREAM_MAILBOX,
      subjects: ['abc.mailbox.>', 'abc.session.events.>'],
      max_age: 24 * 3600 * 1_000_000_000,
    })
  }
}

function decode(m: { data: Uint8Array }): Envelope | null {
  const parsed = EnvelopeSchema.safeParse(
    JSON.parse(Buffer.from(m.data).toString('utf8')),
  )
  return parsed.success ? parsed.data : null
}

function encode(payload: unknown): Buffer {
  return Buffer.from(JSON.stringify(payload))
}

export class NatsBus implements Bus {
  readonly caps = CAPS
  constructor(private readonly nc: NatsConnection) {}

  async request(
    ch: string,
    payload: unknown,
    opts: RequestOpts = {},
  ): Promise<Envelope> {
    const body: Record<string, unknown> = { v: 1, ch, kind: 'req', payload }
    if (opts.sessionName !== undefined) body.session_name = opts.sessionName
    const m = await this.nc.request(ch, encode(body), {
      timeout: opts.timeoutMs ?? 2000,
    })
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
    this.nc.publish(
      ch,
      encode({ v: 1, ch, kind: 'pub', payload }),
      replyTo === undefined ? {} : { reply: replyTo },
    )
  }

  async subscribe(ch: string, opts?: SubscribeOpts): Promise<Subscription> {
    const sub =
      opts?.queue !== undefined
        ? this.nc.subscribe(ch, { queue: opts.queue })
        : this.nc.subscribe(ch)
    return {
      [Symbol.asyncIterator]() {
        return decodeIter(sub)
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
    const body: Record<string, unknown> = {
      v: 1,
      ch,
      kind: 'queue',
      id: opts.id,
      payload,
    }
    if (opts.sessionName !== undefined) body.session_name = opts.sessionName
    await js.publish(ch, encode(body), { msgID: opts.id })
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
      .add(STREAM_MAILBOX, {
        durable_name: durable,
        ack_policy: 'explicit',
        filter_subjects: [subject],
        ack_wait: 30_000_000_000,
        max_deliver: -1,
        max_ack_pending: 1024,
      })
      .catch(() => {})
    const js = jetstream(this.nc)
    const consumer = await js.consumers.get(STREAM_MAILBOX, durable)
    const messages = await consumer.consume()
    return {
      [Symbol.asyncIterator]() {
        return inboxIter(messages)
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

  async kvCreate(
    bucket: string,
    key: string,
    value: string,
    ttlMs: number,
  ): Promise<number | null> {
    const kv = await new Kvm(this.nc).create(bucket, { ttl: ttlMs })
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
    const kv = await new Kvm(this.nc).create(bucket, { ttl: ttlMs })
    await kv.put(key, Buffer.from(value))
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

  async close(): Promise<void> {
    // Drain may reject with ClosedConnectionError when the server already
    // tore down interest (e.g. after consumer/sub teardown races); close is
    // best-effort.
    await this.nc.drain().catch(() => {})
  }
}

async function* decodeIter(sub: NatsSub): AsyncGenerator<Envelope> {
  for await (const m of sub) {
    const env = decode(m)
    if (env === null) continue
    if (m.reply !== '') env.reply_to = m.reply
    yield env
  }
}

async function* inboxIter(
  messages: ConsumerMessages,
): AsyncGenerator<InboxMsg> {
  for await (const m of messages) {
    const env = decode(m)
    if (env === null) continue
    const msg = env as InboxMsg
    msg.ack = () => Promise.resolve(m.ack())
    msg.nak = (delayMs?: number) => Promise.resolve(m.nak(delayMs))
    msg.term = () => Promise.resolve(m.term())
    yield msg
  }
}

export async function connectNatsBus(url?: string): Promise<NatsBus> {
  const servers = url ?? process.env.NATS_URL ?? 'nats://127.0.0.1:4222'
  const nc = await connect({ servers })
  await ensureMailboxStream(nc)
  return new NatsBus(nc)
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
