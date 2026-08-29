import type { Envelope } from '../protocol/envelope.js'

export type { Envelope }

export type RequestOpts = {
  /** Timeout in ms for a 1:1 request. */
  timeoutMs?: number
  /** For 1:N collects: how long to gather replies. */
  maxWaitMs?: number
  /** Rides the envelope's first-class session_name field. */
  sessionName?: string
}

export type SubscribeOpts = {
  /** Queue group: competing subscribers sharing the group load-share. */
  queue?: string
}

export type InboxConsumeOpts = {
  /** Channel (or wildcard) to consume. Defaults to the mailbox wildcard. */
  subject?: string
}

export type InboxPublishOpts = {
  /** Publisher-side idempotency key (also the message id). */
  id: string
  /** Rides the envelope's session_name so the consumer routes to the session. */
  sessionName?: string
}

/** A live subscription handle; yields envelopes as they arrive. */
export interface Subscription {
  [Symbol.asyncIterator](): AsyncIterator<Envelope>
  close(): Promise<void>
}

/** A durable-inbox message with explicit ack/nak/term semantics. */
export interface InboxMsg extends Envelope {
  kind: 'queue'
  ack(): Promise<void>
  /** Negative-ack: redeliver after a delay. */
  nak(delayMs?: number): Promise<void>
  /** Poison: drop permanently. */
  term(): Promise<void>
}

export interface InboxSubscription {
  [Symbol.asyncIterator](): AsyncIterator<InboxMsg>
  close(): Promise<void>
}

/**
 * The transport-agnostic message bus. Role logic (agent/extension) depends
 * only on this interface; transports are adapters.
 */
/**
 * The transport-agnostic message bus. There is exactly one transport
 * (NATS); every listed capability is always available (JetStream).
 */
export interface Bus {
  /** 1:1 request; the transport manages the reply address internally. */
  request(ch: string, payload: unknown, opts?: RequestOpts): Promise<Envelope>

  /** 1:N request; collects replies until maxWaitMs. */
  requestMany(
    ch: string,
    payload: unknown,
    opts?: RequestOpts,
  ): Promise<Envelope[]>

  /** Fire-and-forget publish. No reply address. */
  publish(ch: string, payload: unknown): Promise<void>

  /** Live subscription (opts.queue enables a competing queue group). */
  subscribe(ch: string, opts?: SubscribeOpts): Promise<Subscription>

  /** Durable inbox publish (at-least-once). */
  inboxPublish(
    ch: string,
    payload: unknown,
    opts: InboxPublishOpts,
  ): Promise<void>

  /** Durable inbox consume with explicit ack/nak/term. */
  inboxConsume(opts?: InboxConsumeOpts): Promise<InboxSubscription>

  /**
   * Replay the retained (durably queued) envelopes for a channel, oldest
   * first. Retention is a transport property (NATS: stream max_age; inproc:
   * in-memory log); the protocol promise is a bounded recent window.
   */
  replay(ch: string): Promise<Envelope[]>

  /** Store a (potentially large) object; transports chunk internally. */
  objectPut(name: string, data: Uint8Array): Promise<void>

  /** Fetch an object; null when absent. */
  objectGet(name: string): Promise<Uint8Array | null>

  /** Atomic create (fails if the key exists); returns the revision. */
  kvCreate(
    bucket: string,
    key: string,
    value: string,
    ttlMs: number,
  ): Promise<number | null>

  /** Unconditional put (last-write-wins). */
  kvPut(
    bucket: string,
    key: string,
    value: string,
    ttlMs: number,
  ): Promise<void>

  /** Read a key; null when absent/expired. */
  kvGet(bucket: string, key: string): Promise<string | null>

  /** Compare-and-swap; returns the new revision or null when lost. */
  kvCas(
    bucket: string,
    key: string,
    value: string,
    revision: number,
  ): Promise<number | null>

  /** Delete a key. */
  kvDelete(bucket: string, key: string): Promise<void>

  close(): Promise<void>
}
