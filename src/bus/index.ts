import type { Envelope } from '../protocol/envelope.js'

export type { Envelope }

export type RequestOpts = {
  /** Timeout in ms for a 1:1 request. */
  timeoutMs?: number
  /** For 1:N collects: how long to gather replies. */
  maxWaitMs?: number
  /** Rides the envelope's first-class session_name field. */
  sessionName?: string
  /** Rides the envelope's first-class tenant field (required on data-plane). */
  tenant?: string
}

export type SubscribeOpts = {
  /** Queue group: competing subscribers sharing the group load-share. */
  queue?: string
}

export type InboxConsumeOpts = {
  /** Channel (or wildcard) to consume. Defaults to the mailbox wildcard. */
  subject?: string
}

export type PublishOpts = {
  /** Rides the envelope's first-class tenant field. */
  tenant?: string
  /** Transport-internal reply address (set by request handling). */
  replyTo?: string
}

export type InboxPublishOpts = {
  /** Publisher-side idempotency key (also the message id). */
  id: string
  /** Rides the envelope's session_name so the consumer routes to the session. */
  sessionName?: string
  /** Rides the envelope's first-class tenant field. */
  tenant?: string
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
  /** Terminate without copying to the dead-letter stream. */
  termNoDLQ(): Promise<void>
}

export interface KvEvent {
  key: string
  value: string
  revision: number
  deleted: boolean
  isUpdate: boolean
}

/**
 * Transport-agnostic object storage (bytes). The NATS bus implements this
 * natively (JetStream object stores `ABC_TOOL` transient / `ABC_FILES`
 * durable); a deployment may inject an S3-compatible implementation so
 * long-lived file bytes never sit in NATS. Exactly ONE backend is used per
 * class — there is no read fallback.
 */
export interface ObjectStore {
  /** Store a transient object (short-lived: tool payloads, catalog caches). */
  objectPut(name: string, data: Uint8Array): Promise<void>
  /** Fetch a transient object; null when absent. */
  objectGet(name: string): Promise<Uint8Array | null>
  /** Store a durable object (file bytes). */
  objectPutPersistent(name: string, data: Uint8Array): Promise<void>
  /** Fetch a durable object; null when absent. */
  objectGetPersistent(name: string): Promise<Uint8Array | null>
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
export interface Bus extends ObjectStore {
  /** 1:1 request; the transport manages the reply address internally. */
  request(ch: string, payload: unknown, opts?: RequestOpts): Promise<Envelope>

  /** 1:N request; collects replies until maxWaitMs. */
  requestMany(
    ch: string,
    payload: unknown,
    opts?: RequestOpts,
  ): Promise<Envelope[]>

  /** Fire-and-forget publish. No reply address. */
  publish(ch: string, payload: unknown, opts?: PublishOpts): Promise<void>

  /** Live subscription (opts.queue enables a competing queue group). */
  subscribe(ch: string, opts?: SubscribeOpts): Promise<Subscription>

  /**
   * Ordered stream subscription: FIRST every retained message from
   * `startTimeMs` (or from "now" when omitted), THEN live messages — over a
   * single ordered consumer with no polling and no replay/live handover race.
   */
  subscribeStream(
    ch: string,
    opts?: { startTimeMs?: number },
  ): Promise<Subscription>

  /** Durable inbox publish (at-least-once). */
  inboxPublish(
    ch: string,
    payload: unknown,
    opts: InboxPublishOpts,
  ): Promise<void>

  /** Durable inbox consume with explicit ack/nak/term. */
  inboxConsume(opts?: InboxConsumeOpts): Promise<InboxSubscription>

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
  /** Watch bucket entries matching keys (NATS wildcard). Snapshot entries
   * arrive first, then live updates. NOTE: isUpdate is advisory — some
   * client versions mark replayed snapshot entries as updates; consumers
   * must be idempotent (apply-by-revision), not snapshot/update-sensitive. */
  kvWatch(
    bucket: string,
    keys: string,
  ): Promise<{
    stream: AsyncIterable<KvEvent>
    stop(): Promise<void>
  }>
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
