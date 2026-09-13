import { z } from '../zod.js'
import { validateTenant } from './tenant.js'

/**
 * ABC — Agent Bus Communication Protocol.
 *
 * Wire contract between an **agent** and **extension servers**. Three message
 * primitives ride the single transport (NATS + JetStream):
 *
 *   - `req`   — ask one (or many) and await the answer(s); the transport
 *               manages the reply address internally.
 *   - `pub`   — broadcast / event, fire-and-forget; no reply address.
 *   - `queue` — durable inbox (at-least-once + idempotent + ack/nak/term).
 *
 * v2 adds multi-tenant namespacing: every data-plane subject is
 * `abc.<tenant>.<...>` and every envelope carries the same `tenant` so a
 * receiver can reject a mismatch. v1 envelopes are rejected outright.
 */

export const PROTOCOL_VERSION = 2

export const EnvelopeKindSchema = z.enum(['req', 'pub', 'queue'])
export type EnvelopeKind = z.infer<typeof EnvelopeKindSchema>

/**
 * The generic bus envelope. `payload` carries the business body.
 *
 * `reply_to` is transport-internal: it is present on a `req` so the answering
 * side knows where to `publish` its result. Callers must never set it.
 *
 * `session_name` is the session's single primary key, carried where relevant
 * so extensions know which logical session a message belongs to.
 */
export const EnvelopeSchema = z.object({
  v: z.number().int().default(PROTOCOL_VERSION),
  /**
   * Tenant the message belongs to. MUST equal the second subject segment for
   * every data-plane channel (`abc.<tenant>.<...>`); the global control-plane
   * subject (`abc.discover`) also carries the requesting tenant so an
   * extension can attribute the request.
   */
  tenant: z.string(),
  /**
   * @deprecated Informational copy of the subject. The NATS subject is
   * the authoritative routing truth; consumers MUST NOT dispatch on this
   * field. SDKs populate it from the subject on decode (not from the
   * wire). Planned for removal in v1.1 (senders will stop writing it).
   */
  ch: z.string(),
  kind: EnvelopeKindSchema,
  id: z.string().optional(),
  session_name: z.string().optional(),
  reply_to: z.string().optional(),
  /**
   * Transport metadata (NOT set by publishers): the number of messages still
   * pending on the delivering consumer at the moment this message was
   * delivered. 0 = the consumer has caught up with the stream head (this
   * message is live); >0 = messages remain queued behind it (catch-up). Only
   * populated on ordered-stream deliveries (see `subscribeStream`).
   */
  pending: z.number().int().optional(),
  payload: z
    .unknown()
    .openapi({ description: 'Opaque business body.' })
    .optional(),
})
export type Envelope = z.infer<typeof EnvelopeSchema>

/**
 * Validate the tenant carried on a decoded envelope against the subject it
 * arrived on. Control-plane subjects (no tenant segment) only require a
 * well-formed tenant; data-plane subjects require an exact match. Returns the
 * validated tenant, or throws.
 */
export function validateEnvelopeTenant(
  ch: string,
  tenant: string,
  subjectTenant: string | null,
): string {
  const t = validateTenant(tenant)
  if (subjectTenant !== null && t !== subjectTenant) {
    throw new Error(
      `envelope tenant ${JSON.stringify(t)} does not match subject tenant ${JSON.stringify(subjectTenant)} on ${ch}`,
    )
  }
  return t
}
