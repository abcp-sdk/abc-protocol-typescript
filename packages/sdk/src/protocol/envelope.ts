import { z } from '../zod.js'

/**
 * ABC — Agent Bus Communication Protocol.
 *
 * Wire contract between an **agent** and **extension servers**. Three message
 * primitives ride every transport (inproc / nats / ws):
 *
 *   - `req`   — ask one (or many) and await the answer(s); the transport
 *               manages the reply address internally.
 *   - `pub`   — broadcast / event, fire-and-forget; no reply address.
 *   - `queue` — durable inbox (at-least-once + idempotent + ack/nak/term).
 */

export const PROTOCOL_VERSION = 1

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
  v: z.number().int().default(1),
  ch: z.string(),
  kind: EnvelopeKindSchema,
  id: z.string().optional(),
  session_name: z.string().optional(),
  reply_to: z.string().optional(),
  payload: z
    .unknown()
    .openapi({ description: 'Opaque business body.' })
    .optional(),
})
export type Envelope = z.infer<typeof EnvelopeSchema>
