import { createHmac, timingSafeEqual } from 'node:crypto'

/**
 * Static pre-shared identity. Every agent and extension holds an `id` and a
 * `secret`. Messages carry the sender's `abc-id` plus an HMAC (`abc-sig`) over
 * the canonical fields, so receivers authenticate the sender without a CA.
 * Revocation/rotation are deployment-level (redeploy with a new secret).
 */

export interface Identity {
  id: string
  secret: string
}

/** Build the header a sender attaches to an outgoing message. */
export function authHeader(
  identity: Identity,
  fields: { ch: string; kind: string; id?: string; payload?: unknown },
): { 'abc-id': string; 'abc-sig': string } {
  return {
    'abc-id': identity.id,
    'abc-sig': sign(identity, fields),
  }
}

function canonical(
  id: string,
  fields: { ch: string; kind: string; id?: string; payload?: unknown },
): string {
  const payload =
    fields.payload === undefined ? '' : JSON.stringify(fields.payload)
  const msgId = fields.id ?? ''
  return `${id}\n${fields.ch}\n${fields.kind}\n${msgId}\n${payload}`
}

/**
 * Verify an HMAC against a known secret. Returns true only when the digest
 * matches. Constant-time compare.
 */
export function verify(
  claimedId: string,
  secret: string,
  fields: { ch: string; kind: string; id?: string; payload?: unknown },
  provided: string,
): boolean {
  const want = sign({ id: claimedId, secret }, fields)
  // Compare the base64url strings as bytes (both sides encode the same way;
  // decoding only one side would compare UTF-8 text with raw HMAC bytes).
  const a = Buffer.from(want)
  const b = Buffer.from(provided)
  return a.length === b.length && timingSafeEqual(a, b)
}

function sign(
  identity: Identity,
  fields: { ch: string; kind: string; id?: string; payload?: unknown },
): string {
  return createHmac('sha256', identity.secret)
    .update(canonical(identity.id, fields))
    .digest('base64url')
}
