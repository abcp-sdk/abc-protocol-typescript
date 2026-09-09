import { z } from '../zod.js'

/**
 * Standard error codes, so agents can react reliably across process and
 * language boundaries instead of parsing free-text.
 */
export const ErrorCodeSchema = z.enum([
  'permission_denied',
  'retryable',
  'invalid_argument',
  'not_found',
  'internal',
  'business',
])
export type ErrorCode = z.infer<typeof ErrorCodeSchema>

export const ErrorPayloadSchema = z.object({
  code: ErrorCodeSchema,
  message: z.string(),
})
export type ErrorPayload = z.infer<typeof ErrorPayloadSchema>
