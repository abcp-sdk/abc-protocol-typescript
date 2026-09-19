import type { ErrorCode } from '../protocol/error.js'

/**
 * The protocol error codes a TOOL execution may surface to the agent/model.
 * Mirrors the Go SDK's `ToolResultErrorCode` (and the shared `ErrorCode` enum):
 * a plain thrown error maps to `internal`; a `TypedToolError` preserves its
 * code on the wire so the agent can react (e.g. `not_found` vs `retryable`)
 * instead of treating everything as an opaque failure.
 */
export type ToolErrorCode = Extract<
  ErrorCode,
  | 'permission_denied'
  | 'retryable'
  | 'invalid_argument'
  | 'not_found'
  | 'internal'
  | 'business'
>

/**
 * Throw this from a tool `execute` to return a TYPED tool error. The extension
 * SDK inspects the thrown value and forwards `{ code, message }` verbatim;
 * any other thrown value becomes `{ code: 'internal', message: String(err) }`.
 *
 * ```ts
 * throw new TypedToolError('not_found', `no such file: ${path}`)
 * ```
 */
export class TypedToolError extends Error {
  readonly code: ToolErrorCode

  constructor(code: ToolErrorCode, message: string) {
    super(message)
    this.name = 'TypedToolError'
    this.code = code
  }
}

/** Narrow an unknown thrown value to its on-wire `{ code, message }` shape. */
export function toToolError(e: unknown): { code: ToolErrorCode; message: string } {
  if (e instanceof TypedToolError) {
    return { code: e.code, message: e.message }
  }
  return { code: 'internal', message: String(e) }
}
