import { randomUUID } from 'node:crypto'

/**
 * Typed accessors for a tool/hook `arguments` bag, plus id/coercion helpers.
 * The TS mirror of the Go SDK's `protocol.Arg*` / `NewID` / `Coerce`: every
 * extension otherwise re-implements the same loose parsing, which drifts.
 * All readers are TOTAL — a missing or wrong-typed value yields the default.
 */

/** Read a string argument (missing / wrong type => ""). */
export function argString(args: Record<string, unknown>, key: string): string {
  const v = args[key]
  return typeof v === 'string' ? v : ''
}

/**
 * Read an integer argument (missing / non-finite => `def`). JSON numbers decode
 * as `number`; integral values only (a float is truncated toward zero, matching
 * the Go `ArgInt`).
 */
export function argInt(
  args: Record<string, unknown>,
  key: string,
  def: number,
): number {
  const v = args[key]
  return typeof v === 'number' && Number.isFinite(v) ? Math.trunc(v) : def
}

/** Read a float argument (missing / non-finite => `def`). */
export function argFloat(
  args: Record<string, unknown>,
  key: string,
  def: number,
): number {
  const v = args[key]
  return typeof v === 'number' && Number.isFinite(v) ? v : def
}

/** Read a boolean argument (missing / wrong type => `def`). */
export function argBool(
  args: Record<string, unknown>,
  key: string,
  def: boolean,
): boolean {
  const v = args[key]
  return typeof v === 'boolean' ? v : def
}

/** A fresh random UUID (the TS twin of Go's `protocol.NewID`). */
export function newId(): string {
  return randomUUID()
}

/**
 * Coerce a decoded value into a typed shape via a JSON round trip. Most TS
 * callers validate with zod, but this is the drop-in for Go's `Coerce` when a
 * loose structural cast is wanted. Returns the coerced value, or `undefined`
 * when the value cannot round-trip.
 */
export function coerce<T>(src: unknown): T | undefined {
  if (src === undefined) return undefined
  try {
    return JSON.parse(JSON.stringify(src)) as T
  } catch {
    return undefined
  }
}
