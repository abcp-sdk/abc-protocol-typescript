/**
 * extension-kit — shared primitives for tool `execute` handlers.
 *
 * Every extension repo (bundled / playwright / worker / …) coerces raw
 * JSON-tool arguments the same way; these helpers used to be copy-pasted
 * per repo and drifted (different defaults, different error styles). The
 * PRIMITIVES live here; repo-specific wrappers (an i18n `requireArg`, a
 * clamped `secondsArg`, …) stay local and compose on top.
 *
 * Convention: a missing or wrongly-typed argument never throws — it reads
 * as empty/undefined, and the handler decides (requireArg-style wrappers
 * turn that into a tool error).
 */

/** Read a string tool argument (missing/typed wrong = ""). */
export function strArg(
  args: Record<string, unknown>,
  key: string,
): string {
  const v = args[key]
  return typeof v === 'string' ? v : ''
}

/** Read a finite number argument (missing/typed wrong = undefined). */
export function numArg(
  args: Record<string, unknown>,
  key: string,
): number | undefined {
  const v = args[key]
  return typeof v === 'number' && Number.isFinite(v) ? v : undefined
}

/** Read a boolean argument (missing/typed wrong = undefined). */
export function boolArg(
  args: Record<string, unknown>,
  key: string,
): boolean | undefined {
  const v = args[key]
  return typeof v === 'boolean' ? v : undefined
}

/** Read a string-array argument (non-string members dropped). */
export function strArray(
  args: Record<string, unknown>,
  key: string,
): string[] {
  const v = args[key]
  return Array.isArray(v)
    ? v.filter((x): x is string => typeof x === 'string')
    : []
}
