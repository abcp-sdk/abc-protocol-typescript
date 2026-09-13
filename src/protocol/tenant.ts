/**
 * Tenant namespacing for the abc protocol (v2).
 *
 * A `tenant` is an opaque, plaintext isolation key (typically a user id). It is
 * the SECOND subject segment for every data-plane channel — `abc.<tenant>.<...>`
 * — and is also carried on every envelope so a receiver can validate that the
 * envelope agrees with the subject it arrived on.
 *
 * Plaintext (not hashed) is deliberate: it keeps subjects/KV keys readable for
 * operators. The cost is a strict charset: only `[A-Za-z0-9_-]`, 1..64 chars,
 * so a tenant can never inject a subject separator (`.`), wildcard (`*`/`>`),
 * or whitespace and break routing.
 */

const TENANT_RE = /^[A-Za-z0-9_-]{1,64}$/

/**
 * Reserved tenant used on GLOBAL control-plane messages (discovery), which
 * have no tenant segment in their subject. It is a legal tenant id so it can
 * ride the envelope's required `tenant` field without special-casing.
 */
export const GLOBAL_TENANT = 'global'

/** True when `raw` is a legal tenant id. */
export function isValidTenant(raw: string): boolean {
  return TENANT_RE.test(raw)
}

/**
 * Validate a tenant id, throwing on anything illegal. Call this at every entry
 * point that accepts a tenant from a caller (envelope decode, channel
 * builders) so a malformed id can never reach the wire.
 */
export function validateTenant(raw: string): string {
  if (!isValidTenant(raw)) {
    throw new Error(
      `invalid tenant ${JSON.stringify(raw)}: must match [A-Za-z0-9_-]{1,64}`,
    )
  }
  return raw
}

/** Data-plane subject prefix for a tenant: `abc.<tenant>.`. */
export function tenantPrefix(tenant: string): string {
  return `abc.${validateTenant(tenant)}.`
}

/**
 * Extract the tenant segment from an `abc.<tenant>.<...>` subject, or null when
 * the subject is not tenant-namespaced (e.g. the global `abc.discover`).
 */
export function subjectTenant(ch: string): string | null {
  if (!ch.startsWith('abc.')) return null
  const rest = ch.slice('abc.'.length)
  const dot = rest.indexOf('.')
  if (dot <= 0) return null
  const tenant = rest.slice(0, dot)
  return isValidTenant(tenant) ? tenant : null
}

/** KV key for a tenant-scoped entry in a shared bucket: `t.<tenant>.<rest>`. */
export function tenantKVKey(tenant: string, rest: string): string {
  return `t.${validateTenant(tenant)}.${rest}`
}

/** Object-store name for a tenant-scoped blob: `t.<tenant>.<name>`. */
export function tenantObjectName(tenant: string, name: string): string {
  return `t.${validateTenant(tenant)}.${name}`
}
