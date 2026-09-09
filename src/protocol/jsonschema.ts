/**
 * Minimal JSON-Schema subset validation for hook payloads.
 *
 * The full spec is out of scope; this covers the declarative surface that
 * hook authors actually need: `type` (scalar + object), `required`,
 * `properties[*].type`, and `enum`. Returns a human-readable error or null.
 */
export function validateJsonSchema(
  schema: Record<string, unknown> | undefined,
  value: unknown,
): string | null {
  if (schema === undefined) return null

  if (schema.type === 'string' && typeof value !== 'string')
    return `expected string, got ${typeof value}`
  if (schema.type === 'number' && typeof value !== 'number')
    return `expected number, got ${typeof value}`
  if (schema.type === 'boolean' && typeof value !== 'boolean')
    return `expected boolean, got ${typeof value}`
  if (schema.type === 'object' && (value === null || typeof value !== 'object'))
    return `expected object, got ${value === null ? 'null' : typeof value}`

  if (Array.isArray(schema.enum)) {
    const hit = schema.enum.some(
      e => JSON.stringify(e) === JSON.stringify(value),
    )
    if (!hit) return `value not in the declared enum`
  }

  if (schema.type === 'object' && value !== null && typeof value === 'object') {
    const obj = value as Record<string, unknown>
    if (Array.isArray(schema.required)) {
      for (const k of schema.required) {
        if (typeof k === 'string' && !(k in obj))
          return `missing required field '${k}'`
      }
    }
    const props = schema.properties as
      | Record<string, Record<string, unknown>>
      | undefined
    if (props !== undefined) {
      for (const [k, sub] of Object.entries(props)) {
        if (k in obj) {
          const err = validateJsonSchema(sub, obj[k])
          if (err !== null) return `field '${k}': ${err}`
        }
      }
    }
  }
  return null
}
