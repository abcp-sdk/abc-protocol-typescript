import { extendZodWithOpenApi } from '@asteasolutions/zod-to-openapi'
import { z } from 'zod'

// Extend zod with `.openapi()` before any schema is constructed, so every wire
// schema gains OpenAPI metadata. Import `z` from this module wherever a schema
// that must be exported to OpenAPI is defined.
extendZodWithOpenApi(z)

export { z }
