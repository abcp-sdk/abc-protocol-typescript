import { readFileSync } from 'node:fs'
import { parse as parseYaml } from 'yaml'
import type {
  ConfigSpec,
  ExtensionConfig,
  ToolSpec,
  VariableSpec,
} from './extension/index.js'
import { z } from './zod.js'

const ManifestToolSchema = z.object({
  name: z.string(),
  description: z.string(),
  input_schema: z.record(z.string(), z.unknown()).optional(),
  /** Config names whose value this tool requires to run (may be shared). */
  required_config: z.array(z.string()).optional(),
})

const ManifestVariableSchema = z.object({
  name: z.string(),
  description: z.string().optional(),
  scope: z.enum(['global', 'session']).default('global'),
})

const ManifestConfigSchema = z.object({
  name: z.string(),
  type: z.enum(['string', 'number', 'boolean', 'enum', 'json']),
  enum_values: z.array(z.string()).optional(),
  default: z.unknown().optional(),
  description: z.string().optional(),
  scope: z.enum(['global', 'session']).default('global'),
})

const ManifestSchema = z.object({
  id: z.string(),
  version: z.string(),
  tools: z.array(ManifestToolSchema).optional(),
  variables: z.array(ManifestVariableSchema).optional(),
  hooks: z
    .object({
      call: z.array(z.string()).optional(),
      event: z.array(z.string()).optional(),
      call_schemas: z
        .record(z.string(), z.record(z.string(), z.unknown()))
        .optional(),
      event_schemas: z
        .record(z.string(), z.record(z.string(), z.unknown()))
        .optional(),
    })
    .optional(),
})

/**
 * `input_schema` is an opaque JSON-Schema blob by design: arbitrary schema
 * keywords flow through untouched. One convention is protocol-declared:
 * any schema node (typically a property) may carry
 * `descriptions: Record<locale, string>` next to its `description`, exactly
 * like tool-level descriptions. Consumers resolve
 * `descriptions[locale] → descriptions[primary] → description` and MUST
 * strip the `descriptions` key before handing the schema to a model, so
 * only standard JSON-Schema keys cross the model boundary.
 */
export interface Manifest {
  id: string
  version: string
  tools: Array<{
    name: string
    description: string
    descriptions?: Record<string, string>
    input_schema?: Record<string, unknown>
    required_config?: string[]
  }>
  variables: Array<{
    name: string
    description?: string
    descriptions?: Record<string, string>
    scope: 'global' | 'session'
  }>
  config?: Array<{
    name: string
    type: 'string' | 'number' | 'boolean' | 'enum' | 'json'
    enum_values?: string[]
    default?: unknown
    description?: string
    scope?: 'global' | 'session'
  }>
  hooks?: {
    call?: string[]
    event?: string[]
    call_schemas?: Record<string, Record<string, unknown>>
    event_schemas?: Record<string, Record<string, unknown>>
  }
}

export interface HookSchema {
  call?: Record<string, Record<string, unknown>>
  event?: Record<string, Record<string, unknown>>
}

export function parseManifest(yaml: string): Manifest {
  const raw = parseYaml(yaml)
  const parsed = ManifestSchema.parse(raw)
  return parsed as Manifest
}

export function loadManifest(path: string): Manifest {
  return parseManifest(readFileSync(path, 'utf8'))
}

export function manifestConfig(
  manifest: Manifest,
  opts: {
    handlers?: Record<string, { execute: ToolSpec['execute'] }>
    variables?: Record<string, Omit<VariableSpec, 'description' | 'scope'>>
    onCallHook?: ExtensionConfig['onCallHook']
    onEventHook?: ExtensionConfig['onEventHook']
  } = {},
): ExtensionConfig {
  const tools: Record<string, ToolSpec> = {}
  for (const t of manifest.tools ?? []) {
    const handler = opts.handlers?.[t.name]
    if (handler === undefined) continue
    const spec: ToolSpec = {
      description: t.description,
      execute: handler.execute,
    }
    if (t.input_schema !== undefined) spec.inputSchema = t.input_schema
    if (t.required_config !== undefined) spec.requiredConfig = t.required_config
    tools[t.name] = spec
  }

  const variables: Record<string, VariableSpec> = {}
  for (const v of manifest.variables ?? []) {
    const resolver = opts.variables?.[v.name]
    const spec: VariableSpec = { scope: v.scope }
    if (v.description !== undefined) spec.description = v.description
    if (resolver?.resolve !== undefined) spec.resolve = resolver.resolve
    variables[v.name] = spec
  }

  const cfg: ExtensionConfig = {
    id: manifest.id,
    version: manifest.version,
    tools,
    variables,
  }
  for (const c of manifest.config ?? []) {
    const spec: ConfigSpec = { type: c.type }
    if (c.enum_values !== undefined) spec.enumValues = c.enum_values
    if (c.default !== undefined) spec.default = c.default
    if (c.description !== undefined) spec.description = c.description
    if (c.scope !== undefined) spec.scope = c.scope
    cfg.config = cfg.config ?? {}
    cfg.config[c.name] = spec
  }
  if (manifest.hooks?.call !== undefined) cfg.callHooks = manifest.hooks.call
  if (manifest.hooks?.event !== undefined) cfg.eventHooks = manifest.hooks.event
  if (
    manifest.hooks?.call_schemas !== undefined ||
    manifest.hooks?.event_schemas !== undefined
  ) {
    const hookSchemas: {
      call?: Record<string, Record<string, unknown>>
      event?: Record<string, Record<string, unknown>>
    } = {}
    if (manifest.hooks?.call_schemas !== undefined)
      hookSchemas.call = manifest.hooks.call_schemas
    if (manifest.hooks?.event_schemas !== undefined)
      hookSchemas.event = manifest.hooks.event_schemas
    cfg.hookSchemas = hookSchemas
  }
  if (opts.onCallHook !== undefined) cfg.onCallHook = opts.onCallHook
  if (opts.onEventHook !== undefined) cfg.onEventHook = opts.onEventHook
  return cfg
}
