import { readFileSync } from 'node:fs'
import { parse as parseYaml } from 'yaml'
import type {
  ExtensionConfig,
  ToolSpec,
  VariableSpec,
} from './extension/index.js'
import { z } from './zod.js'

const ManifestToolSchema = z.object({
  name: z.string(),
  description: z.string(),
  input_schema: z.record(z.string(), z.unknown()).optional(),
})

const ManifestVariableSchema = z.object({
  name: z.string(),
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
    })
    .optional(),
})

export interface Manifest {
  id: string
  version: string
  tools: Array<{
    name: string
    description: string
    input_schema?: Record<string, unknown>
  }>
  variables: Array<{
    name: string
    description?: string
    scope: 'global' | 'session'
  }>
  hooks?: {
    call?: string[]
    event?: string[]
  }
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
  if (manifest.hooks?.call !== undefined) cfg.callHooks = manifest.hooks.call
  if (manifest.hooks?.event !== undefined) cfg.eventHooks = manifest.hooks.event
  if (opts.onCallHook !== undefined) cfg.onCallHook = opts.onCallHook
  if (opts.onEventHook !== undefined) cfg.onEventHook = opts.onEventHook
  return cfg
}
