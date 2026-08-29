import { z } from '../zod.js'
import { ErrorPayloadSchema } from './error.js'

/**
 * Business payload types. Tool results come in exactly three shapes:
 *   - plain text (`content`)
 *   - structured JSON (`data`)
 *   - large binary -> an object reference (`object`), bytes in the object store
 */

/**
 * A declared configuration item. Agent-side writes are validated against
 * this declaration; the extension reacts to applied changes.
 */
export const ExtensionConfigItemSchema = z.object({
  name: z.string(),
  /** Value type, used by the agent-side writer for validation. */
  type: z.enum(['string', 'number', 'boolean', 'enum', 'json']),
  /** Allowed values when type is enum. */
  enum_values: z.array(z.string()).optional(),
  /** Applied when no value has been set (yet). */
  default: z
    .unknown()
    .openapi({ description: 'Default value when unset.' })
    .optional(),
  description: z.string().optional(),
  scope: z.enum(['global', 'session']).default('global'),
})
export type ExtensionConfigItem = z.infer<typeof ExtensionConfigItemSchema>

/**
 * One config change delivered to an extension. Sent as a 1:1 `req` on
 * `abc.config.<extId>`; with `ack: true` the extension MUST answer with a
 * HookResponse — `ok: false` rejects the change and the agent keeps the old
 * value (the revision does not advance).
 */
export const ConfigSetSchema = z.object({
  name: z.string(),
  value: z
    .unknown()
    .openapi({ description: 'Validated config value.' })
    .optional(),
  revision: z.number().int(),
  scope: z.enum(['global', 'session']),
  session_name: z.string().optional(),
  /** Require an explicit ack (HookResponse) from the extension. */
  ack: z.boolean().default(true),
})
export type ConfigSet = z.infer<typeof ConfigSetSchema>

/** Answer to the startup snapshot request on `abc.config.get.<extId>`. */
export const ConfigSnapshotSchema = z.object({
  /** Applied global values, keyed by config name. */
  global: z.record(z.string(), z.unknown()),
  /** Per-session override values, keyed by session name then config name. */
  sessions: z.record(z.string(), z.record(z.string(), z.unknown())),
})
export type ConfigSnapshot = z.infer<typeof ConfigSnapshotSchema>

export const ObjectRefSchema = z.object({
  id: z.string(),
  content_type: z.string().optional(),
})
export type ObjectRef = z.infer<typeof ObjectRefSchema>

export const ToolCallEnvelopeSchema = z.object({
  call_id: z.string(),
  arguments: z
    .record(z.string(), z.unknown())
    .openapi({ description: 'Arbitrary key/value tool arguments.' }),
})
export type ToolCallEnvelope = z.infer<typeof ToolCallEnvelopeSchema>

export const ToolResultSchema = z.object({
  call_id: z.string(),
  tool: z.string(),
  content: z.string().optional(),
  data: z
    .unknown()
    .openapi({ description: 'Opaque structured JSON result.' })
    .optional(),
  object: ObjectRefSchema.optional(),
  error: z.lazy(() => ErrorPayloadSchema).optional(),
  metadata: z.unknown().openapi({ description: 'Opaque metadata.' }).optional(),
})
export type ToolResult = z.infer<typeof ToolResultSchema>

export const MailboxMessageSchema = z.object({
  id: z.string(),
  type: z.string(),
  payload: z
    .unknown()
    .openapi({ description: 'Opaque message body.' })
    .optional(),
})
export type MailboxMessage = z.infer<typeof MailboxMessageSchema>

export const ExtensionToolSchema = z.object({
  name: z.string(),
  description: z.string(),
  input_schema: z
    .record(z.string(), z.unknown())
    .openapi({ description: 'JSON Schema describing tool input.' })
    .optional(),
})
export type ExtensionTool = z.infer<typeof ExtensionToolSchema>

export const ExtensionVariableSchema = z.object({
  name: z.string(),
  description: z.string().optional(),
  scope: z.enum(['global', 'session']).default('global'),
})
export type ExtensionVariable = z.infer<typeof ExtensionVariableSchema>

export const ExtensionVariableValueSchema = z.object({
  name: z.string(),
  value: z.string(),
})
export type ExtensionVariableValue = z.infer<
  typeof ExtensionVariableValueSchema
>

export const HooksSchema = z.object({
  call: z.array(z.string()).optional(),
  event: z.array(z.string()).optional(),
  /** Per-hook JSON schema (subset) for call-hook argument payloads. */
  call_schemas: z
    .record(z.string(), z.record(z.string(), z.unknown()))
    .optional(),
  /** Per-hook JSON schema (subset) for event-hook payloads. */
  event_schemas: z
    .record(z.string(), z.record(z.string(), z.unknown()))
    .optional(),
})
export type Hooks = z.infer<typeof HooksSchema>

export const ExtensionManifestSchema = z.object({
  id: z.string(),
  version: z.string(),
  capabilities: z.array(z.enum(['tools', 'prompt'])).default([]),
  /**
   * Protocol features this extension implements, for graceful degradation:
   * an agent may use this to avoid relying on a newer capability the
   * extension does not speak (e.g. config KV recovery, presence, DLQ).
   * Absent = the pre-0.3 baseline (req/pub/queue, tools, hooks, variables).
   */
  features: z.array(z.string()).optional(),
  tools: z.array(ExtensionToolSchema).optional(),
  prompt: z
    .object({ variables: z.array(ExtensionVariableSchema).optional() })
    .optional(),
  hooks: HooksSchema.optional(),
  /** Declared config knobs the agent may set at runtime. */
  config: z.array(ExtensionConfigItemSchema).optional(),
  /**
   * Session lifecycle kinds this extension wants (created / forked /
   * renamed / deleted). The agent publishes lifecycle events regardless;
   * the declaration documents intent.
   */
  lifecycle: z
    .array(z.enum(['created', 'forked', 'renamed', 'deleted']))
    .optional(),
})
export type ExtensionManifest = z.infer<typeof ExtensionManifestSchema>

/** Known protocol features (the bitmap carried by the manifest). */
export const PROTOCOL_FEATURES = [
  'dlq',
  'config-kv',
  'presence',
  'kv-escaping',
  'interrupt-abort',
  'progress',
] as const
export type ProtocolFeature = (typeof PROTOCOL_FEATURES)[number]

/** Call-hook request (sync `req`; failure aborts the enclosing operation). */
export const HookCallSchema = z.object({
  hook: z.string(),
  session_name: z.string(),
  arguments: z
    .record(z.string(), z.unknown())
    .openapi({ description: 'Arbitrary hook arguments.' })
    .optional(),
})
export type HookCall = z.infer<typeof HookCallSchema>

/** Event-hook notification (async `pub`; best-effort). */
export const HookEventSchema = z.object({
  hook: z.string(),
  session_name: z.string(),
  payload: z
    .unknown()
    .openapi({ description: 'Opaque event body.' })
    .optional(),
})
export type HookEvent = z.infer<typeof HookEventSchema>

/**
 * Session lifecycle notification (a `pub` on
 * `abc.session.lifecycle.<kind>`): the agent informs interested extensions
 * that a session changed. `kind` is one of created / forked / renamed /
 * deleted. Field alignment with the legacy abep wire format:
 *   - forked carries `parent` (the session it forked from)
 *   - renamed carries `from` / `to` (previous and new session name)
 * Extensions declare the kinds they care about in the manifest
 * (`lifecycle:`); session-scoped variables and config overrides are cleaned
 * up on `deleted`.
 */
export const LifecycleEventSchema = z.object({
  kind: z.enum(['created', 'forked', 'renamed', 'deleted']),
  session_name: z.string(),
  /** forked: the session this one forked from. */
  parent: z.string().optional(),
  /** renamed: the previous session name. */
  from: z.string().optional(),
  /** renamed: the new session name (== session_name). */
  to: z.string().optional(),
  payload: z
    .unknown()
    .openapi({ description: 'Opaque kind-specific body.' })
    .optional(),
})
export type LifecycleEvent = z.infer<typeof LifecycleEventSchema>

/** Interrupt signal (a `pub`): cancel in-flight work for a session. */
export const InterruptSignalSchema = z.object({
  session_name: z.string().optional(),
  reason: z.string().optional(),
})
export type InterruptSignal = z.infer<typeof InterruptSignalSchema>

/**
 * Progress telemetry for an in-flight tool call. A one-way `pub` on
 * `abc.tool.progress.<callId>`. Emitting such an event implicitly means the
 * tool is still running, so there is no status field: terminal done/failed is
 * decided by the `req` response, and "stalled" is inferred by the agent's
 * orchestration layer from an absence of progress within a timeout. Consumed
 * by the orchestration layer and UI, never fed into the LLM context.
 */
export const ToolProgressSchema = z.object({
  call_id: z.string(),
  /** Current phase label (e.g. "sync"), free-form. */
  phase: z.string().optional(),
  /** 0..1 overall fraction complete. */
  progress: z.number().min(0).max(1).optional(),
  /** Human-readable one-liner for the UI. */
  text: z.string().optional(),
  /** Structured metadata for the UI to render. */
  metadata: z
    .unknown()
    .openapi({ description: 'Opaque UI metadata.' })
    .optional(),
})
export type ToolProgress = z.infer<typeof ToolProgressSchema>

/** Response to a sync call-hook ({ ok, error?, data? }). */
export const HookResponseSchema = z.object({
  ok: z.boolean(),
  error: ErrorPayloadSchema.optional(),
  data: z
    .unknown()
    .openapi({ description: 'Opaque hook result data.' })
    .optional(),
})
export type HookResponse = z.infer<typeof HookResponseSchema>
