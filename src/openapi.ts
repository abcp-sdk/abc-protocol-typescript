import {
  OpenAPIRegistry,
  OpenApiGeneratorV31,
} from '@asteasolutions/zod-to-openapi'
import {
  ConfigSetSchema,
  ConfigSnapshotSchema,
  EnvelopeSchema,
  ErrorPayloadSchema,
  ExtensionConfigItemSchema,
  ExtensionManifestSchema,
  ExtensionToolSchema,
  ExtensionVariableSchema,
  ExtensionVariableValueSchema,
  HookCallSchema,
  HookEventSchema,
  HookResponseSchema,
  InterruptSignalSchema,
  LifecycleEventSchema,
  MailboxMessageSchema,
  ObjectRefSchema,
  ToolCallEnvelopeSchema,
  ToolProgressSchema,
  ToolResultSchema,
} from './protocol/index.js'
import type { z } from './zod.js'

/**
 * OpenAPI generation. Zod is the single source of truth for the ABC wire
 * contract; this module exports the equivalent OpenAPI 3.1 document so the
 * same schemas can generate Go types (`oapi-codegen`), other-language clients
 * (`openapi-generator`), or reference docs — without ever hand-writing a
 * second copy that could drift.
 */
const registry = new OpenAPIRegistry()

const components: Array<{ name: string; schema: z.ZodType }> = [
  { name: 'Envelope', schema: EnvelopeSchema },
  { name: 'ErrorPayload', schema: ErrorPayloadSchema },
  { name: 'ObjectRef', schema: ObjectRefSchema },
  { name: 'ToolCallEnvelope', schema: ToolCallEnvelopeSchema },
  { name: 'ToolResult', schema: ToolResultSchema },
  { name: 'ToolProgress', schema: ToolProgressSchema },
  { name: 'MailboxMessage', schema: MailboxMessageSchema },
  { name: 'ExtensionTool', schema: ExtensionToolSchema },
  { name: 'ExtensionVariable', schema: ExtensionVariableSchema },
  { name: 'ExtensionVariableValue', schema: ExtensionVariableValueSchema },
  { name: 'ExtensionManifest', schema: ExtensionManifestSchema },
  { name: 'HookCall', schema: HookCallSchema },
  { name: 'HookEvent', schema: HookEventSchema },
  { name: 'HookResponse', schema: HookResponseSchema },
  { name: 'InterruptSignal', schema: InterruptSignalSchema },
  { name: 'ExtensionConfigItem', schema: ExtensionConfigItemSchema },
  { name: 'ConfigSet', schema: ConfigSetSchema },
  { name: 'ConfigSnapshot', schema: ConfigSnapshotSchema },
  { name: 'LifecycleEvent', schema: LifecycleEventSchema },
]

for (const { name, schema } of components) {
  registry.register(name, schema)
}

export function generateOpenApi(): Record<string, unknown> {
  const generator = new OpenApiGeneratorV31(registry.definitions)
  return generator.generateDocument({
    openapi: '3.1.0',
    info: {
      title: 'ABC Protocol',
      version: '0.1.0',
      description:
        'Agent Bus Communication Protocol — wire contract between an agent and extension servers. Generated from the zod schemas (@abc-protocol/sdk).',
    },
  }) as unknown as Record<string, unknown>
}
