import { z } from '../zod.js'
import {
  ConfigSetSchema,
  ConfigSnapshotSchema,
  ExtensionManifestSchema,
  ExtensionVariableValueSchema,
  HookCallSchema,
  HookEventSchema,
  HookResponseSchema,
  InterruptSignalSchema,
  MailboxMessageSchema,
  ToolCallEnvelopeSchema,
  ToolProgressSchema,
  ToolResultSchema,
} from './payload.js'

/**
 * Concrete payload schemas for every NAMED channel, so `payload` is never left
 * opaque (`unknown`) at the boundary. Requests and named publications are
 * fully typed against their business shape.
 *
 * Ephemeral reply channels (`abc.ws.reply.*`, `abc.inproc.reply.*`, NATS
 * inboxes) are intentionally absent: their payload is determined by the
 * request they answer (known only to the receiver role), not by the channel
 * name. Those are typed by {@link ReplyPayloadSchema}.
 */
export const NamedPayloadSchema = {
  discover: z.object({}),
  toolCall: ToolCallEnvelopeSchema,
  variable: z.object({ name: z.string() }),
  hookCall: HookCallSchema,
  mailbox: MailboxMessageSchema,
  interrupt: InterruptSignalSchema,
  hookEvent: HookEventSchema,
  toolProgress: ToolProgressSchema,
  configSet: ConfigSetSchema,
} as const

export type NamedPayload = {
  [K in keyof typeof NamedPayloadSchema]: z.infer<
    (typeof NamedPayloadSchema)[K]
  >
}

const Patterns = {
  discover: /^abc\.discover$/,
  toolCall: /^abc\.tool\.call\./,
  variable: /^abc\.var\./,
  hookCall: /^abc\.hook\.call\./,
  mailbox: /^abc\.mailbox\./,
  interrupt: /^abc\.ctl\.interrupt\./,
  hookEvent: /^abc\.hook\.event\./,
  toolProgress: /^abc\.tool\.progress\./,
  config: /^abc\.config\.(get\.)?[^.]+$/,
} as const

/** Resolve the concrete payload schema for a named channel, or null (ephemeral). */
export function namedPayloadSchema(ch: string): z.ZodType | null {
  if (Patterns.discover.test(ch)) return NamedPayloadSchema.discover
  if (Patterns.toolCall.test(ch)) return NamedPayloadSchema.toolCall
  if (Patterns.variable.test(ch)) return NamedPayloadSchema.variable
  if (Patterns.hookCall.test(ch)) return NamedPayloadSchema.hookCall
  if (Patterns.mailbox.test(ch)) return NamedPayloadSchema.mailbox
  if (Patterns.interrupt.test(ch)) return NamedPayloadSchema.interrupt
  if (Patterns.hookEvent.test(ch)) return NamedPayloadSchema.hookEvent
  if (Patterns.toolProgress.test(ch)) return NamedPayloadSchema.toolProgress
  if (ch.startsWith('abc.config.get.')) return ConfigSnapshotSchema
  if (ch.startsWith('abc.config.')) return NamedPayloadSchema.configSet
  return null
}

/** Payload union for ephemeral replies (tool result / variable value / hook response / manifest). */
export const ReplyPayloadSchema = z.union([
  ToolResultSchema,
  ExtensionVariableValueSchema,
  HookResponseSchema,
  ExtensionManifestSchema,
])
export type ReplyPayload = z.infer<typeof ReplyPayloadSchema>
