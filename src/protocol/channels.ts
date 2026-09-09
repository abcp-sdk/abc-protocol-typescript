import { createHash } from 'node:crypto'

/**
 * Deterministic transport-safe token for a session name
 * (sha256 -> base64url -> first 22 chars). The session `name` remains the
 * single primary key; this token is only a routing derivation.
 */
export function sessionToken(sessionName: string): string {
  return createHash('sha256')
    .update(sessionName, 'utf8')
    .digest('base64url')
    .slice(0, 22)
}

/**
 * Logical channel names. All channels derive deterministically from the
 * manifest (`id` + `name`); the manifest is the single source of truth.
 */
export const CH = {
  DISCOVER: 'abc.discover',
  toolCall: (extId: string, tool: string) => `abc.tool.call.${extId}.${tool}`,
  toolProgress: (callId: string) => `abc.tool.progress.${callId}`,
  variable: (extId: string, name: string) => `abc.var.${extId}.${name}`,
  mailbox: (sessionName: string) => `abc.mailbox.${sessionToken(sessionName)}`,
  sessionEvents: (sessionName: string) =>
    `abc.session.events.${sessionToken(sessionName)}`,
  interrupt: (extId: string) => `abc.ctl.interrupt.${extId}`,
  interruptAll: () => 'abc.ctl.interrupt.>',
  hookCall: (extId: string, hook: string) => `abc.hook.call.${extId}.${hook}`,
  hookEvent: (hook: string) => `abc.hook.event.${hook}`,
  config: (extId: string) => `abc.config.${extId}`,
  configGet: (extId: string) => `abc.config.get.${extId}`,
  configWildcard: () => 'abc.config.get.>',
} as const

/** Prefix for all mailbox channels; append `>` for the consume wildcard. */
export const MAILBOX_WILDCARD = 'abc.mailbox.'
/** Durable-inbox consume wildcard covering every session mailbox. */
export const MAILBOX_CONSUME = 'abc.mailbox.>'
