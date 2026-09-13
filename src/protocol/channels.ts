import { createHash } from 'node:crypto'
import { tenantPrefix } from './tenant.js'

/**
 * Deterministic transport-safe token for a session name
 * (sha256 -> base64url -> first 22 chars). The session `name` remains the
 * single primary key; this token is only a routing derivation. It is NOT
 * tenant-scoped: the tenant segment of the subject already separates the same
 * session name across tenants.
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
 *
 * Data-plane channels are `abc.<tenant>.<...>`; the discovery channel is
 * global (`abc.discover`) because every extension serves every tenant in a
 * shared-process deployment.
 */
export const CH = {
  DISCOVER: 'abc.discover',
  toolCall: (tenant: string, extId: string, tool: string) =>
    `${tenantPrefix(tenant)}tool.call.${extId}.${tool}`,
  toolProgress: (tenant: string, callId: string) =>
    `${tenantPrefix(tenant)}tool.progress.${callId}`,
  variable: (tenant: string, extId: string, name: string) =>
    `${tenantPrefix(tenant)}var.${extId}.${name}`,
  mailbox: (tenant: string, sessionName: string) =>
    `${tenantPrefix(tenant)}mailbox.${sessionToken(sessionName)}`,
  sessionEvents: (tenant: string, sessionName: string) =>
    `${tenantPrefix(tenant)}session.events.${sessionToken(sessionName)}`,
  sessionChanged: (tenant: string) =>
    `${tenantPrefix(tenant)}session.changed`,
  lifecycle: (tenant: string, kind: string) =>
    `${tenantPrefix(tenant)}session.lifecycle.${kind}`,
  lifecycleWildcard: (tenant: string) =>
    `${tenantPrefix(tenant)}session.lifecycle.>`,
  interrupt: (tenant: string, extId: string) =>
    `${tenantPrefix(tenant)}ctl.interrupt.${extId}`,
  interruptAll: (tenant: string) => `${tenantPrefix(tenant)}ctl.interrupt.>`,
  hookCall: (tenant: string, extId: string, hook: string) =>
    `${tenantPrefix(tenant)}hook.call.${extId}.${hook}`,
  hookEvent: (tenant: string, hook: string) =>
    `${tenantPrefix(tenant)}hook.event.${hook}`,
  config: (tenant: string, extId: string) =>
    `${tenantPrefix(tenant)}config.${extId}`,
  configGet: (tenant: string, extId: string) =>
    `${tenantPrefix(tenant)}config.get.${extId}`,
  configWildcard: (tenant: string) => `${tenantPrefix(tenant)}config.get.>`,
  dlq: (tenant: string, token: string) =>
    `${tenantPrefix(tenant)}dlq.${token}`,
} as const

/** Prefix for all mailbox channels; append `>` for the consume wildcard. */
export const MAILBOX_WILDCARD = 'abc.'
/** Durable-inbox consume wildcard covering every session mailbox, any tenant. */
export const MAILBOX_CONSUME = 'abc.*.mailbox.>'
/** Cross-tenant consume wildcard for session events. */
export const EVENTS_CONSUME = 'abc.*.session.events.>'
/** Cross-tenant consume wildcard for the dead-letter stream. */
export const DLQ_CONSUME = 'abc.*.dlq.>'
