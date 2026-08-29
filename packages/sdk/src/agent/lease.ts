import type { Bus } from '../bus/index.js'
import { sessionToken } from '../protocol/index.js'

// Session lease semantics (protocol-level, cross-replica): see
// sdk-go/agent/lease.go for the canonical description. Every replica must
// use the same bucket and TTL for the mutual exclusion to hold.

export const LEASE_BUCKET = 'abc-session-state'
export const LEASE_TTL_DEFAULT_MS = 30_000

/** A KV bus subset used by the lease (avoid importing full Bus typing here). */
type LeaseBus = Pick<Bus, 'kvCreate' | 'kvCas' | 'kvDelete' | 'kvGet'>

/**
 * Atomically claim the session's run lease. Returns the KV revision on
 * success (feed it to renewSession), or null when another holder owns it.
 */
export async function claimSession(
  bus: LeaseBus,
  sessionName: string,
  ttlMs = LEASE_TTL_DEFAULT_MS,
): Promise<number | null> {
  const ttl = ttlMs > 0 ? ttlMs : LEASE_TTL_DEFAULT_MS
  void ttl
  try {
    return await bus.kvCreate(
      LEASE_BUCKET,
      sessionToken(sessionName),
      'running',
      ttlMs > 0 ? ttlMs : LEASE_TTL_DEFAULT_MS,
    )
  } catch {
    return null
  }
}

/** Renew the lease via CAS; returns the new revision or null when lost. */
export async function renewSession(
  bus: LeaseBus,
  sessionName: string,
  revision: number,
  _ttlMs = LEASE_TTL_DEFAULT_MS,
): Promise<number | null> {
  try {
    return await bus.kvCas(
      LEASE_BUCKET,
      sessionToken(sessionName),
      'running',
      revision,
    )
  } catch {
    return null
  }
}

/** Release the run lease (back to idle). */
export function releaseSession(
  bus: LeaseBus,
  sessionName: string,
): Promise<void> {
  return bus.kvDelete(LEASE_BUCKET, sessionToken(sessionName))
}

/** True while the session's run lease is held. */
export async function isSessionRunning(
  bus: LeaseBus,
  sessionName: string,
): Promise<boolean> {
  try {
    return (await bus.kvGet(LEASE_BUCKET, sessionToken(sessionName))) !== null
  } catch {
    return false
  }
}

/**
 * Run fn while holding the session's exclusive cross-replica lease. Returns
 * { acquired: false } when another replica already holds it. The lease is
 * renewed on a timer (TTL/3) and released after fn settles; `lost` resolves
 * true when the lease was lost mid-run (callers should abort).
 */
export async function withSessionLease(
  bus: LeaseBus,
  sessionName: string,
  fn: (signal: { lost: boolean }) => Promise<void> | void,
  ttlMs = LEASE_TTL_DEFAULT_MS,
): Promise<{ acquired: boolean; lost: boolean }> {
  const ttl = ttlMs > 0 ? ttlMs : LEASE_TTL_DEFAULT_MS
  const revision = await claimSession(bus, sessionName, ttl)
  if (revision === null) return { acquired: false, lost: false }

  const state = { lost: false }
  const timer = setInterval(
    () => {
      void renewSession(bus, sessionName, revision, ttl).then(next => {
        if (next === null) state.lost = true
      })
    },
    Math.max(ttl / 3, 250),
  )
  try {
    await fn(state)
  } finally {
    clearInterval(timer)
    await releaseSession(bus, sessionName).catch(() => {})
  }
  return { acquired: true, lost: state.lost }
}
