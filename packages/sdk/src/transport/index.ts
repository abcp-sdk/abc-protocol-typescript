import type { Bus } from '../bus/index.js'

/**
 * Transport wiring. There is exactly one transport — NATS — with two
 * deployment forms:
 *
 *   - external: connect to an existing NATS URL (production; extensions and
 *     agents are equal clients)
 *   - embedded: the agent side spawns a local nats-server via `natsrun`
 *     (memory or file storage) and everyone connects to it
 *
 * `Agent.connect(...)` / `Extension.connect(...)` resolve the right form
 * here and return the identical `Bus`.
 */
export type AgentConnect = { url?: string }

export type ExtensionConnect = { url?: string }

export interface ConnectedBus {
  bus: Bus
}

export async function connectBus(
  opts: AgentConnect | ExtensionConnect,
): Promise<ConnectedBus> {
  const m = await import('./nats/index.js')
  return { bus: await m.connectNatsBus(opts.url) }
}
