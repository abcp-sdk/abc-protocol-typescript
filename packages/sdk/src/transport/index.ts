import type { Bus } from '../bus/index.js'

/**
 * Transport wiring. Rather than expose a `createBus` for the user to call, we
 * give `Agent.connect(...)` / `Extension.connect(...)` everything they need
 * here: a single `connectBus(opts)` that resolves the right transport and
 * returns a `Bus` (plus any owned handle, e.g. an embedded WS hub) to clean up.
 *
 * The transports are NOT unified behind one interface — their topology (shared
 * in-memory hub, embedded WS hub, external NATS broker) differs — but the
 * `Bus` they return is identical, so role logic never changes when switching.
 */
export type AgentConnect =
  | { provider: 'inproc'; hub?: unknown }
  | { provider: 'ws'; port?: number }
  | { provider: 'nats'; url?: string }

export type ExtensionConnect =
  | { provider: 'inproc'; hub: unknown }
  | { provider: 'ws'; url: string }
  | { provider: 'nats'; url?: string }

export interface ConnectedBus {
  bus: Bus
  /** Ownership handle for a transport the caller now owns (inproc/ws hub). */
  hub?: unknown
  /** Optional cleanup for that handle, invoked by `close()`. */
  closeHandle?: () => void | Promise<void>
}

export async function connectBus(
  opts: AgentConnect | ExtensionConnect,
): Promise<ConnectedBus> {
  switch (opts.provider) {
    case 'inproc': {
      const m = await import('./inproc/index.js')
      if (opts.hub !== undefined) {
        return { bus: m.createInprocBus(opts.hub as never) }
      }
      const { bus, hub } = m.createInprocHub()
      return { bus, hub }
    }
    case 'ws': {
      const m = await import('@abc-protocol/sdk-ws')
      if ('url' in opts) {
        const { bus } = await m.createWsBus({
          side: 'extension',
          url: opts.url,
        })
        return { bus }
      }
      const { bus, hub } = await m.createWsBus({
        side: 'agent',
        port: opts.port ?? 0,
      })
      return {
        bus,
        hub,
        closeHandle: () => (hub as { close(): void }).close(),
      }
    }
    case 'nats': {
      const m = await import('@abc-protocol/sdk-nats')
      const { bus } = await m.createNatsBus(
        opts.url === undefined ? {} : { url: opts.url },
      )
      return { bus }
    }
  }
}
