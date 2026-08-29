import { afterAll, beforeAll } from 'vitest'
import { type Server, start as startNats } from '../src/natsrun/index.js'
import { connectNatsBus } from '../src/transport/nats/index.js'
import { type Factory, runConformance } from './conformance.js'

/**
 * Single-transport conformance: one shared nats-server (spawned via
 * natsrun, memory storage) serves the whole suite; every test gets two
 * fresh connections (agent + extension) over the same broker. The DLQ/mailbox
 * cases use unique per-test tags so a shared broker never confuses them.
 * Falls back to ABC_NATS_URL / NATS_URL when no local binary exists.
 */

let server: Server | null = null
let url: string | null = null

beforeAll(async () => {
  const external =
    process.env.ABC_NATS_URL ??
    (process.env.NATS_URL?.includes('develop') === true
      ? process.env.NATS_URL
      : undefined)
  if (external !== undefined) {
    url = external
    return
  }
  try {
    server = await startNats({ storage: 'memory' })
    url = server.url
  } catch {
    url = null
  }
}, 30_000)

afterAll(async () => {
  await server?.stop()
})

const factory: Factory = async () => {
  if (url === null) {
    throw new Error(
      'no nats-server available (install nats-server or set ABC_NATS_SERVER_BIN / ABC_NATS_URL)',
    )
  }
  const agentBus = await connectNatsBus(url)
  const extensionBus = await connectNatsBus(url)
  return {
    agentBus,
    extensionBus,
    cleanup: async () => {
      await extensionBus.close().catch(() => {})
      await agentBus.close().catch(() => {})
    },
  }
}

runConformance('nats', factory)
