import { runConformance } from '@abc-protocol/sdk/tests/conformance.js'
import { connectNatsBus } from '../src/index.js'

const url =
  process.env.ABC_NATS_URL ?? 'nats://nats.develop.svc.cluster.local:4222'

runConformance('nats', async () => {
  let available = true
  const agentBus = await connectNatsBus(url).catch(() => {
    available = false
    return null
  })
  if (!available || agentBus === null) {
    return {
      agentBus: null as never,
      extensionBus: null as never,
      cleanup: async () => {},
    }
  }
  const extensionBus = await connectNatsBus(url)
  return {
    agentBus,
    extensionBus,
    cleanup: async () => {
      // Drain can throw ClosedConnectionError when a request timed out and
      // the client already torn down its subs; close is best-effort.
      await extensionBus.close().catch(() => {})
      await agentBus.close().catch(() => {})
    },
  }
})
