// Embedded NATS: the agent side spawns a local nats-server (memory storage
// here; pass { storage: 'file', storeDir } for persistence) and prints the
// URL extensions connect to. This is the single-node form — the agent
// process owns the broker lifecycle.
//
//   npx tsx examples/embedded.ts            # spawns nats-server (PATH)
//   ABC_NATS_SERVER_BIN=... npx tsx examples/embedded.ts
import { Agent, Extension, start as startNats } from '@abc-protocol/sdk'
import { createMockExtensionConfig } from './mock-extension/src/index.js'
import { runMockAgent } from './mock-agent/src/index.js'

async function main(): Promise<void> {
  const server = await startNats({ storage: 'memory' })
  console.log(`[embedded] nats-server on ${server.url}`)

  const agent = await Agent.connect({ url: server.url })
  const ext = await Extension.connect(
    { url: server.url },
    createMockExtensionConfig(),
  )
  await ext.serve()

  await runMockAgent(agent)

  await ext.close()
  await agent.close()
  await server.stop()
  console.log('[embedded] server stopped')
}

await main().catch(err => {
  console.error('[embedded example] failed (is nats-server on PATH?):', err.message)
  process.exit(1)
})
