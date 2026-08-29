import { Agent, Extension } from '@abc-protocol/sdk'
import { createMockExtensionConfig } from './mock-extension/src/index.js'
import { runMockAgent } from './mock-agent/src/index.js'

// WebSocket: the agent starts an embedded hub; the extension dials it.
async function main(): Promise<void> {
  const agent = await Agent.connect({ provider: 'ws', port: 0 })
  const port = (agent.hub as { port: number }).port
  const ext = await Extension.connect(
    { provider: 'ws', url: `ws://127.0.0.1:${port}` },
    createMockExtensionConfig(),
  )
  await ext.serve()

  await runMockAgent(agent)
  await ext.close()
  await agent.close()
}

await main()