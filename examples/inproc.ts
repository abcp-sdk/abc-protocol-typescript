import { Agent, Extension } from '@abc-protocol/sdk'
import { createMockExtensionConfig } from './mock-extension/src/index.js'
import { runMockAgent } from './mock-agent/src/index.js'

// In-process: agent owns the hub; extension attaches via the hub handle.
async function main(): Promise<void> {
  const agent = await Agent.connect({ provider: 'inproc' })
  const ext = await Extension.connect(
    { provider: 'inproc', hub: agent.hub },
    createMockExtensionConfig(),
  )
  await ext.serve()

  await runMockAgent(agent)
  await ext.close()
  await agent.close()
}

await main()