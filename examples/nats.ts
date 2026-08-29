import { Agent, Extension } from '@abc-protocol/sdk'
import { createMockExtensionConfig } from './mock-extension/src/index.js'
import { runMockAgent } from './mock-agent/src/index.js'

// External NATS: agent and extension are equal peers over a broker whose
// lifecycle the deployment owns (the production form).
async function main(): Promise<void> {
  const url = process.env.NATS_URL
  const agent = await Agent.connect({ url })
  const ext = await Extension.connect(
    { url },
    createMockExtensionConfig(),
  )
  await ext.serve()

  await runMockAgent(agent)
  await ext.close()
  await agent.close()
}

await main().catch(err => {
  console.error('[nats example] failed (is a NATS server running?):', err.message)
  process.exit(1)
})