import { runConformance } from '@abc-protocol/sdk/tests/conformance.js'
import { WsBus, WsHub } from '../src/index.js'

runConformance('ws', async () => {
  const hub = new WsHub(0)
  await new Promise(r => setTimeout(r, 20))
  const url = `ws://127.0.0.1:${hub.port}`
  const agentBus = new WsBus(url)
  const extensionBus = new WsBus(url)
  await new Promise(r => setTimeout(r, 50))
  return {
    agentBus,
    extensionBus,
    cleanup: async () => {
      await agentBus.close()
      await extensionBus.close()
      hub.close()
    },
  }
})
