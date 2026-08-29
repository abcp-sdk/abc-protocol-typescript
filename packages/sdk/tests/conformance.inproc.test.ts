import { newInprocPair } from '../src/transport/inproc/index.js'
import { runConformance } from './conformance.js'

runConformance('inproc', async () => {
  const { agentBus, extensionBus } = newInprocPair()
  return {
    agentBus,
    extensionBus,
    cleanup: async () => {},
  }
})
