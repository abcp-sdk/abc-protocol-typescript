import { Agent } from '@abc-protocol/sdk'
import { describe, expect, it } from 'vitest'
import { WsBus, WsHub } from '../src/index.js'

describe('ws transport', () => {
  it('delivers a tool call over websocket', async () => {
    const hub = new WsHub(0)
    await new Promise(r => setTimeout(r, 20))
    const port = hub.port

    const agentBus = new WsBus(`ws://127.0.0.1:${port}`)
    const extBus = new WsBus(`ws://127.0.0.1:${port}`)
    await new Promise(r => setTimeout(r, 50))

    // minimal extension: echo tool
    const { Extension } = await import('@abc-protocol/sdk')
    const ext = new Extension(extBus, {
      id: 'ws-ext',
      version: '1.0',
      tools: {
        echo: {
          description: 'echo',
          execute: async args => ({ content: String(args?.msg ?? '') }),
        },
      },
    })
    await ext.serve()

    const agent = new Agent(agentBus)
    const res = await agent.callTool('s1', 'ws-ext', 'echo', 'c1', {
      msg: 'hi',
    })
    expect(res.content).toBe('hi')

    await ext.close()
    await agentBus.close()
    await extBus.close()
    hub.close()
  })
})
