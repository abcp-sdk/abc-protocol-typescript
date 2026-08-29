import { describe, expect, it } from 'vitest'
import { Agent } from '../src/agent/index.js'
import { Extension } from '../src/extension/index.js'
import { newInprocPair } from '../src/transport/inproc/index.js'

describe('abc protocol inproc', () => {
  it('discovers extensions and returns unique manifests', async () => {
    const { agentBus, extensionBus } = newInprocPair()
    const ext = new Extension(extensionBus, {
      id: 'repo',
      version: '1.0',
      tools: {
        read: {
          description: 'read a file',
          execute: async () => ({ content: 'hello' }),
        },
      },
    })
    await ext.serve()
    const agent = new Agent(agentBus)
    const manifests = await agent.discover(300)
    expect(manifests.map(m => m.id)).toContain('repo')
    await ext.close()
  })

  it('calls a tool and resolves its content', async () => {
    const { agentBus, extensionBus } = newInprocPair()
    const ext = new Extension(extensionBus, {
      id: 'ops',
      version: '1.0',
      tools: {
        echo: {
          description: 'echo',
          execute: async args => ({ content: `got ${args?.msg ?? ''}` }),
        },
      },
    })
    await ext.serve()
    const agent = new Agent(agentBus)
    const result = await agent.callTool('s1', 'ops', 'echo', 'c1', {
      msg: 'hi',
    })
    expect(result.content).toBe('got hi')
    await ext.close()
  })

  it('runs a failing sync call hook', async () => {
    const { agentBus, extensionBus } = newInprocPair()
    const ext = new Extension(extensionBus, {
      id: 'repo',
      version: '1.0',
      callHooks: ['session.before_create'],
      onCallHook: async () => ({
        ok: false,
        error: { code: 'business', message: 'repo exists' },
      }),
    })
    await ext.serve()
    const agent = new Agent(agentBus)
    const res = await agent.callHook('s1', 'repo', 'session.before_create')
    expect(res.ok).toBe(false)
    expect(res.error?.code).toBe('business')
    await ext.close()
  })

  it('delivers an interrupt to the extension event hook', async () => {
    const { agentBus, extensionBus } = newInprocPair()
    let received: string | undefined
    const ext = new Extension(extensionBus, {
      id: 'ops',
      version: '1.0',
      eventHooks: ['interrupt'],
      onEventHook: async (hook, sessionName) => {
        received = `${hook}:${sessionName}`
      },
    })
    await ext.serve()
    const agent = new Agent(agentBus)
    await agent.interrupt('ops', 's1', 'stop')
    await new Promise(r => setTimeout(r, 50))
    expect(received).toBe('interrupt:s1')
    await ext.close()
  })

  it('streams progress telemetry as one-way pub', async () => {
    const { agentBus, extensionBus } = newInprocPair()
    const ext = new Extension(extensionBus, {
      id: 'ops',
      version: '1.0',
      tools: {
        long: {
          description: 'long task',
          execute: async (args, callId) => {
            await new Promise(r => setTimeout(r, 20))
            return { content: `done ${args?.n ?? ''}` }
          },
        },
      },
    })
    await ext.serve()
    const agent = new Agent(agentBus)
    const callId = 'c-progress'
    const sub = await agent.subscribeProgress(callId)
    const collected: unknown[] = []
    void (async () => {
      for await (const env of sub) collected.push(env.payload)
    })()
    // progress is reported out-of-band from the caller side by the extension
    // author; simulate it directly for the telemetry contract.
    await ext.reportProgress(callId, {
      phase: 'sync',
      progress: 0.5,
      text: 'half',
    })
    await new Promise(r => setTimeout(r, 30))
    await sub.close()
    expect(collected.length).toBe(1)
    expect(collected[0]).toMatchObject({
      call_id: callId,
      phase: 'sync',
      progress: 0.5,
    })
    await ext.close()
  })
})
