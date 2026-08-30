import { describe, it } from 'vitest'
import { Agent } from '../src/agent/index.js'
import { Extension } from '../src/extension/index.js'
import type { Identity } from '../src/identity.js'
import { type Server, start as startNats } from '../src/natsrun/index.js'
import { connectNatsBus } from '../src/transport/nats/index.js'

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

describe('abc identity: opt-in message authentication', () => {
  it('signed tool round-trip; unsigned dropped; nil identity zero overhead', async () => {
    const server: Server = await startNats({ storage: 'memory' })
    const idn: Identity = { id: 'a1', secret: 's1' }
    const agentBus = await connectNatsBus(server.url, { identity: idn })
    const extBus = await connectNatsBus(server.url, { identity: idn })

    // 1. signed tool round-trip
    const ext = new Extension(extBus, {
      id: 'id-ext',
      version: '1.0',
      tools: {
        echo: {
          description: 'e',
          execute: async () => ({ content: 'signed-pong' }),
        },
      },
    })
    await ext.serve()
    await sleep(300)
    const tr = await new Agent(agentBus).callTool(
      'sess-id',
      'id-ext',
      'echo',
      'c1',
      {},
    )
    if (tr.content !== 'signed-pong') {
      throw new Error('signed content = ' + String(tr.content))
    }

    // 2. unsigned publish dropped by identity-verifying subscriber
    const verifySub = await extBus.subscribe('abc.test.unsigned')
    let delivered = 0
    void (async () => {
      for await (const _ of verifySub) {
        void _
        delivered++
      }
    })()
    await sleep(100)
    const plainBus = await connectNatsBus(server.url)
    await plainBus.publish('abc.test.unsigned', { hello: 'unsigned' })
    await sleep(500)
    if (delivered !== 0) throw new Error('unsigned message delivered')

    // 3. signed publish flows through the same subscriber
    await agentBus.publish('abc.test.unsigned', { hello: 'signed' })
    const deadline = Date.now() + 3000
    while (delivered === 0 && Date.now() < deadline) await sleep(100)
    if (delivered === 0) throw new Error('signed message not delivered')
    await verifySub.close()

    // 4. nil identity pair still works (zero overhead)
    const plainBus2 = await connectNatsBus(server.url)
    const plainExt = new Extension(await connectNatsBus(server.url), {
      id: 'plain-ext',
      version: '1.0',
      tools: {
        ping: {
          description: 'p',
          execute: async () => ({ content: 'plain-pong' }),
        },
      },
    })
    await plainExt.serve()
    await sleep(300)
    const tr2 = await new Agent(plainBus2).callTool(
      'sess-p',
      'plain-ext',
      'ping',
      'c3',
      {},
    )
    if (tr2.content !== 'plain-pong') {
      throw new Error('plain content = ' + String(tr2.content))
    }

    await ext.close()
    await plainExt.close()
    await agentBus.close()
    await extBus.close()
    await plainBus.close()
    await plainBus2.close()
    await server.stop()
  }, 15_000)
})
