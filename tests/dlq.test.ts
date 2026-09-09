import { afterAll, beforeAll, describe, it } from 'vitest'
import { Agent, TermError } from '../src/agent/index.js'
import { type Server, start as startNats } from '../src/natsrun/index.js'
import { connectNatsBus } from '../src/transport/nats/index.js'

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

// DLQ-dependent cases get their OWN broker: they create/consume durable
// consumers and park messages, which would collide with the shared
// conformance broker's fixed durable names.
describe('abc dlq: poison escalation + requeue', () => {
  let server: Server | null = null
  let url = ''

  beforeAll(async () => {
    server = await startNats({ storage: 'memory' })
    url = server.url
  }, 30_000)

  afterAll(async () => {
    await server?.stop()
  })

  it('poison messages auto-term into the DLQ after maxNaks', async () => {
    const bus = await connectNatsBus(url)
    const a = new Agent(bus)
    const tag = 'escal-' + Math.random().toString(36).slice(2, 8)
    await a.publishMailbox('sess-poison', tag, { n: 1 })
    let deliveries = 0
    const stop = await a.consumeMailbox(
      () => {
        throw new Error('boom ' + ++deliveries)
      },
      3,
      100,
    )
    const found: string[] = []
    const dlqStop = await a.consumeDLQ(m => {
      if (m.type === tag) found.push(m.type)
    })
    const deadline = Date.now() + 15_000
    while (found.length === 0 && Date.now() < deadline) await sleep(200)
    await dlqStop()
    await stop()
    if (found.length === 0) throw new Error('poison never escalated to DLQ')
    if (deliveries > 4) throw new Error('too many deliveries: ' + deliveries)
    await bus.close()
  }, 20_000)

  it('requeues a dead-lettered message back onto its session', async () => {
    const bus = await connectNatsBus(url)
    const a = new Agent(bus)
    const tag = 'rq-' + Math.random().toString(36).slice(2, 8)
    let first = true
    const succeeded: string[] = []
    const stop = await a.consumeMailbox(
      m => {
        if (m.type === tag && !first) succeeded.push(m.id)
        if (m.type === tag) first = false
        throw new TermError()
      },
      2,
      100,
    )
    await a.publishMailbox('sess-rq', tag, { k: 1 })
    let dlqId = ''
    const dlqStop1 = await a.consumeDLQ(m => {
      if (m.type === tag && dlqId === '') dlqId = m.id
      throw new Error('keep')
    })
    const deadline = Date.now() + 10_000
    while (dlqId === '' && Date.now() < deadline) await sleep(200)
    await dlqStop1()
    if (dlqId === '') throw new Error('message never reached the DLQ')
    const ok = await a.requeueDLQ(dlqId)
    if (!ok) throw new Error('requeueDLQ returned false')
    const d2 = Date.now() + 15_000
    while (succeeded.length === 0 && Date.now() < d2) await sleep(200)
    if (succeeded.length === 0)
      throw new Error('requeued message never consumed')
    await stop()
    await bus.close()
  }, 25_000)
})
