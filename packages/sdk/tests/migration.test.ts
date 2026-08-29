import { jetstreamManager } from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'
import { describe, it } from 'vitest'
import { Agent } from '../src/agent/index.js'
import { Extension, publishSessionEvent } from '../src/extension/index.js'
import { start as startNats } from '../src/natsrun/index.js'
import { connectNatsBus } from '../src/transport/nats/index.js'

describe('abc stream topology reconcile', () => {
  it('reconciles drifted stream subjects (declarative topology)', async () => {
    const server = await startNats({ storage: 'memory' })
    try {
      const nc = await connect({ servers: server.url })
      const jsm = await jetstreamManager(nc)
      await jsm.streams.delete('ABC_MAILBOX').catch(() => {})
      await jsm.streams.delete('ABC_EVENTS').catch(() => {})
      await jsm.streams.delete('ABC_DLQ').catch(() => {})
      await jsm.streams.add({
        name: 'ABC_MAILBOX',
        subjects: ['abc.mailbox.>', 'abc.session.events.>'],
      })
      await nc.close()

      const bus = await connectNatsBus(server.url)
      await publishSessionEvent(bus, 'sess-reconcile', 'ok')
      const a = new Agent(bus)
      const envs = await a.replayEvents('sess-reconcile')
      if (envs.length === 0) throw new Error('reconcile broke event replay')
      await bus.close()
    } finally {
      await server.stop()
    }
  })
})
