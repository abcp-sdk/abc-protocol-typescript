import { jetstreamManager } from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'
import { describe, it } from 'vitest'
import { Agent } from '../src/agent/index.js'
import { Extension, publishSessionEvent } from '../src/extension/index.js'
import { start as startNats } from '../src/natsrun/index.js'
import { connectNatsBus } from '../src/transport/nats/index.js'

describe('abc migration: 0.1 -> 0.2 stream layout', () => {
  it('self-migrates a pre-0.2 layout on connect', async () => {
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
      await publishSessionEvent(bus, 'sess-mig', 'mig-done')
      const a = new Agent(bus)
      const envs = await a.replayEvents('sess-mig')
      if (envs.length === 0) {
        throw new Error('no events replayed from the post-migration stream')
      }
      void Extension
      await bus.close()
    } finally {
      await server.stop()
    }
  })
})
