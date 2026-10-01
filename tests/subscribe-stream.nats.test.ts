import { jetstreamManager } from '@nats-io/jetstream'
import { connect as connectRaw } from '@nats-io/transport-node'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { type Server, start as startNats } from '../src/natsrun/index.js'
import { connectNatsBus } from '../src/transport/nats/index.js'

/**
 * Regression: `subscribeStream` used to LEAK its ordered JetStream consumer.
 * The consumer's `inactive_threshold` was a nanos-scale value that nats.js
 * re-scaled to ~694 days, and `close()` only tore down the local iterator
 * (never `consumer.delete()`), so every watch accumulated an ephemeral
 * consumer on the events stream until the server saturated.
 *
 * This proves the fix: after the subscription is closed the consumer is gone
 * from the stream IMMEDIATELY — no waiting for any timeout.
 */
const STREAM_EVENTS = 'ABC_EVENTS'

let server: Server | null = null
let url: string | null = null

beforeAll(async () => {
  const external =
    process.env.ABC_NATS_URL ??
    (process.env.NATS_URL?.includes('develop') === true
      ? process.env.NATS_URL
      : undefined)
  if (external !== undefined) {
    url = external
    return
  }
  try {
    server = await startNats({ storage: 'memory' })
    url = server.url
  } catch {
    url = null
  }
}, 30_000)

afterAll(async () => {
  await server?.stop()
})

async function consumerCount(): Promise<number> {
  const nc = await connectRaw({ servers: url ?? '' })
  try {
    const jsm = await jetstreamManager(nc)
    const info = await jsm.streams.info(STREAM_EVENTS)
    return info.state.consumer_count
  } finally {
    await nc.close()
  }
}

describe('subscribeStream consumer lifecycle', () => {
  it('deletes the server-side consumer on close', async () => {
    if (url === null) {
      // No broker available (CI/local without nats-server): skip silently.
      return
    }
    const bus = await connectNatsBus(url)
    try {
      const sub = await bus.subscribeStream('abc.T1.session.events.S1')
      // Drain like production does; `close()` needs a live iterator to unwind.
      const pump = (async () => {
        try {
          for await (const _ of sub) {
            // no-op
          }
        } catch {
          // closed
        }
      })()
      await new Promise(r => setTimeout(r, 100))
      expect(await consumerCount()).toBe(1)

      await sub.close()
      await pump.catch(() => {})

      // Gone IMMEDIATELY after close (not after an inactive timeout).
      expect(await consumerCount()).toBe(0)
    } finally {
      await bus.close().catch(() => {})
    }
  })
})
