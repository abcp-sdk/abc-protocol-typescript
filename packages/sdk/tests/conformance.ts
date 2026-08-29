import { jetstreamManager } from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'
import { describe, expect, it } from 'vitest'
import {
  Agent,
  type MailboxMessageResolved,
  TermError,
} from '../src/agent/index.js'
import {
  claimSession,
  isSessionRunning,
  releaseSession,
  renewSession,
  withSessionLease,
} from '../src/agent/lease.js'
import type { Bus } from '../src/bus/index.js'
import {
  Extension,
  publishMailboxEvent,
  publishSessionEvent,
  setSessionVariable,
  type ToolResultData,
} from '../src/extension/index.js'
import { start as startNats } from '../src/natsrun/index.js'
import type { ExtensionManifest } from '../src/protocol/index.js'
import { CH, type LifecycleEvent, sessionToken } from '../src/protocol/index.js'
import { connectNatsBus } from '../src/transport/nats/index.js'

export interface Pair {
  agentBus: Bus
  extensionBus: Bus
  cleanup: () => Promise<void> | void
}

export type Factory = () => Promise<Pair> | Pair

const serveConfExt = async (bus: Bus): Promise<Extension> => {
  const ext = new Extension(bus, {
    id: 'conf-ext',
    version: '1.0',
    tools: {
      echo: {
        description: 'echo content',
        execute: async args => ({ content: `echo:${String(args?.msg ?? '')}` }),
      },
      hang: {
        description: 'waits until interrupted (interrupt semantics)',
        execute: async () => {
          await sleep(30_000)
          return { content: 'never' }
        },
      },
      slow: {
        description: 'sleeps before answering (request-timeout regression)',
        execute: async () => {
          await sleep(2500)
          return { content: 'woke' }
        },
      },
      add: {
        description: 'structured data result',
        execute: async args => ({
          data: { sum: Number(args?.a ?? 0) + Number(args?.b ?? 0) },
        }),
      },
      big: {
        description: 'returns >256KB content (object offload)',
        execute: async () => ({ content: 'x'.repeat(300 * 1024) }),
      },
      session: {
        description: 'echoes the session name it was called with',
        execute: async (_args, _callId, sessionName) => ({
          content: `session=${sessionName}`,
        }),
      },
      boom: {
        description: 'always fails',
        execute: async () => {
          throw new Error('nope')
        },
      },
    },
    variables: {
      'base-url': {
        description: 'the base url',
        resolve: sessionName => `https://example.com/${sessionName ?? ''}`,
      },
    },
    callHooks: ['session.before_create'],
    onCallHook: async (hook, sessionName) => ({
      ok: true,
      data: { session: sessionName, hook },
    }),
    eventHooks: ['session.created'],
    onEventHook: (hook, sessionName, payload) => {
      events.push({ hook, sessionName, payload })
    },
  })
  await ext.serve()
  return ext
}

const events: Array<{
  hook: string
  sessionName: string
  payload?: unknown
}> = []

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

/** Run the full conformance suite against one transport factory. */
export function runConformance(name: string, newPair: Factory): void {
  describe(`abc conformance: ${name}`, () => {
    it('discovers extensions', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = await serveConfExt(extensionBus)
      const ms = await new Agent(agentBus).discover(300)
      expect(ms.map(m => m.id)).toContain('conf-ext')
      await ext.close()
      await cleanup()
    })

    it('tool returns content', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = await serveConfExt(extensionBus)
      const tr = await new Agent(agentBus).callTool(
        'sess-1',
        'conf-ext',
        'echo',
        'c1',
        { msg: 'hi' },
      )
      expect(tr.content).toBe('echo:hi')
      await ext.close()
      await cleanup()
    })

    it('tool returns structured data', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = await serveConfExt(extensionBus)
      const tr = await new Agent(agentBus).callTool(
        'sess-1',
        'conf-ext',
        'add',
        'c2',
        { a: 2, b: 3 },
      )
      expect(tr.data).toEqual({ sum: 5 })
      await ext.close()
      await cleanup()
    })

    it('tool offloads large content to the object store', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = await serveConfExt(extensionBus)
      const a = new Agent(agentBus)
      const tr = await a.callTool('sess-1', 'conf-ext', 'big', 'c3', {})
      expect(tr.object?.id).toBeTruthy()
      const bytes = await a.getObject(tr.object?.id ?? '')
      expect(bytes?.length).toBe(300 * 1024)
      await ext.close()
      await cleanup()
    })

    it('tool maps thrown errors to the internal code', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = await serveConfExt(extensionBus)
      const tr = await new Agent(agentBus).callTool(
        'sess-1',
        'conf-ext',
        'boom',
        'c4',
        {},
      )
      expect(tr.error?.code).toBe('internal')
      await ext.close()
      await cleanup()
    })

    it('propagates session_name to the tool', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = await serveConfExt(extensionBus)
      const tr = await new Agent(agentBus).callTool(
        'sess-42',
        'conf-ext',
        'session',
        'c5',
        {},
      )
      expect(tr.content).toBe('session=sess-42')
      await ext.close()
      await cleanup()
    })

    it('times out on unknown tools despite timeoutMs=0 default', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = await serveConfExt(extensionBus)
      const a = new Agent(agentBus)
      await expect(
        Promise.race([
          a.callTool('sess-1', 'conf-ext', 'missing', 'c6', {}),
          sleep(3000).then(() => {
            throw new Error('timeout guard')
          }),
        ]),
      ).rejects.toThrow()
      await ext.close()
      await cleanup()
    })

    it('interrupt cancels the in-flight tool of one session only', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = await serveConfExt(extensionBus)
      const a = new Agent(agentBus)
      const call = (session: string, callId: string) =>
        a.callTool(session, 'conf-ext', 'hang', callId, {}).then(
          tr => tr,
          () => undefined,
        )
      const mine = call('sess-int', 'hang-1')
      const other = call('sess-other', 'hang-2')
      await sleep(500)

      await a.interrupt('conf-ext', 'sess-int', 'test')
      const mineRes = await Promise.race([
        mine,
        sleep(5000).then(() => {
          throw new Error('session interrupt did not cancel within 5s')
        }),
      ])
      if (mineRes?.error?.message?.includes('interrupted') !== true) {
        throw new Error(`interrupted outcome = ${JSON.stringify(mineRes)}`)
      }
      // the other session must still be running
      const otherDone = await Promise.race([
        other.then(() => true),
        sleep(700).then(() => false),
      ])
      if (otherDone)
        throw new Error('session-scoped interrupt leaked to other session')

      // broadcast interrupt reaches the remaining session
      await a.interrupt('conf-ext')
      const otherRes = await Promise.race([
        other,
        sleep(5000).then(() => {
          throw new Error('broadcast interrupt did not cancel within 5s')
        }),
      ])
      if (otherRes?.error?.message?.includes('interrupted') !== true) {
        throw new Error(`broadcast outcome = ${JSON.stringify(otherRes)}`)
      }
      await ext.close()
      await cleanup()
    })

    it('slow tool: no transport-level request cap when timeoutMs=0', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = await serveConfExt(extensionBus)
      const a = new Agent(agentBus)
      const tr = await a.callTool('sess-1', 'conf-ext', 'slow', 'slow-1', {})
      expect(tr.content).toBe('woke')
      await ext.close()
      await cleanup()
    })

    it('resolves and misses variables', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = await serveConfExt(extensionBus)
      const a = new Agent(agentBus)
      expect(await a.resolveVariable('conf-ext', 'base-url', 'sess-1')).toBe(
        'https://example.com/sess-1',
      )
      expect(await a.resolveVariable('conf-ext', 'nope')).toBeNull()
      await ext.close()
      await cleanup()
    })

    it('runs a sync call hook', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = await serveConfExt(extensionBus)
      const hr = await new Agent(agentBus).callHook(
        'sess-h',
        'conf-ext',
        'session.before_create',
        { k: 'v' },
      )
      expect(hr.ok).toBe(true)
      expect(hr.data).toEqual({
        session: 'sess-h',
        hook: 'session.before_create',
      })
      await ext.close()
      await cleanup()
    })

    it('delivers event hooks', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      events.length = 0
      const ext = await serveConfExt(extensionBus)
      await new Agent(agentBus).publishEventHook('sess-ev', 'session.created', {
        x: 1,
      })
      await sleep(100)
      expect(events).toContainEqual({
        hook: 'session.created',
        sessionName: 'sess-ev',
        payload: { x: 1 },
      })
      await ext.close()
      await cleanup()
    })

    it('delivers interrupts', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      events.length = 0
      const ext = await serveConfExt(extensionBus)
      await new Agent(agentBus).interrupt('conf-ext', 'sess-i', 'stop')
      await sleep(100)
      expect(events).toContainEqual({
        hook: 'interrupt',
        sessionName: 'sess-i',
        payload: 'stop',
      })
      await ext.close()
      await cleanup()
    })

    it('streams progress telemetry as one-way pub', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = await serveConfExt(extensionBus)
      const a = new Agent(agentBus)
      const sub = await a.subscribeProgress('c-progress')
      const collected: unknown[] = []
      void (async () => {
        for await (const env of sub) collected.push(env.payload)
      })()
      await ext.reportProgress('c-progress', {
        phase: 'sync',
        progress: 0.5,
        text: 'half',
      })
      await sleep(50)
      await sub.close()
      expect(collected).toMatchObject([
        { call_id: 'c-progress', phase: 'sync', progress: 0.5 },
      ])
      await ext.close()
      await cleanup()
    })

    it('publishes and consumes the durable mailbox', async () => {
      const { agentBus, cleanup } = await newPair()
      const received: MailboxMessageResolved[] = []
      const cancel = await new Agent(agentBus).consumeMailbox(async msg => {
        received.push(msg)
      })
      await sleep(50)
      await new Agent(agentBus).publishMailbox('sess-mb', 'user_prompt', {
        text: 'hello',
      })
      await sleep(150)
      expect(received).toHaveLength(1)
      expect(received[0]).toMatchObject({
        sessionName: 'sess-mb',
        type: 'user_prompt',
        payload: { text: 'hello' },
      })
      await cancel()
      await cleanup()
    })

    it('round-trips the object store', async () => {
      const { agentBus, cleanup } = await newPair()
      const a = new Agent(agentBus)
      await a.putObject('test-obj', new TextEncoder().encode('payload-123'))
      const bytes = await a.getObject('test-obj')
      expect(new TextDecoder().decode(bytes ?? new Uint8Array())).toBe(
        'payload-123',
      )
      expect(await a.getObject('absent-obj')).toBeNull()
      await cleanup()
    })

    it('supports kv create/get/cas/delete', async () => {
      const { agentBus, cleanup } = await newPair()
      const b = new Agent(agentBus).rawBus
      const rev = await b.kvCreate('conf-kv', 'k', 'v1', 60_000)
      expect(rev).not.toBeNull()
      expect(await b.kvCreate('conf-kv', 'k', 'v2', 60_000)).toBeNull()
      expect(await b.kvGet('conf-kv', 'k')).toBe('v1')
      const cas = await b.kvCas('conf-kv', 'k', 'v2', rev as number)
      expect(cas).not.toBeNull()
      expect(await b.kvCas('conf-kv', 'k', 'v3', rev as number)).toBeNull()
      expect(await b.kvGet('conf-kv', 'k')).toBe('v2')
      await b.kvDelete('conf-kv', 'k')
      expect(await b.kvGet('conf-kv', 'k')).toBeNull()
      await cleanup()
    })

    it('exposes the expected channel/token derivations', async () => {
      expect(CH.DISCOVER).toBe('abc.discover')
      expect(CH.toolCall('ops', 'echo')).toBe('abc.tool.call.ops.echo')
      expect(CH.toolProgress('c1')).toBe('abc.tool.progress.c1')
      expect(CH.variable('ops', 'base-url')).toBe('abc.var.ops.base-url')
      expect(CH.interrupt('ops')).toBe('abc.ctl.interrupt.ops')
      expect(CH.hookCall('ops', 'session.before_create')).toBe(
        'abc.hook.call.ops.session.before_create',
      )
      expect(CH.hookEvent('session.created')).toBe(
        'abc.hook.event.session.created',
      )
      expect(CH.config('ops')).toBe('abc.config.ops')
      expect(CH.configGet('ops')).toBe('abc.config.get.ops')
      expect(sessionToken('sess-1')).toBe(GOLDEN.sessionTokenSess1)
      await cleanupNoop()
    })

    it('applies a config change on the extension', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const applied: Array<{ name: string; value: unknown; session?: string }> =
        []
      const ext = new Extension(extensionBus, {
        id: 'cfg-ext',
        version: '1.0',
        config: {
          'poll-interval': {
            description: 'seconds between polls',
            type: 'number',
            default: 30,
          },
        },
        onConfigChange: name => {
          applied.push({ name, value: ext.getConfig('poll-interval') })
        },
      })
      await ext.serve()
      await sleep(50)

      const a = new Agent(agentBus)
      await a.discover(300)
      await a.setConfig('cfg-ext', 'poll-interval', 5)
      await sleep(100)
      expect(applied).toContainEqual({ name: 'poll-interval', value: 5 })
      expect(ext.getConfig('poll-interval')).toBe(5)
      await ext.close()
      await cleanup()
    })

    it('rejects a config change the extension refuses', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = new Extension(extensionBus, {
        id: 'cfg-rej',
        version: '1.0',
        config: {
          knob: { type: 'json', default: { on: false } },
        },
        onConfigChange: name => {
          if (name === 'knob') throw new Error('refused')
        },
      })
      await ext.serve()
      await sleep(50)

      const a = new Agent(agentBus)
      await a.discover(300)
      await expect(
        a.setConfig('cfg-rej', 'knob', { on: true }),
      ).rejects.toThrow()
      // The old value stays effective.
      expect(ext.getConfig('knob')).toEqual({ on: false })
      await ext.close()
      await cleanup()
    })

    it('validates config values against the manifest declaration', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = new Extension(extensionBus, {
        id: 'cfg-val',
        version: '1.0',
        config: {
          limit: { type: 'number', default: 10 },
          mode: { type: 'enum', enumValues: ['fast', 'safe'], default: 'safe' },
        },
      })
      await ext.serve()
      await sleep(50)

      const a = new Agent(agentBus)
      await a.discover(300)
      await expect(a.setConfig('cfg-val', 'limit', 'fast')).rejects.toThrow()
      await expect(a.setConfig('cfg-val', 'mode', 'turbo')).rejects.toThrow()
      await expect(a.setConfig('cfg-val', 'nope', 1)).rejects.toThrow()
      await ext.close()
      await cleanup()
    })

    it('serves a startup snapshot to late extensions', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const first = new Extension(extensionBus, {
        id: 'cfg-snap',
        version: '1.0',
        config: { k: { type: 'number', default: 1 } },
        onConfigChange: () => {},
      })
      await first.serve()
      await sleep(50)

      const a = new Agent(agentBus)
      await a.discover(300)
      await a.serveConfig()
      await a.setConfig('cfg-snap', 'k', 42)
      await sleep(50)

      const late = new Extension(extensionBus, {
        id: 'cfg-snap',
        version: '1.0',
        config: { k: { type: 'number', default: 1 } },
      })
      await late.serve()
      await sleep(50)
      expect(late.getConfig('k')).toBe(42)
      await first.close()
      await late.close()
      await cleanup()
    })

    it('overrides global config per session', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = new Extension(extensionBus, {
        id: 'cfg-sess',
        version: '1.0',
        config: {
          threshold: { type: 'number', default: 1, scope: 'session' },
          verbose: { type: 'boolean', default: false },
        },
      })
      await ext.serve()
      await sleep(50)

      const a = new Agent(agentBus)
      await a.discover(300)
      await a.setConfig('cfg-sess', 'verbose', true)
      await a.setConfig('cfg-sess', 'threshold', 7, 'sess-a')
      await sleep(100)
      expect(ext.getConfig('verbose')).toBe(true)
      expect(ext.getConfig('threshold', 'sess-a')).toBe(7)
      expect(ext.getConfig('threshold')).toBe(1)
      await ext.close()
      await cleanup()
    })

    it('delivers lifecycle events and cleans up on deleted', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const seen: LifecycleEvent[] = []
      const ext = new Extension(extensionBus, {
        id: 'lc-ext',
        version: '1.0',
        variables: {
          ws: {
            scope: 'session',
            resolve: session => `x-${session ?? ''}`,
          },
        },
        lifecycle: ['created', 'forked', 'renamed', 'deleted'],
        onLifecycle: ev => {
          seen.push(ev)
        },
      })
      await ext.serve()
      await sleep(50)

      const a = new Agent(agentBus)
      await a.publishLifecycleEvent('created', 'sess-lc')
      await a.publishLifecycleEvent('forked', 'sess-lc', { parent: 'parent-s' })
      await a.publishLifecycleEvent('renamed', 'sess-lc2', {
        from: 'sess-lc',
        to: 'sess-lc2',
      })
      await a.publishLifecycleEvent('deleted', 'sess-lc2')
      await sleep(150)
      expect(seen.map(e => e.kind)).toEqual([
        'created',
        'forked',
        'renamed',
        'deleted',
      ])
      const forked = seen.find(e => e.kind === 'forked')
      expect(forked?.parent).toBe('parent-s')
      const renamed = seen.find(e => e.kind === 'renamed')
      expect(renamed?.from).toBe('sess-lc')
      expect(renamed?.to).toBe('sess-lc2')
      expect(seen.every(e => e.session_name !== '')).toBe(true)
      await ext.close()
      await cleanup()
    })

    it('publishes session events to the durable stream', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = new Extension(extensionBus, {
        id: 'se-ext',
        version: '1.0',
        tools: {
          ping: {
            description: 'p',
            execute: async () => ({ content: 'pong' }),
          },
        },
      })
      await ext.serve()
      await sleep(50)

      const sub = await agentBus.inboxConsume({
        subject: CH.sessionEvents('sess-se'),
      })
      await sleep(50)
      await publishSessionEvent(extensionBus, 'sess-se', 'todos-updated', {
        count: 3,
      })
      const got: unknown[] = []
      void (async () => {
        for await (const m of sub) {
          got.push(m.payload)
          await m.ack()
        }
      })()
      await sleep(150)
      expect(got).toMatchObject([
        { event: 'todos-updated', params: { count: 3 } },
      ])
      await sub.close()
      await ext.close()
      await cleanup()
    })

    it('replays retained session events oldest-first', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = new Extension(extensionBus, {
        id: 'rp-ext',
        version: '1.0',
        tools: {
          ping: {
            description: 'p',
            execute: async () => ({ content: 'pong' }),
          },
        },
      })
      await ext.serve()
      await sleep(50)

      await publishSessionEvent(extensionBus, 'sess-rp', 'status', {
        type: 'busy',
      })
      await publishSessionEvent(extensionBus, 'sess-rp', 'text', { t: 'a' })
      await publishSessionEvent(extensionBus, 'sess-rp', 'turn-complete', {
        reason: 'end',
      })
      await sleep(150)

      const a = new Agent(agentBus)
      const events = await a.replayEvents('sess-rp')
      expect(events.map(e => e.event)).toEqual([
        'status',
        'text',
        'turn-complete',
      ])
      expect(events.every(e => typeof e.eid === 'string' && e.eid !== '')).toBe(
        true,
      )
      await ext.close()
      await cleanup()
    })

    it('term routes to the dead-letter stream; ack/discard do not', async () => {
      const { agentBus, cleanup } = await newPair()
      const a = new Agent(agentBus)
      await a.publishMailbox('sess-dlq', 'poison', { bad: true })
      await a.publishMailbox('sess-dlq', 'healthy', { ok: true })
      await a.publishMailbox('sess-dlq', 'discard', { gone: true })

      let healthySeen = false
      const stop = await a.consumeMailbox(m => {
        if (m.type === 'poison') throw new TermError()
        if (m.type === 'discard') throw new TermError(true)
        healthySeen = true
      })
      const deadline = Date.now() + 10_000
      while (!healthySeen && Date.now() < deadline) await sleep(200)
      if (!healthySeen) throw new Error('healthy message never consumed')
      await stop()

      const dlq: string[] = []
      const dlqStop = await a.consumeDLQ(m => {
        dlq.push(m.type)
      })
      const dlqDeadline = Date.now() + 10_000
      while (dlq.length < 1 && Date.now() < dlqDeadline) await sleep(200)
      await dlqStop()
      if (dlq.length !== 1 || dlq[0] !== 'poison') {
        throw new Error(
          `DLQ contents = ${JSON.stringify(dlq)}, want exactly ["poison"]`,
        )
      }
      await cleanup()
    })

    it('tool observes the abort signal (cooperative cleanup)', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      let observed = ''
      const ext = new Extension(extensionBus, {
        id: 'conf-ext',
        version: '1.0',
        tools: {
          watch: {
            description: 'watches the abort signal',
            execute: (_args, _callId, _session, signal) =>
              new Promise<ToolResultData | undefined>(() => {
                signal?.addEventListener('abort', () => {
                  observed = 'cleaned'
                })
              }),
          },
        },
      })
      await ext.serve()
      const a = new Agent(agentBus)
      const pending = a.callTool('sess-sig', 'conf-ext', 'watch', 'w1', {})
      await sleep(500)
      await a.interrupt('conf-ext', 'sess-sig', 'cleanup-test')
      const res = await pending.catch(() => undefined)
      if (!String(res?.error?.message ?? '').includes('interrupted')) {
        throw new Error(`interrupted outcome = ${JSON.stringify(res)}`)
      }
      const deadline = Date.now() + 5_000
      while (observed !== 'cleaned' && Date.now() < deadline) await sleep(100)
      if (observed !== 'cleaned')
        throw new Error('signal never aborted for the handler')
      await ext.close()
      await cleanup()
    })

    it('kv watch: snapshot, then live updates and deletes', async () => {
      const { agentBus, cleanup } = await newPair()
      await agentBus.kvPut('watch-test', 'k.a', '1', 0)
      const { stream, stop } = await agentBus.kvWatch('watch-test', 'k.>')
      const it = stream[Symbol.asyncIterator]()
      const next = async () => (await it.next()).value
      let ev = await next()
      if (ev?.key !== 'k.a' || ev.value !== '1') {
        throw new Error(`snapshot event = ${JSON.stringify(ev)}`)
      }
      await agentBus.kvPut('watch-test', 'k.b', '2', 0)
      ev = await next()
      if (ev?.key !== 'k.b' || ev.value !== '2') {
        throw new Error(`update event = ${JSON.stringify(ev)}`)
      }
      await agentBus.kvDelete('watch-test', 'k.b')
      ev = await next()
      if (ev?.key !== 'k.b' || !ev.deleted) {
        throw new Error(`delete event = ${JSON.stringify(ev)}`)
      }
      await stop()
      await cleanup()
    })

    it('config recovers from the cfg KV bucket after extension restart', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const a = new Agent(agentBus)
      await a.serveConfig()
      const manifest = {
        id: 'conf-ext',
        version: '1.0',
        config: [{ name: 'recovered', type: 'string' }],
      } as const
      await a.setConfig('conf-ext', 'recovered', 'hello-kv', undefined, {
        manifest: manifest as unknown as ExtensionManifest,
      })

      // extension boots AFTER the set: state must arrive via the KV watch
      const ext = new Extension(extensionBus, {
        id: 'conf-ext',
        version: '1.0',
        config: { recovered: { type: 'string', default: 'fallback' } },
      })
      await ext.serve()
      const deadline = Date.now() + 10_000
      while (
        ext.getConfig('recovered') !== 'hello-kv' &&
        Date.now() < deadline
      ) {
        await sleep(200)
      }
      if (ext.getConfig('recovered') !== 'hello-kv') {
        throw new Error(
          `config not recovered; got ${String(ext.getConfig('recovered'))}`,
        )
      }
      await ext.close()
      await cleanup()
    })

    it('presence: serving extensions appear in the abc-presence bucket', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = await serveConfExt(extensionBus)
      const deadline = Date.now() + 10_000
      let raw: string | null = null
      while (Date.now() < deadline) {
        raw = await agentBus.kvGet('abc-presence', 'conf-ext')
        if (raw !== null) break
        await sleep(200)
      }
      if (raw === null)
        throw new Error('extension never appeared in the presence bucket')
      if (!raw.includes('conf-ext'))
        throw new Error(`presence value not a manifest: ${raw}`)
      await ext.close()
      await cleanup()
    })

    it('lets extensions publish into the session mailbox', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const ext = new Extension(extensionBus, {
        id: 'xm-ext',
        version: '1.0',
        tools: {
          ping: {
            description: 'p',
            execute: async () => ({ content: 'pong' }),
          },
        },
      })
      await ext.serve()
      await sleep(50)

      const received: MailboxMessageResolved[] = []
      const cancel = await new Agent(agentBus).consumeMailbox(async msg => {
        received.push(msg)
      })
      await sleep(50)
      await publishMailboxEvent(extensionBus, 'sess-xm', 'event', {
        done: true,
      })
      await sleep(150)
      expect(received).toMatchObject([
        { sessionName: 'sess-xm', type: 'event', payload: { done: true } },
      ])
      await cancel()
      await ext.close()
      await cleanup()
    })

    it('resolves variables KV-first with extension write-back', async () => {
      const { agentBus, extensionBus, cleanup } = await newPair()
      const session = `sess-kv-${Date.now()}`
      let resolves = 0
      const ext = new Extension(extensionBus, {
        id: 'var-ext',
        version: '1.0',
        variables: {
          ws: {
            scope: 'session',
            resolve: s => {
              resolves++
              return `ws-${s ?? ''}`
            },
          },
        },
      })
      await ext.serve()
      await sleep(50)

      const a = new Agent(agentBus)
      expect(await a.resolveVariable('var-ext', 'ws', session)).toBe(
        `ws-${session}`,
      )
      await setSessionVariable(
        extensionBus,
        'var-ext',
        session,
        'ws',
        'ws-cached',
      )
      expect(await a.resolveVariable('var-ext', 'ws', session)).toBe(
        'ws-cached',
      )
      expect(resolves).toBe(1)
      await ext.close()
      await cleanup()
    })

    it('enforces the cross-replica session lease', async () => {
      const { agentBus, cleanup } = await newPair()
      const session = `sess-lease-${Date.now()}`

      const rev = await claimSession(agentBus, session, 1000)
      expect(rev).not.toBeNull()
      // Mutual exclusion: a second claim is refused.
      expect(await claimSession(agentBus, session, 1000)).toBeNull()
      expect(await isSessionRunning(agentBus, session)).toBe(true)
      // Renew with the right revision works; a stale one loses.
      const next = await renewSession(agentBus, session, rev as number, 1000)
      expect(next).not.toBeNull()
      expect(
        await renewSession(agentBus, session, rev as number, 1000),
      ).toBeNull()
      // Release → free again.
      await releaseSession(agentBus, session)
      expect(await isSessionRunning(agentBus, session)).toBe(false)
      expect(await claimSession(agentBus, session, 1000)).not.toBeNull()
      await releaseSession(agentBus, session)
      await cleanup()
    })

    it('withSessionLease refuses concurrent runners and releases after', async () => {
      const { agentBus, cleanup } = await newPair()
      const session = `sess-lease-wrap-${Date.now()}`
      const first = await withSessionLease(
        agentBus,
        session,
        async () => {
          const second = await withSessionLease(agentBus, session, async () => {
            throw new Error('should not run')
          })
          expect(second.acquired).toBe(false)
        },
        1000,
      )
      expect(first.acquired).toBe(true)
      expect(first.lost).toBe(false)
      expect(await isSessionRunning(agentBus, session)).toBe(false)
      await cleanup()
    })
  })
}

async function cleanupNoop(): Promise<void> {}

import { GOLDEN } from './golden.js'
