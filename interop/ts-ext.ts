// TS-side interop extension: serves an `echo` tool + variable + hooks over
// NATS, for the Go agent to call.
//
//   NATS_URL=... npx tsx interop/ts-ext.ts
import type { ExtensionConfig } from '@abc-protocol/sdk'
import { Extension } from '@abc-protocol/sdk'
import { connectNatsBus } from '@abc-protocol/sdk'

const config: ExtensionConfig = {
  id: 'ts-ext',
  version: '1.0',
  tools: {
    echo: {
      description: 'echo content back',
      execute: async args => ({
        content: `ts-ext echo: ${String(args?.msg ?? '')}`,
      }),
    },
    add: {
      description: 'structured data result',
      execute: async args => ({
        data: { sum: Number(args?.a ?? 0) + Number(args?.b ?? 0) },
      }),
    },
  },
  config: {
    'poll-interval': {
      description: 'seconds between polls',
      type: 'number',
      default: 30,
    },
  },
  onConfigChange: (name, value) => {
    process.stdout.write(`[ts-ext] config applied: ${name}=${JSON.stringify(value)}\n`)
  },
  variables: {
    'ts-var': {
      description: 'a TS-provided variable',
      resolve: sessionName => `from-ts-ext/${sessionName ?? ''}`,
    },
  },
  callHooks: ['interop.before'],
  onCallHook: async (hook, sessionName) => ({
    ok: true,
    data: { hook, session: sessionName },
  }),
  eventHooks: ['interop.event'],
  onEventHook: (hook, sessionName, payload) => {
    // process.stdout.write is synchronous for pipes/files; console.log may
    // buffer in nohup contexts and get cut off before flush.
    process.stdout.write(
      `[ts-ext] event hook=${hook} session=${sessionName} payload=${JSON.stringify(payload)}\n`,
    )
  },
}

const url = process.env.NATS_URL ?? 'nats://nats.develop.svc.cluster.local:4222'
const bus = await connectNatsBus(url)
const ext = new Extension(bus, config)
await ext.serve()
process.stdout.write(`[ts-ext] serving ts-ext over ${url}\n`)

// serve until Ctrl-C
await new Promise(() => {})
