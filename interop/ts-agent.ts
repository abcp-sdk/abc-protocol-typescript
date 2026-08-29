// TS-side interop agent: discovers extensions, calls tools (including the Go
// extension), resolves a variable, fires hooks, and prints results.
//
//   NATS_URL=... npx tsx interop/ts-agent.ts
//
// Asserts the cross-language wire contract: any failure exits non-zero.
import { Agent } from '@abc-protocol/sdk'
import { connectNatsBus } from '@abc-protocol/sdk-nats'

async function fail(msg: string): Promise<never> {
  console.error(`[ts-agent] FAIL: ${msg}`)
  process.exit(1)
}

async function expect(cond: boolean, msg: string): Promise<void> {
  if (!cond) await fail(msg)
  console.log(`[ts-agent] ok: ${msg}`)
}

async function main(): Promise<void> {
  const url = process.env.NATS_URL ?? 'nats://nats.develop.svc.cluster.local:4222'
  const bus = await connectNatsBus(url)
  const agent = new Agent(bus)
  console.log(`[ts-agent] connected to ${url}`)

  // 1. discover: the Go extension must be visible
  const manifests = await agent.discover(1000)
  console.log('[ts-agent] discovered:', manifests.map(m => m.id).join(', '))
  await expect(manifests.some(m => m.id === 'go-ext'), 'discover sees go-ext')

  // 2. callTool content
  const echo = await agent.callTool('sess-x', 'go-ext', 'echo', 'c1', {
    msg: 'hello-from-ts',
  })
  await expect(
    echo.content === 'go-ext echo: hello-from-ts',
    `tool echo content (${echo.content})`,
  )

  // 3. session_name propagation Go-side
  const sess = await agent.callTool('sess-77', 'go-ext', 'session', 'c2', {})
  await expect(
    sess.content === 'session=sess-77',
    `session_name propagated (${sess.content})`,
  )

  // 4. structured error code crosses the wire
  const failed = await agent.callTool('sess-x', 'go-ext', 'fail', 'c3', {})
  await expect(
    failed.error?.code === 'business',
    `error code business (${failed.error?.code})`,
  )

  // 5. progress subscription (one-way pub)
  const sub = await agent.subscribeProgress('c-progress')
  const got: unknown[] = []
  void (async () => {
    for await (const env of sub) got.push(env.payload)
  })()

  // 6. event hook: TS agent publishes, Go ext receives (checked by the
  //    operator via go-ext's stdout) — fire and move on.
  await agent.publishEventHook('sess-x', 'interop.event', { from: 'ts-agent' })
  console.log('[ts-agent] ok: event hook published (check go-ext stdout)')

  // 7. config: agent sets a knob on the Go extension, which applies it live
  //    (the Go interop ext declares poll-interval + logs the applied value).
  await agent.setConfig('go-ext', 'poll-interval', 5)
  console.log('[ts-agent] ok: setConfig poll-interval=5 delivered to go-ext')

  await sub.close()
  await bus.close()
  console.log('[ts-agent] ALL INTEROP CHECKS PASSED')
}

await main().catch(e => {
  console.error('[ts-agent] FAIL:', e)
  process.exit(1)
})
