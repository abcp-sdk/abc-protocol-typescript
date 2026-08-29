import { Agent } from '@abc-protocol/sdk'

/** Drive an already-built Agent: discover, call tools, resolve variables, hooks. */
export async function runMockAgent(agent: Agent): Promise<void> {
  const manifests = await agent.discover(500)
  console.log('[mock-agent] discovered:', manifests.map(m => m.id).join(', '))

  const echo = await agent.callTool('sess-1', 'mock-ext', 'echo', 'c1', { msg: 'hello' })
  console.log('[mock-agent] echo:', echo.content)

  const add = await agent.callTool('sess-1', 'mock-ext', 'add', 'c2', { a: 2, b: 3 })
  console.log('[mock-agent] add:', JSON.stringify(add.data))

  const url = await agent.resolveVariable('mock-ext', 'base-url')
  console.log('[mock-agent] base-url:', url)

  const hook = await agent.callHook('sess-1', 'mock-ext', 'session.before_create', { org: 'x' })
  console.log('[mock-agent] before_create ok:', hook.ok, JSON.stringify(hook.data))
}