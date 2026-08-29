/**
 * AI-SDK example agent — how an existing agent (like zergx) adopts ABC.
 *
 * Architecture mirrors ~/rucoder-neo/agent-ts:
 *
 *   LLM (Vercel AI SDK) ── tools ──> buildAiTools(discovered) ──> ABC Agent
 *        ^                                                        │
 *        └──── tool results fed back as ToolResultContent ────────┘
 *
 * Events reach the agent loop through three independent doors (see README):
 *   - mailbox     (durable, per-session; user_prompt / interrupt / event)
 *   - event hooks (async broadcast, best-effort)
 *   - progress    (per-call, UI-only, never enters the LLM context)
 *
 * Run (needs `ai` + an `@ai-sdk/*` provider installed):
 *   OPENAI_API_KEY=sk-... NATS_URL=... npx tsx examples/ai-agent.ts
 *   npx tsx examples/ai-agent.ts --no-llm     # dry loop, no API key needed
 */
import { Agent } from '@abc-protocol/sdk'
import { connectNatsBus } from '@abc-protocol/sdk'
import type { ToolResult } from '@abc-protocol/sdk'
import type { Tool } from 'ai'

const url = process.env.NATS_URL ?? 'nats://nats.develop.svc.cluster.local:4222'

// ---------------------------------------------------------------------------
// 1. Discovery -> AI tools (same shape zergx uses in tools.ts)

interface DiscoveredTool {
  extId: string
  name: string
  description: string
  inputSchema: Record<string, unknown>
}

async function discoverTools(agent: Agent): Promise<DiscoveredTool[]> {
  let manifests
  try {
    manifests = await agent.discover(1000)
  } catch {
    // NoResponders: nothing is serving yet — treat as empty.
    manifests = []
  }
  const out: DiscoveredTool[] = []
  for (const m of manifests) {
    for (const t of m.tools ?? []) {
      out.push({
        extId: m.id,
        name: t.name,
        description: t.description,
        inputSchema: t.input_schema ?? { type: 'object', properties: {} },
      })
    }
  }
  return out
}

/**
 * Wrap every ABC tool as an AI SDK tool. `sessionName` rides the first-class
 * envelope field — the model can neither see nor forge it. callId is the AI
 * SDK's toolCallId, so progress telemetry lines up 1:1 with model tool calls.
 *
 * The `ai` package is imported lazily so the dry loop runs without it.
 */
function buildAiTools(
  discovered: DiscoveredTool[],
  agent: Agent,
  sessionName: string,
): Record<string, Tool> {
  // eslint-disable-next-line @typescript-eslint/no-require-imports -- lazy
  const { tool, jsonSchema } = require('ai') as typeof import('ai')
  const tools: Record<string, Tool> = {}
  for (const t of discovered) {
    // Namespace by extension id so same-named tools from different extensions
    // don't collide (same trick zergx uses).
    const aiName = `${t.extId}__${t.name}`
    tools[aiName] = tool({
      description: t.description,
      inputSchema: jsonSchema(t.inputSchema),
      execute: async (args, { toolCallId }) => {
        const result: ToolResult = await agent.callTool(
          sessionName,
          t.extId,
          t.name,
          toolCallId,
          args ?? {},
        )
        return toToolResultContent(result)
      },
    })
  }
  return tools
}

/** Flatten the three result shapes into text for the LLM context. */
function toToolResultContent(r: ToolResult): string {
  if (r.error !== undefined) {
    return `error ${r.error.code}: ${r.error.message}`
  }
  if (r.content !== undefined) return r.content
  if (r.data !== undefined) return JSON.stringify(r.data)
  if (r.object !== undefined) return `[object ${r.object.id} — fetch via getObject]`
  return ''
}

// ---------------------------------------------------------------------------
// 2. The three event doors

async function startMailboxLoop(agent: Agent, onMessage: (m: { type: string; payload: unknown }) => void): Promise<() => Promise<void>> {
  return agent.consumeMailbox(async msg => {
    // user_prompt messages start a turn; interrupt/act on your loop.
    onMessage({ type: msg.type, payload: msg.payload })
  })
}

function startProgressLoop(_agent: Agent, onProgress: (p: Record<string, unknown>) => void): () => void {
  void onProgress
  return () => {}
}

// ---------------------------------------------------------------------------
// 3. Wire it together

async function main(): Promise<void> {
  const bus = await connectNatsBus(url)
  const agent = new Agent(bus)
  const sessionName = `demo-${Date.now()}`
  console.log(`[ai-agent] connected to ${url}, session=${sessionName}`)

  const discovered = await discoverTools(agent)
  console.log(
    '[ai-agent] tools:',
    discovered.map(t => `${t.extId}.${t.name}`).join(', ') || '(none)',
  )

  // Mailbox door: durable per-session messages.
  const stopMailbox = await startMailboxLoop(agent, m => {
    console.log(`[ai-agent] mailbox ${m.type}:`, JSON.stringify(m.payload))
    if (m.type === 'interrupt') {
      console.log('[ai-agent] -> would abort the current turn here')
    }
  })

  const dry = process.argv.includes('--no-llm')
  if (dry || process.env.OPENAI_API_KEY === undefined) {
    // Dry loop: exercise one tool call directly so the example runs without
    // an API key, then listen on the mailbox for a while.
    const first = discovered[0]
    if (first !== undefined) {
      const r = await agent.callTool(sessionName, first.extId, first.name, 'dry-1', {
        msg: 'hello',
      })
      console.log('[ai-agent] dry tool result:', JSON.stringify(r))
    }
    console.log('[ai-agent] listening on the mailbox for 10s (publish to it!)')
    await new Promise(r => setTimeout(r, 10_000))
  } else {
    const { streamText } = await import('ai')
    const { createOpenAI } = await import('@ai-sdk/openai')
    const openai = createOpenAI({})
    const tools = buildAiTools(discovered, agent, sessionName)
    const stream = streamText({
      model: openai('gpt-4o-mini'),
      system: 'You are a helpful assistant with tools on the ABC bus.',
      prompt: 'Say hi and use any available echo-ish tool.',
      tools,
      maxSteps: 5,
    })
    for await (const chunk of stream.textStream) process.stdout.write(chunk)
    process.stdout.write('\n')
  }

  stopMailbox()
  await bus.close()
}

await main().catch(e => {
  console.error('[ai-agent] failed:', e)
  process.exit(1)
})
