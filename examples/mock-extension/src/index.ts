import type { ExtensionConfig } from '@abc-protocol/sdk'

/** A mock extension exposing echo/add/long tools + a variable + a call hook. */
export function createMockExtensionConfig(): ExtensionConfig {
  return {
    id: 'mock-ext',
    version: '1.0.0',
    tools: {
      echo: {
        description: 'Echo back the message',
        inputSchema: {
          type: 'object',
          properties: { msg: { type: 'string' } },
          required: ['msg'],
        },
        execute: async args => ({ content: `echo: ${String(args.msg ?? '')}` }),
      },
      add: {
        description: 'Add two numbers',
        inputSchema: {
          type: 'object',
          properties: { a: { type: 'number' }, b: { type: 'number' } },
          required: ['a', 'b'],
        },
        execute: async args => {
          const a = Number(args.a ?? 0)
          const b = Number(args.b ?? 0)
          return { data: { sum: a + b } }
        },
      },
      long: {
        description: 'A long-running task that reports progress',
        execute: async (_args, callId) => {
          await new Promise(r => setTimeout(r, 100))
          return { content: `long done (${callId})` }
        },
      },
    },
    variables: {
      'base-url': {
        description: 'The base URL of this extension',
        scope: 'global',
        resolve: () => 'https://example.com',
      },
    },
    callHooks: ['session.before_create'],
    onCallHook: async (hook, sessionName, args) => {
      if (hook === 'session.before_create') {
        return { ok: true, data: { repo: `repo-for-${sessionName}`, args } }
      }
      return { ok: false, error: { code: 'not_found', message: `unknown hook ${hook}` } }
    },
    eventHooks: ['interrupt', 'session.created'],
    onEventHook: async (hook, sessionName) => {
      console.log(`[mock-ext] event ${hook} session=${sessionName}`)
    },
  }
}