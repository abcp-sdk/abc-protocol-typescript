import { describe, expect, it } from 'vitest'
import {
  argBool,
  argFloat,
  argInt,
  argString,
  coerce,
  newId,
} from '../src/protocol/args.js'
import {
  toToolError,
  TypedToolError,
} from '../src/extension/typed-error.js'
import { manifestConfig } from '../src/manifest.js'

const args = {
  s: 'hi',
  n: 42,
  f: 3.5,
  b: true,
  nstr: '5',
  undef: undefined,
}

describe('protocol argument helpers', () => {
  it('argString returns the string or ""', () => {
    expect(argString(args, 's')).toBe('hi')
    expect(argString(args, 'n')).toBe('')
    expect(argString(args, 'missing')).toBe('')
  })

  it('argInt truncates / defaults on wrong type', () => {
    expect(argInt(args, 'n', 0)).toBe(42)
    expect(argInt(args, 'f', 0)).toBe(3)
    expect(argInt(args, 'nstr', 7)).toBe(7) // strings are not numbers
    expect(argInt(args, 'missing', 9)).toBe(9)
  })

  it('argFloat passes through / defaults', () => {
    expect(argFloat(args, 'f', 0)).toBe(3.5)
    expect(argFloat(args, 'n', 0)).toBe(42)
    expect(argFloat(args, 'missing', 1.25)).toBe(1.25)
  })

  it('argBool returns the boolean or the default', () => {
    expect(argBool(args, 'b', false)).toBe(true)
    expect(argBool(args, 's', true)).toBe(true)
    expect(argBool(args, 'missing', false)).toBe(false)
  })

  it('newId yields distinct uuids', () => {
    const a = newId()
    const b = newId()
    expect(a).toMatch(/^[0-9a-f-]{36}$/)
    expect(a).not.toBe(b)
  })

  it('coerce round-trips JSON-safe values and drops undefined', () => {
    expect(coerce<{ a: number }>({ a: 1 })).toEqual({ a: 1 })
    expect(coerce(undefined)).toBeUndefined()
  })
})

describe('TypedToolError', () => {
  it('preserves its code on the wire', () => {
    const e = new TypedToolError('not_found', 'gone')
    expect(toToolError(e)).toEqual({ code: 'not_found', message: 'gone' })
  })

  it('maps a plain error to internal', () => {
    expect(toToolError(new Error('boom'))).toEqual({
      code: 'internal',
      message: 'Error: boom',
    })
    expect(toToolError('plain')).toEqual({
      code: 'internal',
      message: 'plain',
    })
  })
})

describe('manifestConfig bindings', () => {
  it('forwards onConfigChange / onLifecycle / onInterrupt', () => {
    const onConfigChange = () => {}
    const onLifecycle = () => {}
    const onInterrupt = () => {}
    const cfg = manifestConfig(
      { id: 'x', version: '1.0.0', tools: [], variables: [] },
      { onConfigChange, onLifecycle, onInterrupt },
    )
    expect(cfg.onConfigChange).toBe(onConfigChange)
    expect(cfg.onLifecycle).toBe(onLifecycle)
    expect(cfg.onInterrupt).toBe(onInterrupt)
  })
})
