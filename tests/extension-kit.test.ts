import { describe, expect, it } from 'vitest'
import { boolArg, numArg, strArg, strArray } from '../src/extension-kit.js'

describe('extension-kit argument coercion', () => {
  it('strArg: string passes through, everything else reads empty', () => {
    const args: Record<string, unknown> = {
      a: 'x',
      b: 1,
      c: null,
      d: undefined,
      e: true,
      f: ['x'],
    }
    expect(strArg(args, 'a')).toBe('x')
    expect(strArg(args, 'b')).toBe('')
    expect(strArg(args, 'c')).toBe('')
    expect(strArg(args, 'd')).toBe('')
    expect(strArg(args, 'e')).toBe('')
    expect(strArg(args, 'f')).toBe('')
    expect(strArg(args, 'missing')).toBe('')
  })

  it('numArg: finite numbers only', () => {
    const args: Record<string, unknown> = {
      a: 3,
      b: -1.5,
      c: Number.NaN,
      d: Number.POSITIVE_INFINITY,
      e: '3',
      f: null,
    }
    expect(numArg(args, 'a')).toBe(3)
    expect(numArg(args, 'b')).toBe(-1.5)
    expect(numArg(args, 'c')).toBeUndefined()
    expect(numArg(args, 'd')).toBeUndefined()
    expect(numArg(args, 'e')).toBeUndefined()
    expect(numArg(args, 'f')).toBeUndefined()
    expect(numArg(args, 'missing')).toBeUndefined()
  })

  it('boolArg: booleans only', () => {
    const args: Record<string, unknown> = {
      a: true,
      b: false,
      c: 1,
      d: 'true',
      e: null,
    }
    expect(boolArg(args, 'a')).toBe(true)
    expect(boolArg(args, 'b')).toBe(false)
    expect(boolArg(args, 'c')).toBeUndefined()
    expect(boolArg(args, 'd')).toBeUndefined()
    expect(boolArg(args, 'e')).toBeUndefined()
    expect(boolArg(args, 'missing')).toBeUndefined()
  })

  it('strArray: drops non-string members, tolerates non-arrays', () => {
    const args: Record<string, unknown> = {
      a: ['x', 1, 'y', null, true, 'z'],
      b: 'not-an-array',
      c: null,
    }
    expect(strArray(args, 'a')).toEqual(['x', 'y', 'z'])
    expect(strArray(args, 'b')).toEqual([])
    expect(strArray(args, 'c')).toEqual([])
    expect(strArray(args, 'missing')).toEqual([])
  })
})
