import { describe, expect, it } from 'vitest'
import { baseLocale, type Catalog, interpolate, translate } from '../src/i18n.js'
import { GOLDEN } from './golden.js'

/**
 * Cross-SDK i18n golden vectors. The Go SDK asserts the identical values
 * (abc-protocol-go/conformance/golden_test.go) so the two i18n cores cannot
 * drift on locale resolution / interpolation.
 */
const catalog = GOLDEN.i18n.catalog as unknown as Catalog<string>

describe('i18n golden vectors (TS/Go lockstep)', () => {
  it('resolves exact, base, fallback and unknown locales', () => {
    expect(translate(catalog, 'hello', 'zh', { name: 'X' })).toBe(GOLDEN.i18n.zh)
    expect(translate(catalog, 'hello', 'zh-CN', { name: 'X' })).toBe(GOLDEN.i18n.zhCN)
    expect(translate(catalog, 'hello', 'zh_Hans', { name: 'X' })).toBe(GOLDEN.i18n.zhHans)
    expect(translate(catalog, 'hello', 'ja-JP', { name: 'X' })).toBe(GOLDEN.i18n.jaJP)
    expect(translate(catalog, 'hello', 'de', { name: 'X' })).toBe(GOLDEN.i18n.de)
    expect(translate(catalog, 'hello', '', { name: 'X' })).toBe(GOLDEN.i18n.empty)
    expect(translate(catalog, 'plain', 'zh')).toBe(GOLDEN.i18n.plainZh)
    expect(translate(catalog, 'plain', 'unknown')).toBe(GOLDEN.i18n.plainUnknown)
  })

  it('baseLocale + interpolate match', () => {
    expect(baseLocale('zh-Hans')).toBe(GOLDEN.i18n.baseLangZhHans)
    expect(interpolate('Hi {a} {b}', { a: '1' })).toBe(GOLDEN.i18n.interp)
  })
})
