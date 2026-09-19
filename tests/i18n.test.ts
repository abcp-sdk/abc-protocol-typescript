import { describe, expect, it } from 'vitest'
import {
  baseLocale,
  type Catalog,
  defineI18n,
  interpolate,
  translate,
} from '../src/i18n.js'

const catalog = {
  hello: { en: 'Hello {name}', zh: '你好 {name}', ja: 'こんにちは {name}' },
  plain: { en: 'plain', zh: '简单' },
} satisfies Catalog<'hello' | 'plain'>

describe('i18n', () => {
  it('baseLocale strips region/script', () => {
    expect(baseLocale('zh-CN')).toBe('zh')
    expect(baseLocale('zh_Hans')).toBe('zh')
    expect(baseLocale('EN')).toBe('en')
    expect(baseLocale('fr')).toBe('fr')
  })

  it('interpolates known params and leaves unknown literal', () => {
    expect(interpolate('Hi {a} {b}', { a: 1 })).toBe('Hi 1 {b}')
    expect(interpolate('no params')).toBe('no params')
  })

  it('resolves exact, then base language', () => {
    expect(translate(catalog, 'hello', 'zh', { name: 'X' })).toBe('你好 X')
    expect(translate(catalog, 'hello', 'zh-CN', { name: 'X' })).toBe('你好 X')
    expect(translate(catalog, 'hello', 'ja-JP', { name: 'X' })).toBe(
      'こんにちは X',
    )
  })

  it('falls back to en for an unknown locale', () => {
    expect(translate(catalog, 'hello', 'de', { name: 'X' })).toBe('Hello X')
    expect(translate(catalog, 'hello', '', { name: 'X' })).toBe('Hello X')
  })

  it('defineI18n binds a translator', () => {
    const { t } = defineI18n(catalog)
    expect(t('zh', 'plain')).toBe('简单')
    expect(t('en', 'plain')).toBe('plain')
    expect(t('unknown', 'plain')).toBe('plain')
  })
})
