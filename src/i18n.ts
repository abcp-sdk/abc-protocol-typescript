/**
 * Typed i18n for extension-authored strings that reach the model context.
 *
 * Tool DESCRIPTIONS are localized through the manifest (`description` +
 * `descriptions[locale]`). This module covers the OTHER half: the runtime
 * `content` and error text a tool returns, which is fed verbatim to the model.
 * A Chinese session should not get English tool results.
 *
 * Design:
 *   - A catalog is `{ key: { locale: template } }`. The locale set is an OPEN
 *     map (any BCP-47-ish tag), so adding a language is pure data — no code.
 *   - Keys are strongly typed (`Catalog<keyof typeof CATALOG>`), so a typo in a
 *     `t('...')` call is a compile error, and every catalog entry is checked to
 *     carry every declared locale.
 *   - Templates interpolate `{name}` placeholders.
 *   - Resolution is total: requested locale -> its base language -> fallback
 *     locale (default `en`) -> its base -> the key itself. A message is never
 *     empty, even for an untranslated locale.
 */

/** Keys a message may use for `{placeholder}` interpolation. */
export type MessageParams = Record<string, string | number>

/**
 * A typed catalog. `Keys` is the union of message ids; each id maps a locale
 * tag to its template. Use `satisfies Catalog<...>` (or the `defineCatalog`
 * helper) so a missing locale or a mistyped key fails to compile.
 */
export type Catalog<Keys extends string> = Record<
  Keys,
  Record<string, string>
>

/** The locales every catalog MUST provide (the fallback chain termination). */
export const BASE_LOCALE = 'en'

/** Lowercase + strip a region subtag: `zh-CN` / `zh_Hans` -> `zh`. */
export function baseLocale(locale: string): string {
  const l = locale.trim().toLowerCase().replace(/_/g, '-')
  const dash = l.indexOf('-')
  return dash === -1 ? l : l.slice(0, dash)
}

/** Interpolate `{name}` placeholders (unknown names are left literal). */
export function interpolate(template: string, params?: MessageParams): string {
  if (params === undefined) return template
  return template.replace(/\{([a-zA-Z0-9_]+)\}/g, (full, name: string) => {
    const v = params[name]
    return v === undefined ? full : String(v)
  })
}

/**
 * Resolve one message. `locale` may be a full tag; matching walks:
 * exact -> base(locale) -> fallbackLocale -> base(fallback) -> key.
 */
export function translate<Keys extends string>(
  catalog: Catalog<Keys>,
  key: Keys,
  locale: string,
  params?: MessageParams,
  fallbackLocale: string = BASE_LOCALE,
): string {
  const entry = catalog[key]
  if (entry === undefined) return key
  const candidates = [
    locale,
    baseLocale(locale),
    fallbackLocale,
    baseLocale(fallbackLocale),
  ]
  for (const c of candidates) {
    const hit = entry[c]
    if (hit !== undefined) return interpolate(hit, params)
  }
  // Last resort: first available translation, else the key.
  const first = Object.values(entry)[0]
  return first !== undefined ? interpolate(first, params) : key
}

/**
 * Build a per-extension translator bound to one catalog. Returns a `t` that
 * takes `(locale, key, params?)`, plus `catalog` for introspection.
 *
 * ```ts
 * const { t } = defineI18n({
 *   wroteBytes: { en: "Wrote {n} bytes to '{path}'.", zh: "已向 '{path}' 写入 {n} 字节。" },
 * })
 * t(locale, 'wroteBytes', { n: 12, path: 'a.txt' })
 * ```
 */
export function defineI18n<Keys extends string>(
  catalog: Catalog<Keys>,
  fallbackLocale: string = BASE_LOCALE,
): {
  t: (locale: string, key: Keys, params?: MessageParams) => string
  catalog: Catalog<Keys>
} {
  return {
    catalog,
    t: (locale, key, params) =>
      translate(catalog, key, locale, params, fallbackLocale),
  }
}
