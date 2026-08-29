// KV key-segment escaping.
//
// NATS KV keys accept only [a-zA-Z0-9 -/_=] ('.' is the hierarchy
// separator); '%' and ':' are invalid. escapeKVSegment emits only the safe
// set using '=' as an escape char plus two uppercase hex digits, so a raw
// session name with dots/slashes/colons round-trips without ever emitting
// the '.' separator.

export function escapeKVSegment(s: string): string {
  let out = ''
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i)
    if (
      (c >= 48 && c <= 57) ||
      (c >= 65 && c <= 90) ||
      (c >= 97 && c <= 122) ||
      c === 45 ||
      c === 95 ||
      c === 47
    ) {
      out += s[i]
    } else {
      out += '=' + c.toString(16).toUpperCase().padStart(2, '0')
    }
  }
  return out
}

export function unescapeKVSegment(s: string): string {
  let out = ''
  for (let i = 0; i < s.length; i++) {
    if (s[i] === '=' && i + 2 < s.length) {
      const hex = s.slice(i + 1, i + 3)
      if (/^[0-9A-F]{2}$/i.test(hex)) {
        out += String.fromCharCode(Number.parseInt(hex, 16))
        i += 2
        continue
      }
    }
    out += s[i]
  }
  return out
}
