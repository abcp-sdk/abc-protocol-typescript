import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'
import { generateOpenApi } from '../src/openapi.js'

const snapshot = JSON.parse(
  readFileSync(
    fileURLToPath(new URL('./golden/openapi.snapshot.json', import.meta.url)),
    'utf8',
  ),
)

describe('openapi snapshot', () => {
  it('matches the committed golden document (schema drift must be intentional)', () => {
    expect(generateOpenApi()).toEqual(snapshot)
  })
})
