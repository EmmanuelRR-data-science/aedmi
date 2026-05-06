// Feature: market-study-app, Propiedad 6 (HEX)
import { describe, it, expect } from 'vitest'
import * as fc from 'fast-check'
import { isValidHex } from '@/components/style-panel/StylePanel'

const validHex3 = fc
  .tuple(
    fc.constantFrom(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
    fc.constantFrom(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
    fc.constantFrom(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
  )
  .map(([a, b, c]) => {
    const d = (n: number) => '0123456789abcdef'[n] ?? '0'
    return `#${d(a)}${d(b)}${d(c)}`
  })

const validHex6 = fc
  .array(
    fc.constantFrom(
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      'a', 'b', 'c', 'd', 'e', 'f', 'A', 'B', 'C', 'D', 'E', 'F',
    ),
    { minLength: 6, maxLength: 6 },
  )
  .map((chars) => `#${chars.join('')}`)

describe('isValidHex', () => {
  it('acepta formas 3/6 nibble y (100 carreras c/u)', () => {
    fc.assert(
      fc.property(validHex3, (h) => isValidHex(h)),
      { numRuns: 100 },
    )
    fc.assert(
      fc.property(validHex6, (h) => isValidHex(h)),
      { numRuns: 100 },
    )
  })
})
