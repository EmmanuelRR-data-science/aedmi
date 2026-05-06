// Feature: market-study-app, Propiedades 1, 7 (paleta)
import { describe, it, expect, beforeEach } from 'vitest'
import * as fc from 'fast-check'
import {
  getColorForIndex,
  useStyleConfig,
  DEFAULT_PALETTE,
} from '@/hooks/useStyleConfig'

const hexaColor = fc
  .array(
    fc.constantFrom(
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      'a', 'b', 'c', 'd', 'e', 'f', 'A', 'B', 'C', 'D', 'E', 'F',
    ),
    { minLength: 6, maxLength: 6 },
  )
  .map((chars) => `#${chars.join('')}`)

describe('getColorForIndex (Prop 1: ciclo por índice)', () => {
  it('100 iteraciones: coherente con módulo de longitud', () => {
    fc.assert(
      fc.property(
        fc.array(hexaColor, { minLength: 1, maxLength: 10 }),
        fc.nat({ max: 1_000 }),
        (palette, idx) => {
          const c = getColorForIndex(palette, idx)
          return c === palette[idx % palette.length]
        },
      ),
      { numRuns: 100 },
    )
  })
})

describe('resetPalette (Prop 7: idempotente)', () => {
  beforeEach(() => {
    useStyleConfig.setState({
      palette: ['#000000', '#ffffff'],
      fontFamily: 'ballingermono-light',
      titleSize: 14,
      xAxisSize: 11,
      yAxisSize: 11,
    })
  })

  it('dos resets seguidas dejan la paleta en DEFAULT', () => {
    useStyleConfig.getState().resetPalette()
    useStyleConfig.getState().resetPalette()
    expect(useStyleConfig.getState().palette).toEqual([...DEFAULT_PALETTE])
  })
})
