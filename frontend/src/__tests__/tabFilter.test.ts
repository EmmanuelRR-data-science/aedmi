// Feature: market-study-app, Propiedad 8 (filtrar por tab geográfico)
import { describe, it } from 'vitest'
import * as fc from 'fast-check'
import type { Indicador } from '@/types'

const niveles: Indicador['nivel_geografico'][] = [
  'nacional', 'estatal', 'municipal', 'localidad', 'ciudad', 'mapa',
]

function filtrarPorNivelGraf(
  inds: Pick<Indicador, 'nivel_geografico'>[],
  tab: Indicador['nivel_geografico'],
) {
  return inds.filter((i) => i.nivel_geografico === tab)
}

function indicBase(n: Indicador['nivel_geografico']): Indicador {
  return {
    id: 1,
    clave: 'c',
    nombre: 'N',
    categoria: 'demografia',
    nivel_geografico: n,
    unidad: 'u',
    descripcion: null,
    activo: true,
    fuente_id: 1,
  }
}

describe('filtrarPorNivelGraf (Prop. 8)', () => {
  it('todos resultados comparten el tab (100 carreras)', () => {
    const nivelArb = fc.constantFrom<Indicador['nivel_geografico']>(...niveles)
    fc.assert(
      fc.property(
        fc.array(
          fc.record({ nivel: nivelArb, id: fc.nat() }),
          { minLength: 0, maxLength: 25 },
        ),
        nivelArb,
        (rows, tab) => {
          const lista: Indicador[] = rows.map((r, i) => ({
            ...indicBase(r.nivel),
            id: (r as { id: number }).id ?? i + 1,
          }))

          const f = filtrarPorNivelGraf(lista, tab)
          return f.length === 0
            ? true
            : f.every((g) => g.nivel_geografico === tab)
        },
      ),
      { numRuns: 100 },
    )
  })
})
