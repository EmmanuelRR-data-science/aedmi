import type { DatoIndicador } from '@/types'

/** Serie exacta para el payload de Gamma / cola (mismo shape que DatoSeriePresentacion). */
export function mapDatosForGammaExport(datos: DatoIndicador[]) {
  return datos.map((d) => ({
    periodo: d.periodo,
    valor: d.valor,
    entidad_clave: d.entidad_clave,
    unidad: d.unidad,
  }))
}
