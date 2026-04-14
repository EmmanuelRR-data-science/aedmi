// ─── Geographic levels ────────────────────────────────────────────────────────

export type TabGeografico = 'nacional' | 'estatal' | 'ciudad' | 'municipal' | 'localidad' | 'mapa'

// ─── Indicator domain ─────────────────────────────────────────────────────────

export type CategoriaIndicador = 'demografia' | 'economia' | 'turismo' | 'conectividad_aerea'

export interface Indicador {
  id: number
  clave: string
  nombre: string
  categoria: CategoriaIndicador
  nivel_geografico: TabGeografico
  unidad: string | null
  fuente_id: number | null
  descripcion: string | null
  activo: boolean
}

export interface DatoIndicador {
  id: number
  indicador_id: number
  nivel_geografico: TabGeografico
  entidad_clave: string | null
  valor: number
  unidad: string | null
  /** Year (anual/quinquenal) or ISO date string (mensual/diario) */
  periodo: string | number
  cargado_at: string
}

// ─── Analysis ─────────────────────────────────────────────────────────────────

export interface Analisis {
  id: number
  indicador_id: number
  nivel_geografico: TabGeografico
  entidad_clave: string | null
  analisis_ia: string | null
  analisis_revisado: string | null
  ia_generado_at: string | null
  revisado_at: string | null
  updated_at: string
}

// ─── Style configuration (Zustand store) ─────────────────────────────────────

export interface StyleConfig {
  palette: string[]
  fontFamily: string
  titleSize: number
  xAxisSize: number
  yAxisSize: number
}

// ─── Chart data helpers ───────────────────────────────────────────────────────

export interface ChartDataPoint {
  name: string
  value: number
  [key: string]: string | number
}
