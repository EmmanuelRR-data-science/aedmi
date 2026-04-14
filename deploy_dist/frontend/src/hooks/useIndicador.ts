import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiFetch } from '@/lib/api'
import type { Indicador, DatoIndicador, Analisis, TabGeografico, CategoriaIndicador } from '@/types'

// ─── Indicadores ──────────────────────────────────────────────────────────────

export function useIndicadores(
  nivel?: TabGeografico,
  categoria?: CategoriaIndicador
) {
  const params = new URLSearchParams()
  if (nivel) params.set('nivel_geografico', nivel)
  if (categoria) params.set('categoria', categoria)
  const query = params.toString() ? `?${params.toString()}` : ''

  return useQuery<Indicador[]>({
    queryKey: ['indicadores', nivel, categoria],
    queryFn: () => apiFetch<Indicador[]>(`/indicadores${query}`),
    staleTime: 10 * 60 * 1000,
    refetchOnWindowFocus: false,
  })
}

interface IndicadorDatosResponse {
  indicador_id: number
  clave: string
  nombre: string
  unidad: string | null
  periodicidad: string
  datos: DatoIndicador[]
}

interface AnalisisPayload {
  entidadClave?: string
  contexto?: Record<string, unknown>
  datosFiltrados?: Record<string, unknown>[]
}

interface IndicadorDatosOptions {
  enabled?: boolean
  entidadPrefix?: string
  limit?: number
  staleTimeMs?: number
}

interface OpcionesGeograficasResponse {
  estados: string[]
  municipios: string[]
  localidades: string[]
}

interface OpcionesGeograficasOptions {
  enabled?: boolean
  estado?: string
  municipio?: string
  q?: string
  staleTimeMs?: number
}

export function useIndicadorDatos(id: number | null, options?: IndicadorDatosOptions) {
  const params = new URLSearchParams()
  if (options?.entidadPrefix) params.set('entidad_prefix', options.entidadPrefix)
  if (typeof options?.limit === 'number') params.set('limit', String(options.limit))
  const query = params.toString() ? `?${params.toString()}` : ''

  return useQuery<IndicadorDatosResponse>({
    queryKey: ['indicador-datos', id, options?.entidadPrefix ?? null, options?.limit ?? null],
    queryFn: () => apiFetch<IndicadorDatosResponse>(`/indicadores/${id}/datos${query}`),
    enabled: id !== null && (options?.enabled ?? true),
    staleTime: options?.staleTimeMs ?? 5 * 60 * 1000,
    refetchOnWindowFocus: false,
  })
}

export function useOpcionesGeograficas(id: number | null, options?: OpcionesGeograficasOptions) {
  const params = new URLSearchParams()
  if (options?.estado) params.set('estado', options.estado)
  if (options?.municipio) params.set('municipio', options.municipio)
  if (options?.q) params.set('q', options.q)
  const query = params.toString() ? `?${params.toString()}` : ''

  return useQuery<OpcionesGeograficasResponse>({
    queryKey: ['opciones-geograficas', id, options?.estado ?? null, options?.municipio ?? null, options?.q ?? null],
    queryFn: () => apiFetch<OpcionesGeograficasResponse>(`/indicadores/${id}/opciones-geograficas${query}`),
    enabled: id !== null && (options?.enabled ?? true),
    staleTime: options?.staleTimeMs ?? 10 * 60 * 1000,
    refetchOnWindowFocus: false,
  })
}

// ─── Análisis ─────────────────────────────────────────────────────────────────

export function useAnalisis(graficaId: number | null, entidadClave?: string) {
  const query = entidadClave
    ? `?entidad_clave=${encodeURIComponent(entidadClave)}`
    : ''
  return useQuery<Analisis | null>({
    queryKey: ['analisis', graficaId, entidadClave ?? null],
    queryFn: () => apiFetch<Analisis | null>(`/analisis/${graficaId}${query}`),
    enabled: graficaId !== null,
    staleTime: 10 * 60 * 1000,
    refetchOnWindowFocus: false,
  })
}

export function useGenerarAnalisisIA(graficaId: number, entidadClave?: string) {
  const queryClient = useQueryClient()
  const query = entidadClave
    ? `?entidad_clave=${encodeURIComponent(entidadClave)}`
    : ''

  return useMutation<Analisis, Error, AnalisisPayload | undefined>({
    mutationFn: (payload?: AnalisisPayload) => {
      const body =
        payload?.datosFiltrados || payload?.contexto
          ? JSON.stringify({
              contexto: payload?.contexto ?? null,
              datos_filtrados: payload?.datosFiltrados ?? null,
            })
          : undefined
      return apiFetch<Analisis>(`/analisis/${graficaId}/ia${query}`, {
        method: 'POST',
        ...(body ? { body } : {}),
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['analisis', graficaId, entidadClave ?? null],
      })
    },
  })
}

export function useGuardarAnalisisRevisado(graficaId: number, entidadClave?: string) {
  const queryClient = useQueryClient()
  const query = entidadClave
    ? `?entidad_clave=${encodeURIComponent(entidadClave)}`
    : ''

  return useMutation<Analisis, Error, string>({
    mutationFn: (texto: string) =>
      apiFetch<Analisis>(`/analisis/${graficaId}/revisado${query}`, {
        method: 'PUT',
        body: JSON.stringify({ texto }),
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ['analisis', graficaId, entidadClave ?? null],
      })
    },
  })
}
