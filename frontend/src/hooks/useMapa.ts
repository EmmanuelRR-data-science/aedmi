import { useMutation, useQuery } from '@tanstack/react-query'

import { apiFetch } from '@/lib/api'

export interface MapaSugerencia {
  id: string
  label: string
  tipo: 'municipio' | 'localidad'
  lat: number
  lng: number
  entidad: string
  cve_ent: string
  cve_mun: string
}

export interface MapaCapa {
  id: string
  nombre: string
  categoria: string
  source_type: 'official' | 'google_places' | 'hybrid'
  disponible: boolean
}

export interface MapaCiudadObjetivo {
  id: string
  nombre: string
  entidad: string
  cve_ent: string
  cve_mun: string
  lat: number
  lng: number
}

export interface MapaAlcanceResponse {
  scope_mode: 'limited_7_cities'
  en_alcance: boolean
  mensaje: string
  ciudad_actual_id: string | null
  ciudades_objetivo: MapaCiudadObjetivo[]
}

export interface MapaCapaFeature {
  id: string
  geometry_type: 'Point' | 'LineString' | 'Polygon' | 'MultiLineString' | 'MultiPolygon'
  coordinates: any[]
  properties: Record<string, string | number | boolean | null>
}

export interface MapaCapaDatos {
  capa_id: string
  nombre: string
  source_type: 'official' | 'google_places' | 'hybrid'
  source_name: string
  disponibilidad: 'ok' | 'parcial' | 'sin_datos'
  features: MapaCapaFeature[]
}

export interface MapaIndicador {
  clave: string
  bloque: 'estado_propiedad' | 'proximidad' | 'mapa_accesos'
  valor: number | null
  unidad: string
  estado: 'ok' | 'degradado' | 'sin_datos'
  source_type: 'official' | 'google_places' | 'hybrid'
  source_name: string
  updated_at: string
}

export interface MapaPoiTop {
  id: string
  nombre: string
  categoria: 'hotelero' | 'comercial' | 'salud' | 'educacion' | 'transporte' | 'turistico'
  lat: number
  lng: number
  distancia_m: number
  source_type: 'official' | 'google_places' | 'hybrid'
  source_name: string
  updated_at: string
}

export interface MapaPuebloMagico {
  id: string
  nombre: string
  entidad: string
  lat: number
  lng: number
  distancia_km: number
  tiempo_estimado_min: number | null
  fuente: string
  fecha_referencia: string
}

export interface MapaQueryResponse {
  runtime_mode: 'real_time_first' | 'official_only'
  ubicacion: {
    lat: number
    lng: number
    entidad: string
    radio_m: number
  }
  capas: {
    overlays_activos: string[]
    capas_disponibles: MapaCapa[]
  }
  indicadores: MapaIndicador[]
  top_puntos_interes: MapaPoiTop[]
  pueblos_magicos_cercanos: MapaPuebloMagico[]
  degradacion: {
    activa: boolean
    motivo: string | null
    fuentes_afectadas: string[]
    fallback_age_seconds: number
  }
}

export interface MapaAgebResponse {
  cvegeo: string
  cve_ent: string
  cve_mun: string
  cve_loc: string
  cve_ageb: string
  ambito: 'U' | 'R'
  fuente: string
  fecha_corte: string
}

export interface MapaAnalisisIAResponse {
  analisis_ia: string
  generated_at: string
}

export function useMapaSugerencias(q: string, enabled = true) {
  const params = new URLSearchParams()
  if (q.trim()) params.set('q', q.trim())
  const query = params.toString() ? `?${params.toString()}` : ''

  return useQuery<MapaSugerencia[]>({
    queryKey: ['mapa-sugerencias', q],
    queryFn: () => apiFetch<MapaSugerencia[]>(`/mapa/sugerencias${query}`),
    enabled,
    staleTime: 2 * 60 * 1000,
    refetchOnWindowFocus: false,
  })
}

export function useMapaTopPoi(args: {
  lat: number
  lng: number
  radio_m: number
  source_mode: 'real_time_first' | 'official_only'
  capas: string[]
  categorias?: string[]
  limit?: number
  enabled: boolean
}) {
  const params = new URLSearchParams({
    lat: String(args.lat),
    lng: String(args.lng),
    radio_m: String(args.radio_m),
    source_mode: args.source_mode,
    limit: String(args.limit ?? 10),
  })
  if (args.capas.length) params.set('capas', args.capas.join(','))
  if (args.categorias?.length) params.set('categorias', args.categorias.join(','))

  return useQuery<MapaPoiTop[]>({
    queryKey: [
      'mapa-top-poi',
      args.lat,
      args.lng,
      args.radio_m,
      args.source_mode,
      args.capas.join(','),
      args.categorias?.join(',') ?? '',
      args.limit ?? 10,
    ],
    queryFn: () => apiFetch<MapaPoiTop[]>(`/mapa/poi/top?${params.toString()}`),
    enabled: args.enabled,
    staleTime: 30 * 1000,
    refetchOnWindowFocus: false,
  })
}

export function useMapaPueblosMagicos(args: {
  lat: number
  lng: number
  enabled: boolean
  limit?: number
  radio_max_km?: number
}) {
  const params = new URLSearchParams({
    lat: String(args.lat),
    lng: String(args.lng),
    limit: String(args.limit ?? 5),
    radio_max_km: String(args.radio_max_km ?? 300),
  })
  return useQuery<MapaPuebloMagico[]>({
    queryKey: [
      'mapa-pueblos-magicos',
      args.lat,
      args.lng,
      args.limit ?? 5,
      args.radio_max_km ?? 300,
    ],
    queryFn: () =>
      apiFetch<MapaPuebloMagico[]>(`/mapa/pueblos-magicos/cercanos?${params.toString()}`),
    enabled: args.enabled,
    staleTime: 2 * 60 * 1000,
    refetchOnWindowFocus: false,
  })
}

export function useMapaCapas() {
  return useQuery<MapaCapa[]>({
    queryKey: ['mapa-capas'],
    queryFn: () => apiFetch<MapaCapa[]>('/mapa/capas'),
    staleTime: 5 * 60 * 1000,
    refetchOnWindowFocus: false,
  })
}

export function useMapaAlcance(args: { cve_ent?: string; cve_mun?: string; enabled: boolean }) {
  const params = new URLSearchParams()
  if (args.cve_ent) params.set('cve_ent', args.cve_ent)
  if (args.cve_mun) params.set('cve_mun', args.cve_mun)
  const query = params.toString() ? `?${params.toString()}` : ''
  return useQuery<MapaAlcanceResponse>({
    queryKey: ['mapa-alcance', args.cve_ent ?? '', args.cve_mun ?? ''],
    queryFn: () => apiFetch<MapaAlcanceResponse>(`/mapa/alcance${query}`),
    enabled: args.enabled,
    staleTime: 5 * 60 * 1000,
    refetchOnWindowFocus: false,
  })
}

export function useMapaCapasDatos(args: {
  lat: number
  lng: number
  radio_m: number
  cve_ent?: string
  cve_mun?: string
  capas: string[]
  enabled: boolean
}) {
  const params = new URLSearchParams({
    lat: String(args.lat),
    lng: String(args.lng),
    radio_m: String(args.radio_m),
    capas: args.capas.join(','),
  })
  if (args.cve_ent) params.set('cve_ent', args.cve_ent)
  if (args.cve_mun) params.set('cve_mun', args.cve_mun)
  return useQuery<MapaCapaDatos[]>({
    queryKey: [
      'mapa-capas-datos',
      args.lat,
      args.lng,
      args.radio_m,
      args.cve_ent ?? '',
      args.cve_mun ?? '',
      args.capas.join(','),
    ],
    queryFn: () => apiFetch<MapaCapaDatos[]>(`/mapa/capas/datos?${params.toString()}`),
    enabled: args.enabled && args.capas.length > 0,
    staleTime: 30 * 1000,
    refetchOnWindowFocus: false,
  })
}

export function useMapaQuery(args: {
  lat: number
  lng: number
  radio_m: number
  source_mode: 'real_time_first' | 'official_only'
  capas: string[]
  enabled: boolean
}) {
  return useQuery<MapaQueryResponse>({
    queryKey: [
      'mapa-query',
      args.lat,
      args.lng,
      args.radio_m,
      args.source_mode,
      args.capas.join(','),
    ],
    queryFn: () =>
      apiFetch<MapaQueryResponse>('/mapa/query', {
        method: 'POST',
        body: JSON.stringify({
          lat: args.lat,
          lng: args.lng,
          radio_m: args.radio_m,
          source_mode: args.source_mode,
          capas: args.capas,
        }),
      }),
    enabled: args.enabled,
    staleTime: 30 * 1000,
    refetchOnWindowFocus: false,
  })
}

export function useMapaAgeb(
  lat: number,
  lng: number,
  enabled: boolean,
  cveEnt?: string,
  cveMun?: string
) {
  const params = new URLSearchParams({
    lat: String(lat),
    lng: String(lng),
  })
  if (cveEnt) params.set('cve_ent', cveEnt)
  if (cveMun) params.set('cve_mun', cveMun)
  return useQuery<MapaAgebResponse>({
    queryKey: ['mapa-ageb', lat, lng, cveEnt ?? null, cveMun ?? null],
    queryFn: () => apiFetch<MapaAgebResponse>(`/mapa/ageb?${params.toString()}`),
    enabled,
    staleTime: 10 * 60 * 1000,
    refetchOnWindowFocus: false,
  })
}

export function useMapaAnalisisIA() {
  return useMutation<
    MapaAnalisisIAResponse,
    Error,
    { contexto?: Record<string, unknown>; datos_filtrados?: Record<string, unknown>[] }
  >({
    mutationFn: (payload) =>
      apiFetch<MapaAnalisisIAResponse>('/mapa/analisis-ia', {
        method: 'POST',
        body: JSON.stringify({
          contexto: payload.contexto ?? {},
          datos_filtrados: payload.datos_filtrados ?? [],
        }),
      }),
  })
}
