'use client'

import { useEffect, useMemo, useState } from 'react'

import {
  useMapaAgeb,
  useMapaCapas,
  useMapaCapasDatos,
  useMapaPueblosMagicos,
  useMapaQuery,
  useMapaSugerencias,
  useMapaTopPoi,
} from '@/hooks/useMapa'

const RADIOS = [1000, 3000, 5000, 10000]
const TOP_POI_LIMITS = [10, 20, 30]
const PUEBLOS_LIMITS = [5, 10, 15]
const PUEBLOS_RADIOS_KM = [150, 300, 500]
const FILTROS_SESSION_KEY = 'mapa-filtros-v1'
const POI_CATEGORIAS = [
  { id: 'hotelero', label: 'Hotelero' },
  { id: 'comercial', label: 'Comercial' },
  { id: 'salud', label: 'Salud' },
  { id: 'educacion', label: 'Educación' },
  { id: 'transporte', label: 'Transporte' },
  { id: 'turistico', label: 'Turístico' },
] as const

function formatNumber(value: number | null): string {
  if (value === null || Number.isNaN(value)) return 'N/D'
  return new Intl.NumberFormat('es-MX', { maximumFractionDigits: 2 }).format(value)
}

function getBloqueLabel(bloque: string): string {
  if (bloque === 'estado_propiedad') return 'Estado de la propiedad'
  if (bloque === 'proximidad') return 'Proximidad'
  if (bloque === 'mapa_accesos') return 'Mapa y accesos'
  return bloque
}

function getModoLabel(modo: 'real_time_first' | 'official_only'): string {
  return modo === 'real_time_first' ? 'Tiempo real (prioritario)' : 'Solo fuentes oficiales'
}

function getTipoLabel(tipo: 'municipio' | 'localidad'): string {
  return tipo === 'municipio' ? 'Municipio' : 'Localidad'
}

function getPoiCategoriaLabel(categoria: string): string {
  const labels: Record<string, string> = {
    hotelero: 'Hotelero',
    comercial: 'Comercial',
    salud: 'Salud',
    educacion: 'Educación',
    transporte: 'Transporte',
    turistico: 'Turístico',
  }
  return labels[categoria] ?? categoria
}

function getSourceTypeLabel(sourceType: string): string {
  if (sourceType === 'official') return 'Oficial'
  if (sourceType === 'hybrid') return 'Híbrida'
  if (sourceType === 'google_places') return 'Google Places'
  return sourceType
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max)
}

function getMapViewport(lat: number, lng: number, radioMetros: number): {
  url: string
  left: number
  right: number
  top: number
  bottom: number
} {
  const deltaLat = radioMetros / 111_320
  const safeCos = Math.max(Math.cos((lat * Math.PI) / 180), 0.1)
  const deltaLng = radioMetros / (111_320 * safeCos)

  const left = clamp(lng - deltaLng, -180, 180)
  const right = clamp(lng + deltaLng, -180, 180)
  const bottom = clamp(lat - deltaLat, -85, 85)
  const top = clamp(lat + deltaLat, -85, 85)

  const url =
    'https://www.openstreetmap.org/export/embed.html' +
    `?bbox=${left}%2C${bottom}%2C${right}%2C${top}` +
    `&layer=mapnik&marker=${lat}%2C${lng}`

  return { url, left, right, top, bottom }
}

export default function MapaInteractivoPanel() {
  const [search, setSearch] = useState('')
  const [selectedLat, setSelectedLat] = useState(20.5888)
  const [selectedLng, setSelectedLng] = useState(-100.3899)
  const [selectedEntidad, setSelectedEntidad] = useState('Querétaro, Querétaro')
  const [selectedCveEnt, setSelectedCveEnt] = useState('22')
  const [selectedCveMun, setSelectedCveMun] = useState('014')
  const [radio, setRadio] = useState(3000)
  const [sourceMode, setSourceMode] = useState<'real_time_first' | 'official_only'>(
    'real_time_first'
  )
  const [selectedCapas, setSelectedCapas] = useState<string[]>([])
  const [poiLimit, setPoiLimit] = useState(10)
  const [pueblosLimit, setPueblosLimit] = useState(5)
  const [pueblosRadioKm, setPueblosRadioKm] = useState(300)
  const [poiCategorias, setPoiCategorias] = useState<string[]>([])

  // Persistir filtros en sesión del navegador para mantener contexto al navegar.
  useEffect(() => {
    if (typeof window === 'undefined') return
    try {
      const raw = window.sessionStorage.getItem(FILTROS_SESSION_KEY)
      if (!raw) return
      const parsed = JSON.parse(raw) as {
        poiLimit?: number
        pueblosLimit?: number
        pueblosRadioKm?: number
        poiCategorias?: string[]
      }
      if (parsed.poiLimit && TOP_POI_LIMITS.includes(parsed.poiLimit)) setPoiLimit(parsed.poiLimit)
      if (parsed.pueblosLimit && PUEBLOS_LIMITS.includes(parsed.pueblosLimit))
        setPueblosLimit(parsed.pueblosLimit)
      if (parsed.pueblosRadioKm && PUEBLOS_RADIOS_KM.includes(parsed.pueblosRadioKm))
        setPueblosRadioKm(parsed.pueblosRadioKm)
      if (Array.isArray(parsed.poiCategorias)) setPoiCategorias(parsed.poiCategorias)
    } catch {
      // noop
    }
  }, [])

  const sugerenciasQuery = useMapaSugerencias(search, true)
  const capasQuery = useMapaCapas()

  const capasDisponibles = capasQuery.data ?? []
  const capasActivas = useMemo(() => {
    if (selectedCapas.length > 0) return selectedCapas
    return capasDisponibles.slice(0, 4).map((c) => c.id)
  }, [capasDisponibles, selectedCapas])

  const hasLocation = Number.isFinite(selectedLat) && Number.isFinite(selectedLng)
  const query = useMapaQuery({
    lat: selectedLat,
    lng: selectedLng,
    radio_m: radio,
    source_mode: sourceMode,
    capas: capasActivas,
    enabled: hasLocation,
  })
  const agebQuery = useMapaAgeb(
    selectedLat,
    selectedLng,
    hasLocation,
    selectedCveEnt,
    selectedCveMun
  )
  const topPoiQuery = useMapaTopPoi({
    lat: selectedLat,
    lng: selectedLng,
    radio_m: radio,
    source_mode: sourceMode,
    capas: capasActivas,
    categorias: poiCategorias,
    limit: poiLimit,
    enabled: hasLocation,
  })
  const pueblosMagicosQuery = useMapaPueblosMagicos({
    lat: selectedLat,
    lng: selectedLng,
    enabled: hasLocation,
    limit: pueblosLimit,
    radio_max_km: pueblosRadioKm,
  })
  const capasDatosQuery = useMapaCapasDatos({
    lat: selectedLat,
    lng: selectedLng,
    radio_m: radio,
    cve_ent: selectedCveEnt,
    cve_mun: selectedCveMun,
    capas: capasActivas,
    enabled: hasLocation && capasActivas.length > 0,
  })

  const onPickSuggestion = (index: number) => {
    const item = sugerenciasQuery.data?.[index]
    if (!item) return
    setSelectedLat(item.lat)
    setSelectedLng(item.lng)
    setSelectedEntidad(item.entidad)
    setSelectedCveEnt(item.cve_ent)
    setSelectedCveMun(item.cve_mun)
    setSearch(item.label)
  }

  const toggleCapa = (id: string) => {
    setSelectedCapas((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]
    )
  }

  const togglePoiCategoria = (categoria: string) => {
    setPoiCategorias((prev) =>
      prev.includes(categoria) ? prev.filter((item) => item !== categoria) : [...prev, categoria]
    )
  }

  const indicadores = query.data?.indicadores ?? []
  const topPoi = topPoiQuery.data ?? query.data?.top_puntos_interes ?? []
  const pueblosMagicos = pueblosMagicosQuery.data ?? query.data?.pueblos_magicos_cercanos ?? []
  const mapViewport = useMemo(
    () => getMapViewport(selectedLat, selectedLng, Math.max(radio, 500)),
    [selectedLat, selectedLng, radio]
  )
  const mapaEmbedUrl = mapViewport.url

  useEffect(() => {
    if (typeof window === 'undefined') return
    window.sessionStorage.setItem(
      FILTROS_SESSION_KEY,
      JSON.stringify({
        poiLimit,
        pueblosLimit,
        pueblosRadioKm,
        poiCategorias,
      })
    )
  }, [poiLimit, pueblosLimit, pueblosRadioKm, poiCategorias])

  const overlayData = useMemo(() => {
    const capaColor: Record<string, string> = {
      red_vial: '#0576F3',
      ageb_urbana: '#8b5cf6',
      ageb_rural: '#f59e0b',
      nodos_transporte: '#14b8a6',
      poi_hoteles: '#0ea5e9',
      poi_comercio: '#f97316',
    }
    const points: Array<{ key: string; x: number; y: number; color: string; title: string }> = []
    const lines: Array<{ key: string; d: string; color: string; title: string }> = []
    const polygons: Array<{ key: string; d: string; color: string; title: string }> = []
    const lngSpan = Math.max(mapViewport.right - mapViewport.left, 1e-8)
    const latSpan = Math.max(mapViewport.top - mapViewport.bottom, 1e-8)
    const project = (lat: number, lng: number) => ({
      x: clamp(((lng - mapViewport.left) / lngSpan) * 100, 0, 100),
      y: clamp(((mapViewport.top - lat) / latSpan) * 100, 0, 100),
    })
    const mkPath = (coords: number[][]) =>
      coords
        .map((coord, idx) => {
          const lng = Number(coord[0])
          const lat = Number(coord[1])
          const p = project(lat, lng)
          return `${idx === 0 ? 'M' : 'L'} ${p.x} ${p.y}`
        })
        .join(' ')

    const capasDatos = capasDatosQuery.data ?? []
    capasDatos.forEach((capa) => {
      const color = capaColor[capa.capa_id] ?? '#94a3b8'
      capa.features.forEach((feature) => {
        if (feature.geometry_type === 'Point') {
          const [lng, lat] = feature.coordinates as [number, number]
          const p = project(lat, lng)
          points.push({
            key: `${capa.capa_id}-${feature.id}`,
            x: p.x,
            y: p.y,
            color,
            title: `${capa.nombre}`,
          })
          return
        }
        if (feature.geometry_type === 'LineString') {
          const coords = feature.coordinates as number[][]
          if (coords.length >= 2) {
            lines.push({
              key: `${capa.capa_id}-${feature.id}`,
              d: mkPath(coords),
              color,
              title: `${capa.nombre}`,
            })
          }
          return
        }
        if (feature.geometry_type === 'MultiLineString') {
          const multi = feature.coordinates as number[][][]
          multi.forEach((coords, idx) => {
            if (coords.length >= 2) {
              lines.push({
                key: `${capa.capa_id}-${feature.id}-${idx}`,
                d: mkPath(coords),
                color,
                title: `${capa.nombre}`,
              })
            }
          })
          return
        }
        if (feature.geometry_type === 'Polygon') {
          const rings = feature.coordinates as number[][][]
          const outer = rings[0]
          if (outer && outer.length >= 3) {
            polygons.push({
              key: `${capa.capa_id}-${feature.id}`,
              d: `${mkPath(outer)} Z`,
              color,
              title: `${capa.nombre}`,
            })
          }
          return
        }
        if (feature.geometry_type === 'MultiPolygon') {
          const multi = feature.coordinates as number[][][][]
          multi.forEach((poly, idx) => {
            const outer = poly[0]
            if (outer && outer.length >= 3) {
              polygons.push({
                key: `${capa.capa_id}-${feature.id}-${idx}`,
                d: `${mkPath(outer)} Z`,
                color,
                title: `${capa.nombre}`,
              })
            }
          })
        }
      })
    })

    // Mantener POI y pueblos mágicos visibles como apoyo visual.
    topPoi.forEach((poi) => {
      const p = project(poi.lat, poi.lng)
      points.push({
        key: `poi-${poi.id}`,
        x: p.x,
        y: p.y,
        color: '#22c55e',
        title: `${poi.nombre} (${getPoiCategoriaLabel(poi.categoria)})`,
      })
    })
    pueblosMagicos.forEach((pueblo) => {
      const p = project(pueblo.lat, pueblo.lng)
      points.push({
        key: `pm-${pueblo.id}`,
        x: p.x,
        y: p.y,
        color: '#a855f7',
        title: `Pueblo mágico: ${pueblo.nombre}`,
      })
    })

    return { points, lines, polygons }
  }, [capasDatosQuery.data, mapViewport, pueblosMagicos, topPoi])

  const clearFiltros = () => {
    setPoiCategorias([])
    setPoiLimit(10)
    setPueblosLimit(5)
    setPueblosRadioKm(300)
  }

  return (
    <section
      style={{
        border: '1px solid #2d3148',
        borderRadius: 10,
        background: '#131722',
        padding: 16,
        display: 'flex',
        flexDirection: 'column',
        gap: 16,
      }}
    >
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h2 style={{ margin: 0, color: '#e2e8f0', fontSize: 18, fontWeight: 500 }}>
          Sección Mapa Interactivo
        </h2>
        <span style={{ fontSize: 12, color: '#94a3b8' }}>
          Estado: {query.data?.degradacion.activa ? 'Fallback activo' : 'Tiempo real'} ·{' '}
          {getModoLabel(sourceMode)}
        </span>
      </header>

      <div
        style={{
          display: 'grid',
          gap: 10,
          gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
        }}
      >
        <label style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <span style={{ color: '#94a3b8', fontSize: 12 }}>Buscar ubicación</span>
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Ciudad o municipio (ej. Mérida, León, Puebla)"
            style={{
              background: '#0f1117',
              color: '#e2e8f0',
              border: '1px solid #2d3148',
              borderRadius: 6,
              padding: '8px 10px',
            }}
          />
        </label>

        <label style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <span style={{ color: '#94a3b8', fontSize: 12 }}>Radio (m)</span>
          <select
            value={radio}
            onChange={(e) => setRadio(Number(e.target.value))}
            style={{
              background: '#0f1117',
              color: '#e2e8f0',
              border: '1px solid #2d3148',
              borderRadius: 6,
              padding: '8px 10px',
            }}
          >
            {RADIOS.map((r) => (
              <option key={r} value={r}>
                {r.toLocaleString('es-MX')}
              </option>
            ))}
          </select>
        </label>

        <label style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <span style={{ color: '#94a3b8', fontSize: 12 }}>Modo operativo</span>
          <select
            value={sourceMode}
            onChange={(e) =>
              setSourceMode(e.target.value as 'real_time_first' | 'official_only')
            }
            style={{
              background: '#0f1117',
              color: '#e2e8f0',
              border: '1px solid #2d3148',
              borderRadius: 6,
              padding: '8px 10px',
            }}
          >
            <option value="real_time_first">Priorizar tiempo real (con fallback)</option>
            <option value="official_only">Solo fuentes oficiales</option>
          </select>
        </label>
      </div>

      {sugerenciasQuery.data && sugerenciasQuery.data.length > 0 && (
        <div
          style={{
            border: '1px solid #2d3148',
            borderRadius: 8,
            background: '#0f1117',
            padding: 8,
          }}
        >
          <div style={{ color: '#94a3b8', fontSize: 12, marginBottom: 6 }}>Sugerencias</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {sugerenciasQuery.data.slice(0, 8).map((item, idx) => (
              <button
                key={item.id}
                onClick={() => onPickSuggestion(idx)}
                style={{
                  background: '#1a1d27',
                  border: '1px solid #2d3148',
                  color: '#cbd5e1',
                  borderRadius: 999,
                  padding: '6px 10px',
                  fontSize: 12,
                  cursor: 'pointer',
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                }}
              >
                <span>{item.label}</span>
                <span
                  style={{
                    background: item.tipo === 'municipio' ? '#0b5cc4' : '#0f7a52',
                    color: '#e2e8f0',
                    borderRadius: 999,
                    padding: '2px 8px',
                    fontSize: 10,
                    textTransform: 'uppercase',
                    letterSpacing: '0.04em',
                  }}
                >
                  {getTipoLabel(item.tipo)}
                </span>
              </button>
            ))}
          </div>
        </div>
      )}

      <div
        style={{
          border: '1px solid #2d3148',
          borderRadius: 10,
          overflow: 'hidden',
          position: 'relative',
        }}
      >
        <iframe
          title="Mapa de ubicación"
          src={mapaEmbedUrl}
          style={{ width: '100%', height: 360, border: 0, display: 'block' }}
          loading="lazy"
        />
        <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none' }}>
          <svg viewBox="0 0 100 100" preserveAspectRatio="none" style={{ width: '100%', height: '100%' }}>
            {overlayData.polygons.map((poly) => (
              <path
                key={poly.key}
                d={poly.d}
                fill={poly.color}
                fillOpacity={0.15}
                stroke={poly.color}
                strokeWidth={0.25}
              >
                <title>{poly.title}</title>
              </path>
            ))}
            {overlayData.lines.map((line) => (
              <path key={line.key} d={line.d} fill="none" stroke={line.color} strokeWidth={0.35}>
                <title>{line.title}</title>
              </path>
            ))}
          </svg>
          {overlayData.points.map((point) => (
            <div
              key={point.key}
              title={point.title}
              style={{
                position: 'absolute',
                left: `${point.x}%`,
                top: `${point.y}%`,
                width: 9,
                height: 9,
                borderRadius: '50%',
                background: point.color,
                border: '1px solid rgba(255,255,255,0.8)',
                transform: 'translate(-50%, -50%)',
                boxShadow: '0 0 0 2px rgba(0,0,0,0.25)',
              }}
            />
          ))}
        </div>
      </div>

      <div style={{ color: '#94a3b8', fontSize: 12 }}>
        Ubicación: {selectedEntidad} ({selectedLat.toFixed(4)}, {selectedLng.toFixed(4)}) · Clave geo:{' '}
        {selectedCveEnt}-{selectedCveMun}
      </div>

      <section>
        <div style={{ color: '#94a3b8', fontSize: 12, marginBottom: 8 }}>Capas</div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
          {capasDisponibles.map((capa) => {
            const isOn = capasActivas.includes(capa.id)
            return (
              <button
                key={capa.id}
                onClick={() => toggleCapa(capa.id)}
                style={{
                  background: isOn ? '#0576F3' : '#1a1d27',
                  border: '1px solid #2d3148',
                  color: isOn ? '#fff' : '#cbd5e1',
                  borderRadius: 999,
                  padding: '6px 10px',
                  fontSize: 12,
                  cursor: 'pointer',
                }}
              >
                {capa.nombre}
              </button>
            )
          })}
        </div>
      </section>

      <section
        style={{
          border: '1px solid #2d3148',
          borderRadius: 8,
          background: '#0f1117',
          padding: 12,
        }}
      >
        <div style={{ color: '#94a3b8', fontSize: 12, marginBottom: 8 }}>
          Estado de capas de datos
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 8 }}>
          {capasDisponibles.map((capa) => {
            const isOn = capasActivas.includes(capa.id)
            return (
              <div
                key={`estado-${capa.id}`}
                style={{
                  border: '1px solid #2d3148',
                  borderRadius: 8,
                  padding: 8,
                  background: isOn ? 'rgba(5,118,243,0.08)' : '#141826',
                }}
              >
                <div style={{ color: '#e2e8f0', fontSize: 12 }}>{capa.nombre}</div>
                <div style={{ color: '#94a3b8', fontSize: 11, marginTop: 4 }}>
                  {isOn ? 'Activa' : 'Inactiva'} · {getSourceTypeLabel(capa.source_type)}
                </div>
              </div>
            )
          })}
        </div>
      </section>

      <section
        style={{
          display: 'grid',
          gap: 10,
          gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
        }}
      >
        {indicadores.map((ind) => (
          <article
            key={ind.clave}
            style={{
              border: '1px solid #2d3148',
              borderRadius: 8,
              background: '#0f1117',
              padding: 12,
            }}
          >
            <div style={{ color: '#94a3b8', fontSize: 11, textTransform: 'uppercase' }}>
              {getBloqueLabel(ind.bloque)}
            </div>
            <div style={{ color: '#e2e8f0', fontSize: 13, marginTop: 4 }}>{ind.clave}</div>
            <div style={{ color: '#fff', fontSize: 20, marginTop: 6 }}>
              {formatNumber(ind.valor)}
            </div>
            <div style={{ color: '#94a3b8', fontSize: 11, marginTop: 4 }}>
              {ind.unidad} · {ind.source_name}
            </div>
          </article>
        ))}
      </section>

      <section
        style={{
          border: '1px solid #2d3148',
          borderRadius: 8,
          background: '#0f1117',
          padding: 12,
        }}
      >
        <div style={{ color: '#94a3b8', fontSize: 12, marginBottom: 8 }}>
          Top 10 puntos de interés
        </div>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
            gap: 8,
            marginBottom: 10,
          }}
        >
          <label style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            <span style={{ color: '#94a3b8', fontSize: 11 }}>Límite de resultados</span>
            <select
              value={poiLimit}
              onChange={(e) => setPoiLimit(Number(e.target.value))}
              style={{
                background: '#141826',
                color: '#e2e8f0',
                border: '1px solid #2d3148',
                borderRadius: 6,
                padding: '7px 10px',
              }}
            >
              {TOP_POI_LIMITS.map((limit) => (
                <option key={limit} value={limit}>
                  Top {limit}
                </option>
              ))}
            </select>
          </label>
          <div style={{ display: 'flex', alignItems: 'flex-end' }}>
            <button
              onClick={clearFiltros}
              style={{
                background: '#1a1d27',
                color: '#e2e8f0',
                border: '1px solid #2d3148',
                borderRadius: 6,
                padding: '8px 10px',
                fontSize: 12,
                cursor: 'pointer',
              }}
            >
              Limpiar filtros
            </button>
          </div>
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginBottom: 10 }}>
          {POI_CATEGORIAS.map((categoria) => {
            const isActive = poiCategorias.includes(categoria.id)
            return (
              <button
                key={categoria.id}
                onClick={() => togglePoiCategoria(categoria.id)}
                style={{
                  background: isActive ? '#0b5cc4' : '#1a1d27',
                  border: '1px solid #2d3148',
                  color: '#e2e8f0',
                  borderRadius: 999,
                  padding: '5px 10px',
                  fontSize: 11,
                  cursor: 'pointer',
                }}
              >
                {categoria.label}
              </button>
            )
          })}
        </div>
        {topPoiQuery.isLoading && <div style={{ color: '#94a3b8', fontSize: 12 }}>Cargando POI...</div>}
        {topPoi.length === 0 ? (
          <div style={{ color: '#94a3b8', fontSize: 12 }}>
            Sin resultados para las capas y radio seleccionados.
          </div>
        ) : (
          <div style={{ display: 'grid', gap: 8 }}>
            {topPoi.map((poi, idx) => (
              <div
                key={poi.id}
                style={{
                  border: '1px solid #2d3148',
                  borderRadius: 8,
                  padding: 10,
                  display: 'flex',
                  justifyContent: 'space-between',
                  gap: 10,
                  flexWrap: 'wrap',
                }}
              >
                <div>
                  <div style={{ color: '#e2e8f0', fontSize: 13 }}>
                    #{idx + 1} {poi.nombre}
                  </div>
                  <div style={{ color: '#94a3b8', fontSize: 11, marginTop: 4 }}>
                    {getPoiCategoriaLabel(poi.categoria)} · {poi.distancia_m.toLocaleString('es-MX')} m
                  </div>
                </div>
                <div style={{ color: '#94a3b8', fontSize: 11 }}>
                  {getSourceTypeLabel(poi.source_type)} · {poi.source_name}
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      <section
        style={{
          border: '1px solid #2d3148',
          borderRadius: 8,
          background: '#0f1117',
          padding: 12,
        }}
      >
        <div style={{ color: '#94a3b8', fontSize: 12, marginBottom: 8 }}>
          Pueblos mágicos cercanos
        </div>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
            gap: 8,
            marginBottom: 10,
          }}
        >
          <label style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            <span style={{ color: '#94a3b8', fontSize: 11 }}>Máximo resultados</span>
            <select
              value={pueblosLimit}
              onChange={(e) => setPueblosLimit(Number(e.target.value))}
              style={{
                background: '#141826',
                color: '#e2e8f0',
                border: '1px solid #2d3148',
                borderRadius: 6,
                padding: '7px 10px',
              }}
            >
              {PUEBLOS_LIMITS.map((limit) => (
                <option key={limit} value={limit}>
                  {limit}
                </option>
              ))}
            </select>
          </label>
          <label style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            <span style={{ color: '#94a3b8', fontSize: 11 }}>Radio máximo (km)</span>
            <select
              value={pueblosRadioKm}
              onChange={(e) => setPueblosRadioKm(Number(e.target.value))}
              style={{
                background: '#141826',
                color: '#e2e8f0',
                border: '1px solid #2d3148',
                borderRadius: 6,
                padding: '7px 10px',
              }}
            >
              {PUEBLOS_RADIOS_KM.map((km) => (
                <option key={km} value={km}>
                  {km} km
                </option>
              ))}
            </select>
          </label>
        </div>
        {pueblosMagicosQuery.isLoading && (
          <div style={{ color: '#94a3b8', fontSize: 12 }}>Cargando pueblos mágicos...</div>
        )}
        {pueblosMagicos.length === 0 ? (
          <div style={{ color: '#94a3b8', fontSize: 12 }}>
            No se encontraron pueblos mágicos cercanos para esta ubicación.
          </div>
        ) : (
          <div style={{ display: 'grid', gap: 8 }}>
            {pueblosMagicos.map((pueblo) => (
              <div
                key={pueblo.id}
                style={{
                  border: '1px solid #2d3148',
                  borderRadius: 8,
                  padding: 10,
                  display: 'flex',
                  justifyContent: 'space-between',
                  gap: 10,
                  flexWrap: 'wrap',
                }}
              >
                <div>
                  <div style={{ color: '#e2e8f0', fontSize: 13 }}>
                    {pueblo.nombre}, {pueblo.entidad}
                  </div>
                  <div style={{ color: '#94a3b8', fontSize: 11, marginTop: 4 }}>
                    Distancia: {pueblo.distancia_km.toLocaleString('es-MX')} km · Tiempo estimado:{' '}
                    {pueblo.tiempo_estimado_min ?? 'N/D'} min
                  </div>
                </div>
                <div style={{ color: '#94a3b8', fontSize: 11 }}>{pueblo.fuente}</div>
              </div>
            ))}
          </div>
        )}
      </section>

      <footer
        style={{
          borderTop: '1px solid #2d3148',
          paddingTop: 10,
          color: '#94a3b8',
          fontSize: 12,
          display: 'flex',
          flexWrap: 'wrap',
          gap: 14,
        }}
      >
        <span>AGEB: {agebQuery.data?.cvegeo ?? 'N/D'}</span>
        <span>Fuente AGEB: {agebQuery.data?.fuente ?? 'N/D'}</span>
        <span>
          Degradación: {query.data?.degradacion.activa ? query.data.degradacion.motivo : 'sin degradación'}
        </span>
        {query.isLoading && <span>Cargando datos de mapa...</span>}
      </footer>
    </section>
  )
}
