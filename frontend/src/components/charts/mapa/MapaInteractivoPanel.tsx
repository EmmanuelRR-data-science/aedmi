'use client'

import { useEffect, useMemo, useState } from 'react'

import {
  type MapaCapaDatos,
  useMapaAgeb,
  useMapaAlcance,
  useMapaAnalisisIA,
  useMapaCapas,
  useMapaCapasDatos,
  useMapaPueblosMagicos,
  useMapaQuery,
  useMapaSugerencias,
  useMapaTopPoi,
} from '@/hooks/useMapa'
import { apiFetch } from '@/lib/api'

const RADIOS = [1000, 3000, 5000, 10000]
const TOP_POI_LIMITS = [10, 20, 30]
const PUEBLOS_LIMITS = [5, 10, 15]
const PUEBLOS_RADIOS_KM = [150, 300, 500]
const FILTROS_SESSION_KEY = 'mapa-filtros-v1'
const MAP_LAYER_COLORS: Record<string, string> = {
  red_vial: '#0576F3',
  ageb_urbano: '#8b5cf6',
  ageb_urbana: '#8b5cf6',
  ageb_rural: '#f59e0b',
  nodos_transporte: '#14b8a6',
  poi_hoteles: '#22c55e',
  poi_comercio: '#f97316',
  zona_industrial: '#ef4444',
  zona_vivienda: '#10b981',
}
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

function getDisponibilidadLabel(disponibilidad: 'ok' | 'parcial' | 'sin_datos'): string {
  if (disponibilidad === 'ok') return 'OK'
  if (disponibilidad === 'parcial') return 'Parcial'
  return 'Sin datos'
}

function getPropLabel(key: string): string {
  const labels: Record<string, string> = {
    name: 'Nombre',
    nombre: 'Nombre',
    categoria: 'Categoría',
    kind: 'Tipo',
    landuse: 'Uso de suelo',
    building: 'Edificación',
    cvegeo: 'CVEGEO',
    distancia_m: 'Distancia (m)',
    fallback: 'Fallback',
  }
  return labels[key] ?? key
}

function mapLanduseValue(value: string): string {
  const normalized = value.trim().toLowerCase()
  const labels: Record<string, string> = {
    residential: 'vivienda',
    industrial: 'industrial',
    commercial: 'comercial',
    retail: 'comercial',
    mixed: 'mixto',
  }
  return labels[normalized] ?? value
}

function mapBuildingValue(value: string): string {
  const normalized = value.trim().toLowerCase()
  const labels: Record<string, string> = {
    residential: 'vivienda',
    apartments: 'departamentos',
    house: 'casa',
    commercial: 'comercial',
    industrial: 'industrial',
  }
  return labels[normalized] ?? value
}

function mapTransportKindValue(value: string): string {
  const normalized = value.trim().toLowerCase()
  const labels: Record<string, string> = {
    stop_position: 'parada de transporte',
    platform: 'andén/plataforma',
    station: 'estación',
    bus_station: 'terminal de autobuses',
    aerodrome: 'aeródromo',
    tram_stop: 'parada de tranvía',
    halt: 'paradero',
  }
  return labels[normalized] ?? value
}

function formatPropValue(key: string, value: string | number | boolean | null): string {
  if (value === null) return 'N/D'
  if (typeof value === 'boolean') return value ? 'Sí' : 'No'
  if (key === 'landuse' && typeof value === 'string') return mapLanduseValue(value)
  if (key === 'building' && typeof value === 'string') return mapBuildingValue(value)
  if (key === 'kind' && typeof value === 'string') return mapTransportKindValue(value)
  return String(value)
}

function colorWithAlpha(hexColor: string, alpha: number): string {
  const hex = hexColor.replace('#', '')
  if (hex.length !== 6) return hexColor
  const r = parseInt(hex.slice(0, 2), 16)
  const g = parseInt(hex.slice(2, 4), 16)
  const b = parseInt(hex.slice(4, 6), 16)
  if (Number.isNaN(r) || Number.isNaN(g) || Number.isNaN(b)) return hexColor
  return `rgba(${r}, ${g}, ${b}, ${alpha})`
}

function getFeatureTitle(
  capaNombre: string,
  props: Record<string, string | number | boolean | null>,
  featureId: string
): string {
  const nombre = props.name ?? props.nombre
  const categoria = props.categoria
  if (typeof nombre === 'string' && nombre.trim()) {
    if (typeof categoria === 'string' && categoria.trim()) {
      return `${nombre} (${categoria})`
    }
    return nombre
  }
  const landuse = props.landuse
  if (typeof landuse === 'string' && landuse.trim()) {
    return `Zona ${mapLanduseValue(landuse)} detectada`
  }
  const building = props.building
  if (typeof building === 'string' && building.trim()) {
    return `Polígono ${mapBuildingValue(building)} detectado`
  }
  const cvegeo = props.cvegeo
  if (typeof cvegeo === 'string' && cvegeo.trim()) {
    return `AGEB ${cvegeo}`
  }
  const kind = props.kind
  if (typeof kind === 'string' && kind.trim()) {
    return `Nodo de transporte (${mapTransportKindValue(kind)})`
  }
  return `${capaNombre} · ${featureId}`
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

function slugifyLabel(value: string): string {
  const normalized = value
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
  return normalized || 'ciudad'
}

function escapeForInlineScript(value: string): string {
  return value.replace(/<\//g, '<\\/')
}

function buildMapDownloadHtml(args: {
  ciudad: string
  lat: number
  lng: number
  radio_m: number
  capas: Array<{ id: string; nombre: string }>
  capasDatos: MapaCapaDatos[]
  generatedAtISO: string
}): string {
  const capasConColor = args.capas.map((capa) => ({
    id: capa.id,
    nombre: capa.nombre,
    color: MAP_LAYER_COLORS[capa.id] ?? '#94a3b8',
  }))

  const payload = {
    meta: {
      ciudad: args.ciudad,
      lat: args.lat,
      lng: args.lng,
      radio_m: args.radio_m,
      generated_at: args.generatedAtISO,
    },
    capas: capasConColor,
    capasDatos: args.capasDatos,
  }

  const payloadJson = escapeForInlineScript(JSON.stringify(payload))
  const generatedLabel = new Date(args.generatedAtISO).toLocaleString('es-MX')

  return `<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Mapa exportado · ${args.ciudad}</title>
    <link
      rel="stylesheet"
      href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
      integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
      crossorigin=""
    />
    <style>
      body {
        margin: 0;
        font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
        background: #0f172a;
        color: #e2e8f0;
      }
      .layout {
        display: grid;
        grid-template-rows: auto 1fr;
        min-height: 100vh;
      }
      .header {
        border-bottom: 1px solid #334155;
        padding: 12px 16px;
        background: #111827;
      }
      .title {
        margin: 0;
        font-size: 18px;
      }
      .meta {
        margin-top: 4px;
        font-size: 12px;
        color: #94a3b8;
      }
      #map {
        width: 100%;
        height: calc(100vh - 92px);
      }
      .leaflet-control-layers {
        max-width: 320px;
      }
      .leaflet-control-layers-overlays label {
        font-size: 12px;
      }
      .feature-card {
        position: fixed;
        right: 16px;
        bottom: 16px;
        width: min(360px, calc(100vw - 32px));
        max-height: 45vh;
        overflow: auto;
        border: 1px solid #334155;
        border-radius: 10px;
        background: rgba(15, 23, 42, 0.95);
        color: #e2e8f0;
        box-shadow: 0 8px 24px rgba(2, 6, 23, 0.45);
        backdrop-filter: blur(4px);
        z-index: 1000;
      }
      .feature-card.is-hidden {
        display: none;
      }
      .feature-card__body {
        padding: 10px 12px;
      }
      .feature-card__title {
        font-size: 13px;
        font-weight: 600;
        margin: 0;
      }
      .feature-card__subtitle {
        margin: 4px 0 0;
        font-size: 11px;
        color: #94a3b8;
      }
      .feature-card__meta {
        margin-top: 8px;
        display: grid;
        gap: 4px;
        font-size: 12px;
      }
      .feature-card__empty {
        color: #94a3b8;
        font-size: 12px;
      }
      .feature-card__close {
        position: absolute;
        top: 8px;
        right: 8px;
        border: 1px solid #334155;
        background: #1e293b;
        color: #e2e8f0;
        border-radius: 6px;
        padding: 3px 7px;
        cursor: pointer;
        font-size: 11px;
      }
    </style>
  </head>
  <body>
    <div class="layout">
      <header class="header">
        <h1 class="title">Mapa exportado · ${args.ciudad}</h1>
        <div class="meta">
          Coordenadas: ${args.lat.toFixed(6)}, ${args.lng.toFixed(6)} · Radio: ${args.radio_m.toLocaleString('es-MX')} m · Generado: ${generatedLabel}
        </div>
      </header>
      <main id="map"></main>
    </div>
    <aside id="feature-card" class="feature-card is-hidden">
      <button id="feature-card-close" class="feature-card__close" type="button">Cerrar</button>
      <div class="feature-card__body">
        <h2 id="feature-card-title" class="feature-card__title">Elemento seleccionado</h2>
        <p id="feature-card-subtitle" class="feature-card__subtitle"></p>
        <div id="feature-card-meta" class="feature-card__meta"></div>
      </div>
    </aside>

    <script
      src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
      integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
      crossorigin=""
    ></script>
    <script>
      const payload = ${payloadJson}
      const map = L.map('map', { preferCanvas: true }).setView([payload.meta.lat, payload.meta.lng], 13)
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 19,
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      }).addTo(map)
      const featureCard = document.getElementById('feature-card')
      const featureCardTitle = document.getElementById('feature-card-title')
      const featureCardSubtitle = document.getElementById('feature-card-subtitle')
      const featureCardMeta = document.getElementById('feature-card-meta')
      const featureCardClose = document.getElementById('feature-card-close')

      const showFeatureCard = (name, layerName, properties) => {
        if (!featureCard || !featureCardTitle || !featureCardSubtitle || !featureCardMeta) return
        featureCard.classList.remove('is-hidden')
        featureCardTitle.textContent = name || 'Elemento seleccionado'
        featureCardSubtitle.textContent = layerName ? 'Capa: ' + layerName : ''
        const safeEntries = Object.entries(properties || {})
          .filter(([key, value]) => !String(key).startsWith('__') && value !== null && value !== '')
          .slice(0, 12)
        if (!safeEntries.length) {
          featureCardMeta.innerHTML = '<div class="feature-card__empty">Sin atributos disponibles.</div>'
          return
        }
        featureCardMeta.innerHTML = safeEntries
          .map(([key, value]) => '<div><strong>' + key + ':</strong> ' + String(value) + '</div>')
          .join('')
      }

      if (featureCardClose && featureCard) {
        featureCardClose.addEventListener('click', () => {
          featureCard.classList.add('is-hidden')
        })
      }

      const overlayMaps = {}
      const bounds = L.latLngBounds([])
      const colorById = Object.fromEntries(payload.capas.map((capa) => [capa.id, capa.color]))
      const nombreById = Object.fromEntries(payload.capas.map((capa) => [capa.id, capa.nombre]))

      const styleByLayer = (layerId) => {
        const color = colorById[layerId] || '#94a3b8'
        return { color, weight: 2, fillColor: color, fillOpacity: 0.18, opacity: 0.9 }
      }

      payload.capas.forEach((capa) => {
        const grupo = L.layerGroup().addTo(map)
        overlayMaps[capa.nombre] = grupo
      })

      payload.capasDatos.forEach((capaData) => {
        const nombre = nombreById[capaData.capa_id] || capaData.nombre || capaData.capa_id
        const grupo = overlayMaps[nombre] || L.layerGroup().addTo(map)
        overlayMaps[nombre] = grupo
        const featureCollection = {
          type: 'FeatureCollection',
          features: (capaData.features || []).map((feature) => ({
            type: 'Feature',
            id: feature.id,
            geometry: {
              type: feature.geometry_type,
              coordinates: feature.coordinates,
            },
            properties: {
              ...(feature.properties || {}),
              __layer_name: nombre,
              __layer_id: capaData.capa_id,
              __geometry_type: feature.geometry_type,
            },
          })),
        }

        const geo = L.geoJSON(featureCollection, {
          style: () => styleByLayer(capaData.capa_id),
          pointToLayer: (feature, latlng) =>
            L.circleMarker(latlng, {
              radius: 5,
              ...styleByLayer(capaData.capa_id),
              fillOpacity: 0.85,
            }),
          onEachFeature: (feature, layer) => {
            const props = feature.properties || {}
            const title = props.name || props.nombre || nombre
            const info = Object.entries(props)
              .filter(([key, value]) => !String(key).startsWith('__') && value !== null && value !== '')
              .slice(0, 8)
              .map(([key, value]) => '<div><strong>' + key + ':</strong> ' + String(value) + '</div>')
              .join('')
            layer.bindPopup('<div><strong>' + title + '</strong><div style="margin-top:6px">' + info + '</div></div>')
            layer.on('click', () => {
              const geometry = String(props.__geometry_type || '')
              if (
                geometry === 'Point' ||
                geometry === 'Polygon' ||
                geometry === 'MultiPolygon'
              ) {
                showFeatureCard(String(title), String(nombre), props)
              }
            })
          },
        }).addTo(grupo)

        try {
          const geoBounds = geo.getBounds()
          if (geoBounds.isValid()) bounds.extend(geoBounds)
        } catch {
          // noop
        }
      })

      L.circle([payload.meta.lat, payload.meta.lng], {
        radius: payload.meta.radio_m,
        color: '#ffffff',
        weight: 1,
        fillOpacity: 0.05,
      }).addTo(map)

      L.marker([payload.meta.lat, payload.meta.lng]).addTo(map).bindPopup('Ubicación consultada')
      L.control.layers({}, overlayMaps, { collapsed: false }).addTo(map)

      if (bounds.isValid()) {
        map.fitBounds(bounds.pad(0.1))
      }
    </script>
  </body>
</html>`
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
  const [selectedMapItem, setSelectedMapItem] = useState<{
    tipo: 'punto' | 'linea' | 'geocerca'
    capa: string
    titulo: string
    fuente?: string
    propiedades?: Record<string, string | number | boolean | null>
  } | null>(null)
  const [isDownloadingMapHtml, setIsDownloadingMapHtml] = useState(false)
  const [downloadMapError, setDownloadMapError] = useState<string | null>(null)

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
  const capasActivas = useMemo(() => selectedCapas, [selectedCapas])

  const hasLocation = Number.isFinite(selectedLat) && Number.isFinite(selectedLng)
  const alcanceQuery = useMapaAlcance({
    cve_ent: selectedCveEnt,
    cve_mun: selectedCveMun,
    enabled: hasLocation,
  })
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
  const capasDatosPorId = useMemo(() => {
    const entries = (capasDatosQuery.data ?? []).map((item) => [item.capa_id, item] as const)
    return new Map(entries)
  }, [capasDatosQuery.data])
  const analisisMapaMutation = useMapaAnalisisIA()

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
  const capasActivasKey = useMemo(() => capasActivas.join(','), [capasActivas])

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
    if (capasActivas.length === 0) {
      return { points: [], lines: [], polygons: [] }
    }
    const points: Array<{
      key: string
      x: number
      y: number
      color: string
      title: string
      capa: string
      source?: string
      properties: Record<string, string | number | boolean | null>
    }> = []
    const lines: Array<{
      key: string
      d: string
      color: string
      title: string
      capa: string
      source?: string
      properties: Record<string, string | number | boolean | null>
    }> = []
    const polygons: Array<{
      key: string
      d: string
      color: string
      title: string
      capa: string
      source?: string
      properties: Record<string, string | number | boolean | null>
    }> = []
    const lngSpan = Math.max(mapViewport.right - mapViewport.left, 1e-8)
    const latSpan = Math.max(mapViewport.top - mapViewport.bottom, 1e-8)
    const project = (lat: number, lng: number) => ({
      x: ((lng - mapViewport.left) / lngSpan) * 100,
      y: ((mapViewport.top - lat) / latSpan) * 100,
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
      const color = MAP_LAYER_COLORS[capa.capa_id] ?? '#94a3b8'
      capa.features.forEach((feature) => {
        const props = feature.properties ?? {}
        // Solo renderizar elementos con datos reales; ocultar geometrías de fallback/sintéticas.
        const isFallback =
          props.fallback === true ||
          props.synthetic === true ||
          String(props.kind ?? '').toLowerCase() === 'fallback'
        if (isFallback) return
        const title = getFeatureTitle(capa.nombre, props, feature.id)
        if (feature.geometry_type === 'Point') {
          const [lng, lat] = feature.coordinates as [number, number]
          const p = project(lat, lng)
          const isVisiblePoint = p.x >= 0 && p.x <= 100 && p.y >= 0 && p.y <= 100
          if (!isVisiblePoint) return
          points.push({
            key: `${capa.capa_id}-${feature.id}`,
            x: p.x,
            y: p.y,
            color,
            title,
            capa: capa.nombre,
            source: capa.source_name,
            properties: props,
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
              title,
              capa: capa.nombre,
              source: capa.source_name,
              properties: props,
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
                title,
                capa: capa.nombre,
                source: capa.source_name,
                properties: props,
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
              title,
              capa: capa.nombre,
              source: capa.source_name,
              properties: props,
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
                title,
                capa: capa.nombre,
                source: capa.source_name,
                properties: props,
              })
            }
          })
        }
      })
    })

    return { points, lines, polygons }
  }, [capasActivas.length, capasDatosQuery.data, mapViewport])

  useEffect(() => {
    setSelectedMapItem(null)
  }, [selectedLat, selectedLng, radio, capasActivasKey])

  const clearFiltros = () => {
    setPoiCategorias([])
    setPoiLimit(10)
    setPueblosLimit(5)
    setPueblosRadioKm(300)
  }

  const descargarMapaHtml = async () => {
    if (!hasLocation || capasDisponibles.length === 0 || isDownloadingMapHtml) return
    setIsDownloadingMapHtml(true)
    setDownloadMapError(null)
    try {
      const capasIds = capasDisponibles.map((capa) => capa.id)
      const params = new URLSearchParams({
        lat: String(selectedLat),
        lng: String(selectedLng),
        radio_m: String(radio),
        capas: capasIds.join(','),
      })
      if (selectedCveEnt) params.set('cve_ent', selectedCveEnt)
      if (selectedCveMun) params.set('cve_mun', selectedCveMun)

      const capasDatos = await apiFetch<MapaCapaDatos[]>(`/mapa/capas/datos?${params.toString()}`)
      const generatedAtISO = new Date().toISOString()
      const html = buildMapDownloadHtml({
        ciudad: selectedEntidad,
        lat: selectedLat,
        lng: selectedLng,
        radio_m: radio,
        capas: capasDisponibles.map((capa) => ({ id: capa.id, nombre: capa.nombre })),
        capasDatos,
        generatedAtISO,
      })
      const blob = new Blob([html], { type: 'text/html;charset=utf-8' })
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = `mapa-${slugifyLabel(selectedEntidad)}-${generatedAtISO.slice(0, 10)}.html`
      document.body.appendChild(link)
      link.click()
      link.remove()
      URL.revokeObjectURL(url)
    } catch {
      setDownloadMapError('No se pudo generar la descarga del mapa en este momento.')
    } finally {
      setIsDownloadingMapHtml(false)
    }
  }

  const generarInformeIA = async () => {
    const capasActivasData = (capasDatosQuery.data ?? []).filter((capa) =>
      capasActivas.includes(capa.capa_id)
    )
    await analisisMapaMutation.mutateAsync({
      contexto: {
        ubicacion: selectedEntidad,
        cve_ent: selectedCveEnt,
        cve_mun: selectedCveMun,
        lat: selectedLat,
        lng: selectedLng,
        radio_m: radio,
        source_mode: sourceMode,
        capas_activas: capasActivas,
      },
      datos_filtrados: [
        ...capasActivasData.map((capa) => ({
          tipo: 'capa',
          capa_id: capa.capa_id,
          nombre: capa.nombre,
          disponibilidad: capa.disponibilidad,
          fuente: capa.source_name,
          total_features: capa.features.length,
        })),
        ...topPoi.slice(0, 10).map((poi) => ({
          tipo: 'poi',
          nombre: poi.nombre,
          categoria: poi.categoria,
          distancia_m: poi.distancia_m,
          fuente: poi.source_name,
          source_type: poi.source_type,
        })),
        ...pueblosMagicos.slice(0, 10).map((pueblo) => ({
          tipo: 'pueblo_magico',
          nombre: pueblo.nombre,
          entidad: pueblo.entidad,
          distancia_km: pueblo.distancia_km,
          tiempo_estimado_min: pueblo.tiempo_estimado_min,
          fuente: pueblo.fuente,
        })),
        ...indicadores.map((indicador) => ({
          tipo: 'indicador',
          clave: indicador.clave,
          bloque: indicador.bloque,
          valor: indicador.valor,
          unidad: indicador.unidad,
          fuente: indicador.source_name,
        })),
      ],
    })
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
        <h2 style={{ margin: 0, color: '#e2e8f0', fontSize: 18, fontWeight: 500 }}>Sección Mapa Interactivo</h2>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
          <span style={{ fontSize: 12, color: '#94a3b8' }}>
            Estado: {query.data?.degradacion.activa ? 'Fallback activo' : 'Tiempo real'} ·{' '}
            {getModoLabel(sourceMode)}
          </span>
          <button
            onClick={descargarMapaHtml}
            disabled={!hasLocation || capasDisponibles.length === 0 || isDownloadingMapHtml}
            style={{
              background:
                !hasLocation || capasDisponibles.length === 0 || isDownloadingMapHtml ? '#202635' : '#0b5cc4',
              color: '#e2e8f0',
              border: '1px solid #2d3148',
              borderRadius: 6,
              padding: '8px 10px',
              fontSize: 12,
              cursor:
                !hasLocation || capasDisponibles.length === 0 || isDownloadingMapHtml
                  ? 'not-allowed'
                  : 'pointer',
            }}
          >
            {isDownloadingMapHtml ? 'Generando HTML...' : 'Descargar mapa HTML'}
          </button>
        </div>
      </header>
      {downloadMapError && <div style={{ color: '#fca5a5', fontSize: 12 }}>{downloadMapError}</div>}

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
                style={{ pointerEvents: 'auto', cursor: 'pointer' }}
                onClick={() =>
                  setSelectedMapItem({
                    tipo: 'geocerca',
                    capa: poly.capa,
                    titulo: poly.title,
                    fuente: poly.source,
                    propiedades: poly.properties,
                  })
                }
              >
                <title>{poly.title}</title>
              </path>
            ))}
            {overlayData.lines.map((line) => (
              <path
                key={line.key}
                d={line.d}
                fill="none"
                stroke={line.color}
                strokeWidth={0.35}
                style={{ pointerEvents: 'auto', cursor: 'pointer' }}
                onClick={() =>
                  setSelectedMapItem({
                    tipo: 'linea',
                    capa: line.capa,
                    titulo: line.title,
                    fuente: line.source,
                    propiedades: line.properties,
                  })
                }
              >
                <title>{line.title}</title>
              </path>
            ))}
          </svg>
          {overlayData.points.map((point) => (
            <div
              key={point.key}
              title={point.title}
              onClick={() =>
                setSelectedMapItem({
                  tipo: 'punto',
                  capa: point.capa,
                  titulo: point.title,
                  fuente: point.source,
                  propiedades: point.properties,
                })
              }
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
                pointerEvents: 'auto',
                cursor: 'pointer',
              }}
            />
          ))}
        </div>
      </div>
      {selectedMapItem && (
        <div
          style={{
            border: '1px solid #2d3148',
            borderRadius: 8,
            background: '#0f1117',
            padding: 10,
          }}
        >
          <div style={{ color: '#e2e8f0', fontSize: 13, fontWeight: 600 }}>
            {selectedMapItem.tipo.toUpperCase()} · {selectedMapItem.capa}
          </div>
          <div style={{ color: '#94a3b8', fontSize: 12, marginTop: 4 }}>{selectedMapItem.titulo}</div>
          <div style={{ color: '#64748b', fontSize: 11, marginTop: 4 }}>
            Fuente: {selectedMapItem.fuente ?? 'N/D'}
          </div>
          {selectedMapItem.propiedades && (
            <div style={{ marginTop: 8, display: 'grid', gap: 4 }}>
              {Object.entries(selectedMapItem.propiedades)
                .filter(([, value]) => value !== null && value !== '')
                .slice(0, 8)
                .map(([key, value]) => (
                  <div key={key} style={{ color: '#94a3b8', fontSize: 11 }}>
                    {getPropLabel(key)}: {formatPropValue(key, value)}
                  </div>
                ))}
            </div>
          )}
        </div>
      )}

      <div style={{ color: '#94a3b8', fontSize: 12 }}>
        Ubicación: {selectedEntidad} ({selectedLat.toFixed(4)}, {selectedLng.toFixed(4)}) · Clave geo:{' '}
        {selectedCveEnt}-{selectedCveMun}
      </div>
      {alcanceQuery.data && !alcanceQuery.data.en_alcance && (
        <div
          style={{
            border: '1px solid #7c2d12',
            background: 'rgba(124,45,18,0.15)',
            borderRadius: 8,
            color: '#fdba74',
            fontSize: 12,
            padding: 10,
          }}
        >
          {alcanceQuery.data.mensaje}
        </div>
      )}

      <section>
        <div style={{ color: '#94a3b8', fontSize: 12, marginBottom: 8 }}>Capas</div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
          {capasDisponibles.map((capa) => {
            const isOn = capasActivas.includes(capa.id)
            const capaColor = MAP_LAYER_COLORS[capa.id] ?? '#94a3b8'
            return (
              <button
                key={capa.id}
                onClick={() => toggleCapa(capa.id)}
                style={{
                  background: isOn ? capaColor : colorWithAlpha(capaColor, 0.12),
                  border: `1px solid ${capaColor}`,
                  color: isOn ? '#ffffff' : capaColor,
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
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: 10,
            flexWrap: 'wrap',
          }}
        >
          <div style={{ color: '#94a3b8', fontSize: 12 }}>
            Informe IA de los filtros activos del mapa
          </div>
          <button
            onClick={generarInformeIA}
            disabled={capasActivas.length === 0 || analisisMapaMutation.isPending}
            style={{
              background:
                capasActivas.length === 0 || analisisMapaMutation.isPending ? '#202635' : '#0b5cc4',
              color: '#e2e8f0',
              border: '1px solid #2d3148',
              borderRadius: 6,
              padding: '8px 10px',
              fontSize: 12,
              cursor:
                capasActivas.length === 0 || analisisMapaMutation.isPending ? 'not-allowed' : 'pointer',
            }}
          >
            {analisisMapaMutation.isPending ? 'Generando informe...' : 'Generar informe IA'}
          </button>
        </div>
        {capasActivas.length === 0 && (
          <div style={{ color: '#94a3b8', fontSize: 12, marginTop: 10 }}>
            Activa al menos una capa para generar un informe contextual.
          </div>
        )}
        {analisisMapaMutation.isError && (
          <div style={{ color: '#fca5a5', fontSize: 12, marginTop: 10 }}>
            No se pudo generar el informe IA en este momento.
          </div>
        )}
        {analisisMapaMutation.data?.analisis_ia && (
          <div
            style={{
              marginTop: 10,
              border: '1px solid #2d3148',
              borderRadius: 8,
              background: '#141826',
              padding: 10,
              color: '#e2e8f0',
              fontSize: 12,
              lineHeight: 1.5,
              whiteSpace: 'pre-wrap',
            }}
          >
            {analisisMapaMutation.data.analisis_ia}
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
          Estado de capas de datos
        </div>
        {capasDatosQuery.isLoading && (
          <div style={{ color: '#94a3b8', fontSize: 11, marginBottom: 8 }}>
            Consultando capas activas...
          </div>
        )}
        {capasDatosQuery.isError && (
          <div style={{ color: '#fca5a5', fontSize: 11, marginBottom: 8 }}>
            No fue posible cargar una o más capas en este momento.{' '}
            {capasDatosQuery.error instanceof Error ? capasDatosQuery.error.message : 'Intenta de nuevo.'}
          </div>
        )}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 8 }}>
          {capasDisponibles.map((capa) => {
            const isOn = capasActivas.includes(capa.id)
            const capaColor = MAP_LAYER_COLORS[capa.id] ?? '#94a3b8'
            const capaData = capasDatosPorId.get(capa.id)
            return (
              <div
                key={`estado-${capa.id}`}
                style={{
                  border: '1px solid #2d3148',
                  borderRadius: 8,
                  padding: 8,
                  background: isOn ? colorWithAlpha(capaColor, 0.12) : '#141826',
                }}
              >
                <div style={{ color: '#e2e8f0', fontSize: 12 }}>{capa.nombre}</div>
                <div style={{ color: '#94a3b8', fontSize: 11, marginTop: 4 }}>
                  {isOn ? 'Activa' : 'Inactiva'} · {getSourceTypeLabel(capa.source_type)}
                </div>
                <div style={{ color: '#94a3b8', fontSize: 11, marginTop: 2 }}>
                  Disponibilidad:{' '}
                  {isOn && capaData
                    ? getDisponibilidadLabel(capaData.disponibilidad)
                    : 'No consultada'}
                </div>
                <div style={{ color: '#64748b', fontSize: 10, marginTop: 2 }}>
                  Fuente: {isOn && capaData ? capaData.source_name : 'N/D'}
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
            const poiColor =
              categoria.id === 'hotelero' ? MAP_LAYER_COLORS.poi_hoteles : MAP_LAYER_COLORS.poi_comercio
            return (
              <button
                key={categoria.id}
                onClick={() => togglePoiCategoria(categoria.id)}
                style={{
                  background: isActive ? poiColor : colorWithAlpha(poiColor, 0.12),
                  border: `1px solid ${poiColor}`,
                  color: isActive ? '#ffffff' : poiColor,
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
