'use client'

import { useRef, type RefObject } from 'react'
import { useQuery } from '@tanstack/react-query'
import { apiFetch } from '@/lib/api'
import { useStyleConfig } from '@/hooks/useStyleConfig'
import MexicoMap from './MexicoMap'

function downloadChartAsPng(containerRef: RefObject<HTMLDivElement>, filename: string) {
  const svg = containerRef.current?.querySelector('svg')
  if (!svg) return
  const scale = 3
  const { width: w, height: h } = svg.getBoundingClientRect()
  const blob = new Blob([new XMLSerializer().serializeToString(svg)], { type: 'image/svg+xml;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const img = new Image()
  img.onload = () => {
    const canvas = document.createElement('canvas')
    canvas.width = w * scale; canvas.height = h * scale
    const ctx = canvas.getContext('2d')
    if (!ctx) return
    ctx.fillStyle = '#1a1d27'; ctx.fillRect(0, 0, canvas.width, canvas.height)
    ctx.drawImage(img, 0, 0, canvas.width, canvas.height)
    URL.revokeObjectURL(url)
    const link = document.createElement('a')
    link.download = `${filename}.png`; link.href = canvas.toDataURL('image/png'); link.click()
  }
  img.src = url
}

function fmt(v: number): string {
  return new Intl.NumberFormat('es-MX', { maximumFractionDigits: 0 }).format(v)
}

interface EstadoInfo {
  estado: string
  pib: number
  poblacion: number
  extension: number
  pib_percapita: number
}

export default function EstadosPoblacionPIB({ estado, onEstadoChange }: { estado: string; onEstadoChange: (e: string) => void }) {
  const { palette, fontFamily, titleSize } = useStyleConfig()
  const mapRef = useRef<HTMLDivElement>(null)
  const selected = estado

  const { data, isLoading } = useQuery<{ estados: EstadoInfo[] }>({
    queryKey: ['estados-info'],
    queryFn: () => apiFetch<{ estados: EstadoInfo[] }>('/indicadores/estados/info'),
  })

  const estados = data?.estados ?? []
  const estadoData = estados.find((e) => e.estado === selected)
  const estadosAlfa = [...estados].sort((a, b) => a.estado.localeCompare(b.estado))
  const color = palette[0]

  if (isLoading) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '400px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>Cargando información estatal...</div>)
  }

  const kpis = estadoData ? [
    { label: 'PIB', value: `$${fmt(estadoData.pib)} M`, sub: 'Millones MXN' },
    { label: 'Población', value: fmt(estadoData.poblacion), sub: 'Personas' },
    { label: 'Extensión', value: `${fmt(estadoData.extension)} km²`, sub: 'Superficie' },
    { label: 'PIB per cápita', value: `$${fmt(estadoData.pib_percapita)}`, sub: 'MXN por persona' },
  ] : []

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* Selector de estado */}
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap' }}>
          <div>
            <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '3px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Seleccionar Estado</label>
            <select value={selected} onChange={(e) => onEstadoChange(e.target.value)}
              style={{ background: '#0f1117', border: '1px solid #2d3148', borderRadius: '4px', color: '#e2e8f0', fontSize: '14px', fontFamily, padding: '8px 12px', cursor: 'pointer', outline: 'none', minWidth: '250px' }}>
              {estadosAlfa.map((e) => (<option key={e.estado} value={e.estado}>{e.estado}</option>))}
            </select>
          </div>
          <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700 }}>{selected}</p>
        </div>
      </div>

      {/* KPI Cards */}
      {estadoData && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '12px' }}>
          {kpis.map((kpi, i) => (
            <div key={kpi.label} style={{
              background: '#1a1d27', border: `1px solid ${palette[i % palette.length]}33`,
              borderTop: `3px solid ${palette[i % palette.length]}`, borderRadius: '10px', padding: '16px', fontFamily,
            }}>
              <p style={{ fontSize: '11px', color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.08em', margin: '0 0 8px 0' }}>{kpi.label}</p>
              <p style={{ fontSize: '22px', fontWeight: 300, color: palette[i % palette.length], margin: '0 0 4px 0', lineHeight: 1 }}>{kpi.value}</p>
              <p style={{ fontSize: '11px', color: '#4a5568', margin: 0 }}>{kpi.sub}</p>
            </div>
          ))}
        </div>
      )}

      {/* Mapa placeholder + botón descarga */}
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '12px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
          <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
            Mapa — {selected}
          </p>
          <button onClick={() => downloadChartAsPng(mapRef, `mapa-${selected.toLowerCase().replace(/ /g, '-')}`)} title="Descargar mapa"
            style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
            onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
            onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>↓ PNG</button>
        </div>

        <div ref={mapRef} style={{ display: 'flex', justifyContent: 'center', padding: '20px 0' }}>
          <MexicoMap
            selected={selected}
            onSelect={onEstadoChange}
            highlightColor={color}
            defaultColor="#1e2235"
          />
        </div>

        <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: 0, textAlign: 'left' }}>
          Fuente: INEGI — Censos de Población y Vivienda, Cuentas Nacionales
        </p>
      </div>
    </div>
  )
}
