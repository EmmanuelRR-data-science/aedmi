'use client'

import { useRef, useState, type RefObject } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, Line, ComposedChart,
} from 'recharts'
import { useStyleConfig } from '@/hooks/useStyleConfig'
import { useIndicadores, useIndicadorDatos } from '@/hooks/useIndicador'
import AnalisisIA from '@/components/ai/AnalisisIA'
import AnalisisRevisado from '@/components/ai/AnalisisRevisado'
import type { DatoIndicador } from '@/types'

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

type Tab = 'historico' | 'categoria'

export default function ActividadHoteleraChart() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)
  const [tab, setTab] = useState<Tab>('historico')

  const { data: indicadores } = useIndicadores('nacional', 'turismo')
  const indicador = indicadores?.find((i) => i.clave === 'inegi.actividad_hotelera')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>Cargando actividad hotelera...</div>)
  }
  if (!datos.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>Sin datos disponibles.</div>)
  }

  // Histórico: agrupar por año
  const histAnios: Record<string, { disp: number; ocup: number; pct: number }> = {}
  datos.filter((d) => d.entidad_clave?.startsWith('hist:')).forEach((d) => {
    const anio = String(d.periodo)
    if (!histAnios[anio]) histAnios[anio] = { disp: 0, ocup: 0, pct: 0 }
    if (d.entidad_clave?.includes('disp')) histAnios[anio].disp = Number(d.valor)
    if (d.entidad_clave?.includes('ocup')) histAnios[anio].ocup = Number(d.valor)
    if (d.entidad_clave?.includes('pct')) histAnios[anio].pct = Number(d.valor)
  })
  const histData = Object.entries(histAnios).sort(([a], [b]) => Number(a) - Number(b)).map(([anio, v]) => ({ anio, ...v }))

  // Categorías
  const catMap: Record<string, { disp: number; ocup: number; pct: number }> = {}
  datos.filter((d) => d.entidad_clave?.startsWith('cat:')).forEach((d) => {
    const parts = d.entidad_clave?.split(':') ?? []
    const cat = parts[2] ?? ''
    if (!catMap[cat]) catMap[cat] = { disp: 0, ocup: 0, pct: 0 }
    if (d.entidad_clave?.includes('cat:disp')) catMap[cat].disp = Number(d.valor)
    if (d.entidad_clave?.includes('cat:ocup')) catMap[cat].ocup = Number(d.valor)
    if (d.entidad_clave?.includes('cat:pct')) catMap[cat].pct = Number(d.valor)
  })
  const catData = Object.entries(catMap).map(([cat, v]) => ({ cat, ...v }))

  const titles: Record<Tab, string> = { historico: 'Actividad Hotelera — Histórico Nacional', categoria: 'Ocupación Hotelera por Categoría (2024)' }
  const maxHist = histData.length ? Math.max(...histData.map((d) => d.disp)) : 0
  const yMaxHist = Math.ceil(maxHist * 1.15)
  const maxCat = catData.length ? Math.max(...catData.map((d) => d.disp)) : 0
  const yMaxCat = Math.ceil(maxCat * 1.15)

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>{titles[tab]}</p>
        <button onClick={() => downloadChartAsPng(chartRef, `actividad-hotelera-${tab}`)} title="Descargar" style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>↓ PNG</button>
      </div>
      <div style={{ display: 'flex', gap: '6px', justifyContent: 'center' }}>
        {([['historico', 'Histórico Nacional'], ['categoria', 'Por Categoría']] as [Tab, string][]).map(([t, l]) => (
          <button key={t} onClick={() => setTab(t)} style={{ padding: '6px 16px', fontSize: '12px', fontFamily, borderRadius: '4px', cursor: 'pointer', background: t === tab ? palette[0] ?? palette[1] : 'transparent', color: t === tab ? '#fff' : '#94a3b8', border: t === tab ? 'none' : '1px solid #2d3148' }}>{l}</button>
        ))}
      </div>
      <div ref={chartRef}>
        {tab === 'historico' ? (
          <ResponsiveContainer width="100%" height={320}>
            <ComposedChart data={histData} margin={{ top: 24, right: 50, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis yAxisId="left" domain={[0, yMaxHist]} tickFormatter={(v) => fmt(v)} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis yAxisId="right" orientation="right" domain={[0, 100]} tickFormatter={(v) => `${v}%`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
              <Bar yAxisId="left" dataKey="disp" fill={palette[0] ?? palette[1]} radius={[3, 3, 0, 0]} name="Disponibles (miles)" />
              <Bar yAxisId="left" dataKey="ocup" fill={palette[1] ?? palette[0]} radius={[3, 3, 0, 0]} name="Ocupados (miles)" />
              <Line yAxisId="right" type="monotone" dataKey="pct" stroke={palette[2] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="% Ocupación" />
            </ComposedChart>
          </ResponsiveContainer>
        ) : (
          <ResponsiveContainer width="100%" height={320}>
            <ComposedChart data={catData} margin={{ top: 24, right: 50, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis dataKey="cat" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis yAxisId="left" domain={[0, yMaxCat]} tickFormatter={(v) => fmt(v)} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis yAxisId="right" orientation="right" domain={[0, 100]} tickFormatter={(v) => `${v}%`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
              <Bar yAxisId="left" dataKey="disp" fill={palette[0] ?? palette[1]} radius={[3, 3, 0, 0]} name="Disponibles (miles)" />
              <Bar yAxisId="left" dataKey="ocup" fill={palette[1] ?? palette[0]} radius={[3, 3, 0, 0]} name="Ocupados (miles)" />
              <Line yAxisId="right" type="monotone" dataKey="pct" stroke={palette[2] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="% Ocupación" />
            </ComposedChart>
          </ResponsiveContainer>
        )}
      </div>
      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>Fuente: INEGI / Datatur — Estadísticas de ocupación hotelera</p>
      {indicador && <AnalisisIA graficaId={indicador.id} />}
      {indicador && <AnalisisRevisado graficaId={indicador.id} />}
    </div>
  )
}
