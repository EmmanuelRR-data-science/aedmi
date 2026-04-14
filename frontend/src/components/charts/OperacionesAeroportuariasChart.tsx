'use client'

import { useRef, useState, type RefObject } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, Cell,
} from 'recharts'
import { useStyleConfig, getColorForIndex } from '@/hooks/useStyleConfig'
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

type Tab = 'total' | 'grupo' | 'apto'

export default function OperacionesAeroportuariasChart() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)
  const [tab, setTab] = useState<Tab>('total')

  const { data: indicadores } = useIndicadores('nacional', 'conectividad_aerea')
  const indicador = indicadores?.find((i) => i.clave === 'afac.operaciones_aeroportuarias')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>Cargando operaciones...</div>)
  }
  if (!datos.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>Sin datos disponibles.</div>)
  }

  const filter = (prefix: string) => datos.filter((d) => d.entidad_clave?.startsWith(`${prefix}:`))
    .map((d) => ({ name: d.entidad_clave?.replace(`${prefix}:`, '') ?? '', value: Number(d.valor), anio: String(d.periodo) }))

  const totalData = filter('total').sort((a, b) => Number(a.anio) - Number(b.anio))
  const grupoData = filter('grupo').sort((a, b) => b.value - a.value)
  const aptoData = filter('apto').sort((a, b) => b.value - a.value)

  const titles: Record<Tab, string> = { total: 'Total de Operaciones Aeroportuarias', grupo: 'Operaciones por Grupo Aeroportuario (2024)', apto: 'Top 10 Aeropuertos (2024)' }
  const activeData = tab === 'total' ? totalData : tab === 'grupo' ? grupoData : aptoData
  const maxVal = activeData.length ? Math.max(...activeData.map((d) => d.value)) : 0
  const yMax = Math.ceil(maxVal * 1.15)

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>{titles[tab]}</p>
        <button onClick={() => downloadChartAsPng(chartRef, `operaciones-aeroportuarias-${tab}`)} title="Descargar" style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>↓ PNG</button>
      </div>
      <div style={{ display: 'flex', gap: '6px', justifyContent: 'center' }}>
        {([['total', 'Total'], ['grupo', 'Por Grupo'], ['apto', 'Top Aeropuertos']] as [Tab, string][]).map(([t, l]) => (
          <button key={t} onClick={() => setTab(t)} style={{ padding: '6px 16px', fontSize: '12px', fontFamily, borderRadius: '4px', cursor: 'pointer', background: t === tab ? palette[0] ?? palette[1] : 'transparent', color: t === tab ? '#fff' : '#94a3b8', border: t === tab ? 'none' : '1px solid #2d3148' }}>{l}</button>
        ))}
      </div>
      <div ref={chartRef}>
        {tab === 'total' ? (
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={totalData.map((d) => ({ anio: d.anio, valor: d.value }))} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis domain={[0, yMax]} tickFormatter={(v) => fmt(v)} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number) => [`${fmt(v)} miles`, 'Operaciones']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} formatter={() => 'Operaciones (miles)'} />
              <Bar dataKey="valor" fill={palette[0] ?? palette[1]} radius={[3, 3, 0, 0]} name="Operaciones" />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <ResponsiveContainer width="100%" height={Math.max(320, activeData.length * 40)}>
            <BarChart data={activeData} layout="vertical" margin={{ top: 8, right: 60, left: 10, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" horizontal={false} />
              <XAxis type="number" domain={[0, yMax]} tickFormatter={(v) => fmt(v)} tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis type="category" dataKey="name" width={140} tick={{ fontSize: yAxisSize - 1, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number) => [`${fmt(v)} miles`, 'Operaciones']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Bar dataKey="value" radius={[0, 3, 3, 0]} name="Operaciones">
                {activeData.map((_, i) => (<Cell key={i} fill={getColorForIndex(palette, i)} />))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>Fuente: AFAC / Datatur — Estadísticas de aviación civil</p>
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead><tr style={{ borderBottom: '1px solid #2d3148' }}>
            <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>{tab === 'total' ? 'Año' : 'Nombre'}</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Operaciones (miles)</th>
          </tr></thead>
          <tbody>
            {activeData.map((d) => (
              <tr key={tab === 'total' ? d.anio : d.name} style={{ borderBottom: '1px solid #1e2235' }}>
                <td style={{ padding: '5px 8px' }}>{tab === 'total' ? d.anio : d.name}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(d.value)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {indicador && <AnalisisIA graficaId={indicador.id} />}
      {indicador && <AnalisisRevisado graficaId={indicador.id} />}
    </div>
  )
}
