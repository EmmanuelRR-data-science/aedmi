'use client'

import { useRef, useState, type RefObject } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, Cell,
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

export default function IEDEstadosChart() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)
  const [selected, setSelected] = useState<string>('Ciudad de México')

  const { data: indicadores } = useIndicadores('nacional', 'economia')
  const indicador = indicadores?.find((i) => i.clave === 'se.ied_estados')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>Cargando IED por estado...</div>)
  }
  if (!datos.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>Sin datos disponibles.</div>)
  }

  const chartData = datos
    .filter((d) => d.entidad_clave)
    .map((d) => ({ estado: d.entidad_clave ?? '', mdd: Number(d.valor) }))
    .sort((a, b) => b.mdd - a.mdd)

  const selectedData = chartData.find((d) => d.estado === selected)
  const maxVal = chartData.length ? Math.max(...chartData.map((d) => d.mdd)) : 0
  const yMax = Math.ceil(maxVal * 1.15)
  const defaultColor = palette[0]
  const highlightColor = palette[1]

  // Lista alfabética para el dropdown
  const estadosAlfa = [...chartData].sort((a, b) => a.estado.localeCompare(b.estado))

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* Título + badge estado + botón descarga */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          Flujo de IED por Estado (2024)
        </p>
        <button onClick={() => downloadChartAsPng(chartRef, 'ied-estados')} title="Descargar" style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>↓ PNG</button>
      </div>

      {/* Selector de estado + badge */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '16px', justifyContent: 'center', flexWrap: 'wrap' }}>
        <div>
          <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '3px' }}>Seleccionar Estado</label>
          <select value={selected} onChange={(e) => setSelected(e.target.value)}
            style={{ background: '#0f1117', border: '1px solid #2d3148', borderRadius: '4px', color: '#e2e8f0', fontSize: '12px', fontFamily, padding: '6px 10px', cursor: 'pointer', outline: 'none', minWidth: '200px' }}>
            {estadosAlfa.map((d) => (<option key={d.estado} value={d.estado}>{d.estado}</option>))}
          </select>
        </div>
        {/* Badge del estado seleccionado */}
        {selectedData && (
          <div style={{ background: `${highlightColor}15`, border: `1px solid ${highlightColor}`, borderRadius: '8px', padding: '10px 20px', textAlign: 'center' }}>
            <p style={{ fontSize: '11px', color: highlightColor, margin: '0 0 4px 0', textTransform: 'uppercase', letterSpacing: '0.06em' }}>{selectedData.estado}</p>
            <p style={{ fontSize: '20px', color: '#e2e8f0', margin: 0, fontWeight: 700, fontFamily }}>${fmt(selectedData.mdd)} MDD</p>
            <p style={{ fontSize: '10px', color: '#64748b', margin: '2px 0 0 0' }}>Posición #{chartData.findIndex((d) => d.estado === selected) + 1} de 32</p>
          </div>
        )}
      </div>

      {/* Gráfica */}
      <div ref={chartRef}>
        <ResponsiveContainer width="100%" height={380}>
          <BarChart data={chartData} margin={{ top: 24, right: 16, left: 0, bottom: 60 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
            <XAxis dataKey="estado" tick={{ fontSize: 8, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} angle={-45} textAnchor="end" interval={0} />
            <YAxis domain={[0, yMax]} tickFormatter={(v) => `$${fmt(v)}`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number) => [`$${fmt(v)} MDD`, 'IED']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
            <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '24px' }} formatter={() => 'IED 2024 (Millones de USD)'} />
            <Bar dataKey="mdd" radius={[3, 3, 0, 0]} name="IED">
              {chartData.map((d) => (<Cell key={d.estado} fill={d.estado === selected ? highlightColor : defaultColor} />))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>Fuente: Secretaría de Economía — Comisión Nacional de Inversiones Extranjeras</p>

      {indicador && <AnalisisIA graficaId={indicador.id} />}
      {indicador && <AnalisisRevisado graficaId={indicador.id} />}
    </div>
  )
}
