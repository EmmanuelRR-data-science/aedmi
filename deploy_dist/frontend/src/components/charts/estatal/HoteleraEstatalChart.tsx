'use client'

import { useRef, type RefObject } from 'react'
import {
  ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
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
  return new Intl.NumberFormat('es-MX', { maximumFractionDigits: 1 }).format(v)
}

const MESES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']

interface Props { estado: string }

export default function HoteleraEstatalChart({ estado }: Props) {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)

  const { data: indicadores } = useIndicadores('estatal', 'turismo')
  const indicador = indicadores?.find((i) => i.clave === 'sectur.hotelera_estatal')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>Cargando actividad hotelera...</div>)
  }

  const edoDatos = datos.filter((d) => d.entidad_clave?.endsWith(`:${estado}`))

  if (!edoDatos.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '40px', paddingBottom: '40px' }}>Sin datos hoteleros para {estado}.</div>)
  }

  // Agrupar por mes
  const porMes: Record<string, { disp: number; ocup: number; pct: number }> = {}
  edoDatos.forEach((d) => {
    const parts = String(d.periodo).split('-')
    const mes = parseInt(parts[1] ?? '1')
    const label = MESES[mes - 1] ?? String(mes)
    if (!porMes[label]) porMes[label] = { disp: 0, ocup: 0, pct: 0 }
    if (d.entidad_clave?.startsWith('hotel_disp:')) porMes[label].disp = Number(d.valor)
    if (d.entidad_clave?.startsWith('hotel_ocup:')) porMes[label].ocup = Number(d.valor)
    if (d.entidad_clave?.startsWith('hotel_pct:')) porMes[label].pct = Number(d.valor)
  })

  const chartData = MESES.filter((m) => porMes[m]).map((m) => ({ mes: m, ...porMes[m] }))
  const maxDisp = chartData.length ? Math.max(...chartData.map((d) => d.disp)) : 0
  const yMax = Math.ceil(maxDisp * 1.15)

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          Actividad Hotelera — {estado} (2025)
        </p>
        <button onClick={() => downloadChartAsPng(chartRef, `hotelera-${estado.toLowerCase().replace(/ /g, '-')}`)} title="Descargar"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>↓ PNG</button>
      </div>

      <div ref={chartRef}>
        <ResponsiveContainer width="100%" height={320}>
          <ComposedChart data={chartData} margin={{ top: 24, right: 50, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
            <XAxis dataKey="mes" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <YAxis yAxisId="left" domain={[0, yMax]} tickFormatter={(v) => fmt(v)} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <YAxis yAxisId="right" orientation="right" domain={[0, 100]} tickFormatter={(v) => `${v}%`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
            <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
            <Bar yAxisId="left" dataKey="disp" fill={palette[0]} radius={[3, 3, 0, 0]} name="Disponibles (miles)" />
            <Bar yAxisId="left" dataKey="ocup" fill={palette[1]} radius={[3, 3, 0, 0]} name="Ocupados (miles)" />
            <Line yAxisId="right" type="monotone" dataKey="pct" stroke={palette[2]} strokeWidth={2} dot={{ r: 3, fill: palette[2] }} name="% Ocupación" />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
        Fuente: Datatur / SECTUR — Estadísticas de ocupación hotelera
      </p>

      {indicador && <AnalisisIA graficaId={indicador.id} />}
      {indicador && <AnalisisRevisado graficaId={indicador.id} />}
    </div>
  )
}
