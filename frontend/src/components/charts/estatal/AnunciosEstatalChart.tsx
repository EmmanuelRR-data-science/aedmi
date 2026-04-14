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
  return new Intl.NumberFormat('es-MX', { maximumFractionDigits: 0 }).format(v)
}

interface Props { estado: string }

export default function AnunciosEstatalChart({ estado }: Props) {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)

  const { data: indicadores } = useIndicadores('estatal', 'economia')
  const indicador = indicadores?.find((i) => i.clave === 'se.anuncios_estatal')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>Cargando anuncios de inversión...</div>)
  }

  const edoDatos = datos.filter((d) => d.entidad_clave?.endsWith(`:${estado}`))

  if (!edoDatos.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '40px', paddingBottom: '40px' }}>Sin datos de inversión para {estado}.</div>)
  }

  const porAnio: Record<string, { mdd: number; anuncios: number }> = {}
  edoDatos.forEach((d) => {
    const anio = String(d.periodo)
    if (!porAnio[anio]) porAnio[anio] = { mdd: 0, anuncios: 0 }
    if (d.entidad_clave?.startsWith('inv_mdd:')) porAnio[anio].mdd = Number(d.valor)
    if (d.entidad_clave?.startsWith('inv_num:')) porAnio[anio].anuncios = Number(d.valor)
  })

  const chartData = Object.entries(porAnio).sort(([a], [b]) => Number(a) - Number(b)).map(([anio, v]) => ({ anio, ...v }))
  const maxMdd = chartData.length ? Math.max(...chartData.map((d) => d.mdd)) : 0
  const yMaxMdd = Math.ceil(maxMdd * 1.15)
  const maxAnun = chartData.length ? Math.max(...chartData.map((d) => d.anuncios)) : 0
  const yMaxAnun = Math.ceil(maxAnun * 1.15)

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          Anuncios de Inversión — {estado}
        </p>
        <button onClick={() => downloadChartAsPng(chartRef, `anuncios-inversion-${estado.toLowerCase().replace(/ /g, '-')}`)} title="Descargar"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>↓ PNG</button>
      </div>

      <div ref={chartRef}>
        <ResponsiveContainer width="100%" height={320}>
          <ComposedChart data={chartData} margin={{ top: 24, right: 50, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
            <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <YAxis yAxisId="left" domain={[0, yMaxMdd]} tickFormatter={(v) => `$${fmt(v)}`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <YAxis yAxisId="right" orientation="right" domain={[0, yMaxAnun]} tickFormatter={fmt} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
            <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
            <Bar yAxisId="left" dataKey="mdd" fill={palette[0]} radius={[3, 3, 0, 0]} name="Monto (MDD)" />
            <Line yAxisId="right" type="monotone" dataKey="anuncios" stroke={palette[2]} strokeWidth={2} dot={{ r: 4, fill: palette[2] }} name="Nº Anuncios" />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
        Fuente: DataMéxico (Secretaría de Economía) — Anuncios de inversión
      </p>

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead><tr style={{ borderBottom: '1px solid #2d3148' }}>
            <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Monto (MDD)</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Anuncios</th>
          </tr></thead>
          <tbody>
            {chartData.map((d) => (
              <tr key={d.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                <td style={{ padding: '5px 8px' }}>{d.anio}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>${fmt(d.mdd)}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(d.anuncios)}</td>
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
