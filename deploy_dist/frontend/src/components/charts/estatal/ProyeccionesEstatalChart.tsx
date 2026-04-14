'use client'

import { useRef, type RefObject } from 'react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
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

function fmtM(v: number): string {
  return (v / 1e6).toFixed(2) + ' M'
}

interface Props { estado: string }

export default function ProyeccionesEstatalChart({ estado }: Props) {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)

  const { data: indicadores } = useIndicadores('estatal', 'demografia')
  const indicador = indicadores?.find((i) => i.clave === 'conapo.proyecciones_estatal')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>Cargando proyecciones...</div>)
  }

  // Filtrar por estado
  const edoDatos = datos.filter((d) => d.entidad_clave?.endsWith(`:${estado}`))

  if (!edoDatos.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '40px', paddingBottom: '40px' }}>Sin proyecciones disponibles para {estado}.</div>)
  }

  // Agrupar por año
  const porAnio: Record<string, { total: number; hombres: number; mujeres: number }> = {}
  edoDatos.forEach((d) => {
    const anio = String(d.periodo)
    if (!porAnio[anio]) porAnio[anio] = { total: 0, hombres: 0, mujeres: 0 }
    if (d.entidad_clave?.startsWith('proy_total:')) porAnio[anio].total = Number(d.valor)
    if (d.entidad_clave?.startsWith('proy_h:')) porAnio[anio].hombres = Number(d.valor)
    if (d.entidad_clave?.startsWith('proy_m:')) porAnio[anio].mujeres = Number(d.valor)
  })

  const chartData = Object.entries(porAnio)
    .sort(([a], [b]) => Number(a) - Number(b))
    .map(([anio, v]) => ({ anio, ...v }))

  const maxVal = chartData.length ? Math.max(...chartData.map((d) => d.total)) : 0
  const yMax = Math.ceil(maxVal * 1.15)

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          Proyecciones de Población — {estado} (2026-2030)
        </p>
        <button onClick={() => downloadChartAsPng(chartRef, `proyecciones-${estado.toLowerCase().replace(/ /g, '-')}`)} title="Descargar"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>↓ PNG</button>
      </div>

      <div ref={chartRef}>
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={chartData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
            <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <YAxis domain={['auto', yMax]} tickFormatter={fmtM} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
              formatter={(v: number, n: string) => [fmt(v) + ' personas', n]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
            <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
            <Line key={`t-${palette[0]}`} type="monotone" dataKey="total" stroke={palette[0]} strokeWidth={2.5} dot={{ r: 4, fill: palette[0] }} name="Total" />
            <Line key={`h-${palette[1]}`} type="monotone" dataKey="hombres" stroke={palette[1]} strokeWidth={1.5} dot={{ r: 3, fill: palette[1] }} strokeDasharray="5 3" name="Hombres" />
            <Line key={`m-${palette[2]}`} type="monotone" dataKey="mujeres" stroke={palette[2]} strokeWidth={1.5} dot={{ r: 3, fill: palette[2] }} strokeDasharray="5 3" name="Mujeres" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
        Fuente: CONAPO — Proyecciones de Población de México y Entidades Federativas 2020-2070
      </p>

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead><tr style={{ borderBottom: '1px solid #2d3148' }}>
            <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Total</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Hombres</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Mujeres</th>
          </tr></thead>
          <tbody>
            {chartData.map((d) => (
              <tr key={d.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                <td style={{ padding: '5px 8px' }}>{d.anio}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(d.total)}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(d.hombres)}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(d.mujeres)}</td>
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
