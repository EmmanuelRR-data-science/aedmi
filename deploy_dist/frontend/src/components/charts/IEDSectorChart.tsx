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

export default function IEDSectorChart() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)
  const [selectedYear, setSelectedYear] = useState<number>(2024)

  const { data: indicadores } = useIndicadores('nacional', 'economia')
  const indicador = indicadores?.find((i) => i.clave === 'se.ied_sector')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', height: '320px', display: 'flex', alignItems: 'center',
        justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>
        Cargando datos de IED...
      </div>
    )
  }

  if (!datos.length) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily,
        textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>
        Sin datos disponibles para IED por Sector.
      </div>
    )
  }

  // Obtener años disponibles
  const years = [...new Set(datos.map((d) => Number(d.periodo)))].sort((a, b) => b - a)

  // Filtrar por año seleccionado y ordenar de mayor a menor
  const chartData = datos
    .filter((d) => Number(d.periodo) === selectedYear && d.entidad_clave)
    .map((d) => ({ sector: d.entidad_clave ?? '', mdd: Number(d.valor) }))
    .sort((a, b) => b.mdd - a.mdd)

  const maxValor = Math.max(...chartData.map((d) => d.mdd))
  const xMax = Math.ceil(maxValor * 1.15)

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
      padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          Inversión Extranjera Directa por Sector ({selectedYear})
        </p>
        <button onClick={() => downloadChartAsPng(chartRef, `ied-sector-${selectedYear}`)}
          title="Descargar gráfica en alta resolución"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148',
            borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px',
            cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>
          ↓ PNG
        </button>
      </div>

      {/* Selector de año */}
      <div style={{ display: 'flex', gap: '6px', justifyContent: 'center' }}>
        {years.map((y) => (
          <button key={y} onClick={() => setSelectedYear(y)}
            style={{
              padding: '4px 14px', fontSize: '12px', fontFamily, borderRadius: '4px', cursor: 'pointer',
              background: y === selectedYear ? palette[0] ?? palette[1] : 'transparent',
              color: y === selectedYear ? '#fff' : '#94a3b8',
              border: y === selectedYear ? 'none' : '1px solid #2d3148',
            }}>
            {y}
          </button>
        ))}
      </div>

      {/* Gráfica horizontal */}
      <div ref={chartRef}>
        <ResponsiveContainer width="100%" height={Math.max(320, chartData.length * 40)}>
          <BarChart data={chartData} layout="vertical" margin={{ top: 8, right: 60, left: 10, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" horizontal={false} />
            <XAxis type="number" domain={[0, xMax]} tickFormatter={(v) => `$${fmt(v)}`}
              tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }}
              axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <YAxis type="category" dataKey="sector" width={200}
              tick={{ fontSize: yAxisSize - 1, fontFamily, fill: '#94a3b8' }}
              axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
              formatter={(v: number) => [`$${fmt(v)} MDD`, 'IED']}
              labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
            <Legend verticalAlign="bottom" align="center"
              wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }}
              formatter={() => `IED ${selectedYear} (Millones de dólares)`} />
            <Bar dataKey="mdd" radius={[0, 3, 3, 0]} name="IED">
              {chartData.map((_, i) => (
                <Cell key={i} fill={getColorForIndex(palette, i)} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
        Fuente: Secretaría de Economía — Comisión Nacional de Inversiones Extranjeras
      </p>

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #2d3148' }}>
              <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Sector</th>
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>IED (MDD)</th>
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>% del total</th>
            </tr>
          </thead>
          <tbody>
            {(() => {
              const total = chartData.reduce((s, d) => s + d.mdd, 0)
              return chartData.map((d) => (
                <tr key={d.sector} style={{ borderBottom: '1px solid #1e2235' }}>
                  <td style={{ padding: '5px 8px' }}>{d.sector}</td>
                  <td style={{ padding: '5px 8px', textAlign: 'right' }}>${fmt(d.mdd)}</td>
                  <td style={{ padding: '5px 8px', textAlign: 'right' }}>{total > 0 ? ((d.mdd / total) * 100).toFixed(1) + '%' : '—'}</td>
                </tr>
              ))
            })()}
          </tbody>
        </table>
      </div>

      {indicador && <AnalisisIA graficaId={indicador.id} />}
      {indicador && <AnalisisRevisado graficaId={indicador.id} />}
    </div>
  )
}
