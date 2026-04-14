'use client'

import { useRef, type RefObject } from 'react'
import { PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer } from 'recharts'
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

function fmtB(v: number): string {
  return '$' + new Intl.NumberFormat('es-MX', { maximumFractionDigits: 0 }).format(v / 1e6) + ' B'
}

export default function PIBSectorChart() {
  const { palette, fontFamily, titleSize, xAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)

  const { data: indicadores } = useIndicadores('nacional', 'economia')
  const indicador = indicadores?.find((i) => i.clave === 'inegi.pib_sector')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>Cargando PIB por sector...</div>)
  }
  if (!datos.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>Sin datos disponibles.</div>)
  }

  const chartData = datos
    .filter((d) => d.entidad_clave)
    .map((d) => ({ name: d.entidad_clave ?? '', value: Number(d.valor) }))
    .sort((a, b) => b.value - a.value)
  const total = chartData.reduce((s, d) => s + d.value, 0)
  const fmt = (v: number) => new Intl.NumberFormat('es-MX', { maximumFractionDigits: 0 }).format(v)

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>PIB por Sector Económico (2024)</p>
        <button onClick={() => downloadChartAsPng(chartRef, 'pib-sector-economico')} title="Descargar" style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>↓ PNG</button>
      </div>
      <p style={{ textAlign: 'center', fontSize: '13px', color: '#94a3b8', fontFamily, margin: 0 }}>Total: <strong style={{ color: '#e2e8f0' }}>${fmt(total)} millones MXN</strong></p>
      <div ref={chartRef}>
        <ResponsiveContainer width="100%" height={360}>
          <PieChart>
            <Pie data={chartData} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={80} outerRadius={130} paddingAngle={3}
              label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`} labelLine={{ stroke: '#4a5568' }}>
              {chartData.map((_, i) => (<Cell key={i} fill={getColorForIndex(palette, i)} />))}
            </Pie>
            <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
              formatter={(v: number, name: string) => [`$${fmt(v)} M (${total > 0 ? ((v / total) * 100).toFixed(1) : 0}%)`, name]}
              labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
            <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
          </PieChart>
        </ResponsiveContainer>
      </div>
      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>Fuente: INEGI — Cuentas Nacionales / DataMéxico</p>
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead><tr style={{ borderBottom: '1px solid #2d3148' }}>
            <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Sector</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>PIB (M MXN)</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>%</th>
          </tr></thead>
          <tbody>
            {chartData.map((d) => (
              <tr key={d.name} style={{ borderBottom: '1px solid #1e2235' }}>
                <td style={{ padding: '5px 8px' }}>{d.name}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>${fmt(d.value)}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{total > 0 ? ((d.value / total) * 100).toFixed(1) + '%' : '—'}</td>
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
