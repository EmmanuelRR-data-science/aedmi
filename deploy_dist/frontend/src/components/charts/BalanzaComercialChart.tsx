'use client'

import { useRef, useState, type RefObject } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, Cell, PieChart, Pie,
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
  return new Intl.NumberFormat('es-MX', { maximumFractionDigits: 1 }).format(v)
}

type Tab = 'monto' | 'productos' | 'composicion'

export default function BalanzaComercialChart() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)
  const [tab, setTab] = useState<Tab>('monto')

  const { data: indicadores } = useIndicadores('nacional', 'economia')
  const indicador = indicadores?.find((i) => i.clave === 'inegi.balanza_comercial')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>Cargando balanza comercial...</div>)
  }
  if (!datos.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>Sin datos disponibles.</div>)
  }

  // Balanza anual: agrupar exp/imp por año
  const balAnual: Record<string, { exp: number; imp: number }> = {}
  datos.filter((d) => d.entidad_clave?.startsWith('bal:')).forEach((d) => {
    const anio = String(d.periodo)
    if (!balAnual[anio]) balAnual[anio] = { exp: 0, imp: 0 }
    if (d.entidad_clave?.includes('exp')) balAnual[anio].exp = Number(d.valor)
    if (d.entidad_clave?.includes('imp')) balAnual[anio].imp = Number(d.valor)
  })
  const balData = Object.entries(balAnual).sort(([a], [b]) => Number(a) - Number(b)).map(([anio, v]) => ({ anio, ...v }))

  const prodData = datos.filter((d) => d.entidad_clave?.startsWith('prod:')).map((d) => ({ name: d.entidad_clave?.replace('prod:', '') ?? '', value: Number(d.valor) })).sort((a, b) => b.value - a.value)

  const compData = datos.filter((d) => d.entidad_clave?.startsWith('comp:')).map((d) => ({ name: d.entidad_clave?.replace('comp:', '') ?? '', value: Number(d.valor) })).sort((a, b) => b.value - a.value)

  const titles: Record<Tab, string> = { monto: 'Balanza Comercial (Miles de Millones USD)', productos: 'Top 10 Productos Exportados (2024)', composicion: 'Composición de Exportaciones (2024)' }
  const maxBal = balData.length ? Math.max(...balData.flatMap((d) => [d.exp, d.imp])) : 0
  const yMaxBal = Math.ceil(maxBal * 1.15)
  const maxProd = prodData.length ? Math.max(...prodData.map((d) => d.value)) : 0
  const xMaxProd = Math.ceil(maxProd * 1.15)

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>{titles[tab]}</p>
        <button onClick={() => downloadChartAsPng(chartRef, `balanza-comercial-${tab}`)} title="Descargar" style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>↓ PNG</button>
      </div>
      <div style={{ display: 'flex', gap: '6px', justifyContent: 'center' }}>
        {([['monto', 'Monto Anual'], ['productos', 'Top 10 Productos'], ['composicion', 'Composición']] as [Tab, string][]).map(([t, l]) => (
          <button key={t} onClick={() => setTab(t)} style={{ padding: '6px 16px', fontSize: '12px', fontFamily, borderRadius: '4px', cursor: 'pointer', background: t === tab ? palette[0] ?? palette[1] : 'transparent', color: t === tab ? '#fff' : '#94a3b8', border: t === tab ? 'none' : '1px solid #2d3148' }}>{l}</button>
        ))}
      </div>
      <div ref={chartRef}>
        {tab === 'monto' && (
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={balData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis domain={[0, yMaxBal]} tickFormatter={(v) => `$${fmt(v)}`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number, n: string) => [`$${fmt(v)} B USD`, n]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
              <Bar dataKey="exp" fill={palette[0] ?? palette[1]} radius={[3, 3, 0, 0]} name="Exportaciones" />
              <Bar dataKey="imp" fill={palette[1] ?? palette[0]} radius={[3, 3, 0, 0]} name="Importaciones" />
            </BarChart>
          </ResponsiveContainer>
        )}
        {tab === 'productos' && (
          <ResponsiveContainer width="100%" height={Math.max(320, prodData.length * 40)}>
            <BarChart data={prodData} layout="vertical" margin={{ top: 8, right: 60, left: 10, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" horizontal={false} />
              <XAxis type="number" domain={[0, xMaxProd]} tickFormatter={(v) => `$${fmt(v)}`} tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis type="category" dataKey="name" width={180} tick={{ fontSize: yAxisSize - 1, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number) => [`$${fmt(v)} B USD`, 'Exportaciones']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Bar dataKey="value" radius={[0, 3, 3, 0]} name="Exportaciones">
                {prodData.map((_, i) => (<Cell key={i} fill={getColorForIndex(palette, i)} />))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
        {tab === 'composicion' && (
          <ResponsiveContainer width="100%" height={360}>
            <PieChart>
              <Pie data={compData} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={70} outerRadius={130} paddingAngle={3}
                label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`} labelLine={{ stroke: '#4a5568' }}>
                {compData.map((_, i) => (<Cell key={i} fill={getColorForIndex(palette, i)} />))}
              </Pie>
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number, n: string) => [`${v.toFixed(1)}%`, n]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
            </PieChart>
          </ResponsiveContainer>
        )}
      </div>
      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>Fuente: INEGI / DataMéxico — Comercio exterior de México</p>
      {indicador && <AnalisisIA graficaId={indicador.id} />}
      {indicador && <AnalisisRevisado graficaId={indicador.id} />}
    </div>
  )
}
