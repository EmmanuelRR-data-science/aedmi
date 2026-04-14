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

type Tab = 'nacional' | 'estados' | 'dona'

export default function AnunciosInversionChart() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)
  const [tab, setTab] = useState<Tab>('nacional')

  const { data: indicadores } = useIndicadores('nacional', 'economia')
  const indicador = indicadores?.find((i) => i.clave === 'se.anuncios_inversion')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', height: '320px', display: 'flex', alignItems: 'center',
        justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>
        Cargando anuncios de inversión...
      </div>
    )
  }

  if (!datos.length) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily,
        textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>
        Sin datos disponibles para Anuncios de Inversión.
      </div>
    )
  }

  // Nacional: monto por año
  const nacMonto = datos
    .filter((d) => d.entidad_clave === 'nac:monto')
    .map((d) => ({ anio: String(d.periodo), mdd: Number(d.valor) }))
    .sort((a, b) => Number(a.anio) - Number(b.anio))

  const nacAnuncios = datos
    .filter((d) => d.entidad_clave === 'nac:anuncios')
    .map((d) => ({ anio: String(d.periodo), count: Number(d.valor) }))
    .sort((a, b) => Number(a.anio) - Number(b.anio))

  // Estados
  const edoData = datos
    .filter((d) => d.entidad_clave?.startsWith('edo:'))
    .map((d) => ({ name: d.entidad_clave?.replace('edo:', '') ?? '', value: Number(d.valor) }))
    .sort((a, b) => b.value - a.value)

  const titles: Record<Tab, string> = {
    nacional: 'Anuncios de Inversión — Nacional por Año',
    estados: 'Top 10 Estados por IED (2024)',
    dona: 'Composición de IED por Estado (2024)',
  }

  const maxMdd = nacMonto.length ? Math.max(...nacMonto.map((d) => d.mdd)) : 0
  const yMaxMdd = Math.ceil(maxMdd * 1.15)
  const maxEdo = edoData.length ? Math.max(...edoData.map((d) => d.value)) : 0
  const xMaxEdo = Math.ceil(maxEdo * 1.15)
  const totalEdo = edoData.reduce((s, d) => s + d.value, 0)

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
      padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          {titles[tab]}
        </p>
        <button onClick={() => downloadChartAsPng(chartRef, `anuncios-inversion-${tab}`)}
          title="Descargar gráfica en alta resolución"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148',
            borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px',
            cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>
          ↓ PNG
        </button>
      </div>

      <div style={{ display: 'flex', gap: '6px', justifyContent: 'center' }}>
        {([['nacional', 'Nacional'], ['estados', 'Top 10 Estados'], ['dona', 'Dona por Estado']] as [Tab, string][]).map(([t, label]) => (
          <button key={t} onClick={() => setTab(t)}
            style={{
              padding: '6px 16px', fontSize: '12px', fontFamily, borderRadius: '4px', cursor: 'pointer',
              background: t === tab ? palette[0] ?? palette[1] : 'transparent',
              color: t === tab ? '#fff' : '#94a3b8',
              border: t === tab ? 'none' : '1px solid #2d3148',
            }}>
            {label}
          </button>
        ))}
      </div>

      <div ref={chartRef}>
        {tab === 'nacional' && (
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={nacMonto} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis domain={[0, yMaxMdd]} tickFormatter={(v) => `$${fmt(v)}`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
                formatter={(v: number) => [`$${fmt(v)} MDD`, 'IED']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }}
                formatter={() => 'IED (Miles de MDD)'} />
              <Bar dataKey="mdd" fill={palette[0] ?? palette[1]} radius={[3, 3, 0, 0]} name="IED" />
            </BarChart>
          </ResponsiveContainer>
        )}
        {tab === 'estados' && (
          <ResponsiveContainer width="100%" height={Math.max(320, edoData.length * 40)}>
            <BarChart data={edoData} layout="vertical" margin={{ top: 8, right: 60, left: 10, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" horizontal={false} />
              <XAxis type="number" domain={[0, xMaxEdo]} tickFormatter={(v) => `$${fmt(v)}`} tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis type="category" dataKey="name" width={140} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
                formatter={(v: number) => [`$${fmt(v)} MDD`, 'IED']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Bar dataKey="value" radius={[0, 3, 3, 0]} name="IED">
                {edoData.map((_, i) => (<Cell key={i} fill={getColorForIndex(palette, i)} />))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
        {tab === 'dona' && (
          <ResponsiveContainer width="100%" height={380}>
            <PieChart>
              <Pie data={edoData} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={70} outerRadius={130} paddingAngle={2}
                label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`} labelLine={{ stroke: '#4a5568' }}>
                {edoData.map((_, i) => (<Cell key={i} fill={getColorForIndex(palette, i)} />))}
              </Pie>
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
                formatter={(v: number, name: string) => [`$${fmt(v)} MDD (${totalEdo > 0 ? ((v / totalEdo) * 100).toFixed(1) : 0}%)`, name]}
                labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
            </PieChart>
          </ResponsiveContainer>
        )}
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
        Fuente: DataMéxico (Secretaría de Economía) — API fdi_10_year_country
      </p>

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #2d3148' }}>
              {tab === 'nacional' ? (
                <>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>IED (MDD)</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Anuncios</th>
                </>
              ) : (
                <>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Estado</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>IED (MDD)</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>% del total</th>
                </>
              )}
            </tr>
          </thead>
          <tbody>
            {tab === 'nacional' ? nacMonto.map((d) => {
              const anuncios = nacAnuncios.find((a) => a.anio === d.anio)
              return (
                <tr key={d.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                  <td style={{ padding: '5px 8px' }}>{d.anio}</td>
                  <td style={{ padding: '5px 8px', textAlign: 'right' }}>${fmt(d.mdd)}</td>
                  <td style={{ padding: '5px 8px', textAlign: 'right' }}>{anuncios ? fmt(anuncios.count) : '—'}</td>
                </tr>
              )
            }) : edoData.map((d) => (
              <tr key={d.name} style={{ borderBottom: '1px solid #1e2235' }}>
                <td style={{ padding: '5px 8px' }}>{d.name}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>${fmt(d.value)}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{totalEdo > 0 ? ((d.value / totalEdo) * 100).toFixed(1) + '%' : '—'}</td>
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
