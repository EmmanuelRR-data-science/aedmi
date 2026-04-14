'use client'

import { useRef, useState, type RefObject } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, PieChart, Pie, Cell,
} from 'recharts'
import { useStyleConfig, getColorForIndex } from '@/hooks/useStyleConfig'
import { useIndicadores, useIndicadorDatos } from '@/hooks/useIndicador'
import AnalisisIA from '@/components/ai/AnalisisIA'
import AnalisisRevisado from '@/components/ai/AnalisisRevisado'
import type { DatoIndicador } from '@/types'

interface Props { estado: string }
type Tab = 'evolucion' | 'ranking'

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

export default function AeropuertosEstatalChart({ estado }: Props) {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)
  const [tab, setTab] = useState<Tab>('evolucion')

  const { data: indicadores } = useIndicadores('estatal', 'conectividad_aerea')
  const indicador = indicadores?.find((i) => i.clave === 'afac.aeropuertos_estatal')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>Cargando datos aeroportuarios...</div>)
  }
  if (!datos.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>Sin datos aeroportuarios disponibles.</div>)
  }

  const aptoRows = datos
    .filter((d) => d.entidad_clave?.startsWith('apto:'))
    .map((d) => {
      const parts = (d.entidad_clave ?? '').split(':')
      if (parts.length < 3) return null
      return {
        aeropuerto: parts[1] ?? '',
        estado: parts.slice(2).join(':'),
        anio: Number(d.periodo),
        value: Number(d.valor),
      }
    })
    .filter((r): r is { aeropuerto: string; estado: string; anio: number; value: number } => Boolean(r))

  const estadoRows = aptoRows.filter((r) => r.estado === estado)
  const hasAirports = estadoRows.length > 0

  const byYear: Record<string, Record<string, number | string>> = {}
  estadoRows.forEach((row) => {
    const y = String(row.anio)
    if (!byYear[y]) byYear[y] = { anio: y }
    byYear[y][row.aeropuerto] = row.value
  })

  const latestYear = estadoRows.length ? Math.max(...estadoRows.map((r) => r.anio)) : 0
  const rankingRows = estadoRows
    .filter((r) => r.anio === latestYear)
    .sort((a, b) => b.value - a.value)

  const airportNames = [...new Set(estadoRows.map((r) => r.aeropuerto))]
    .sort((a, b) => {
      const va = rankingRows.find((r) => r.aeropuerto === a)?.value ?? 0
      const vb = rankingRows.find((r) => r.aeropuerto === b)?.value ?? 0
      return vb - va
    })

  const evolucionData = Object.values(byYear)
    .sort((a, b) => Number(a.anio) - Number(b.anio))
    .slice(-5)
  const maxEvo = evolucionData.length
    ? Math.max(...evolucionData.flatMap((row) => airportNames.map((n) => Number(row[n] ?? 0))))
    : 0
  const yMaxEvo = Math.ceil(maxEvo * 1.15)

  const pieData = rankingRows.map((r) => ({ name: r.aeropuerto, value: r.value }))
  const titles: Record<Tab, string> = {
    evolucion: `Aeropuertos — Evolución (${estado})`,
    ranking: `Aeropuertos — Ranking actual (${estado}, ${latestYear || 'N/A'})`,
  }

  const currentTableRows = tab === 'evolucion'
    ? evolucionData.map((row) => ({
      col1: String(row.anio),
      col2: airportNames.map((name) => `${name}: ${fmt(Number(row[name] ?? 0))}`).join(' | '),
    }))
    : pieData.map((row) => ({
      col1: row.name,
      col2: `${fmt(row.value)} operaciones`,
    }))

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>{titles[tab]}</p>
        <button onClick={() => downloadChartAsPng(chartRef, `aeropuertos-estatal-${tab}-${estado.toLowerCase().replace(/ /g, '-')}`)} title="Descargar"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>↓ PNG</button>
      </div>

      <div style={{ display: 'flex', gap: '6px', justifyContent: 'center', flexWrap: 'wrap' }}>
        {([['evolucion', 'Evolución de aeropuertos'], ['ranking', 'Ranking actual']] as [Tab, string][]).map(([t, l]) => (
          <button key={t} onClick={() => setTab(t)} style={{ padding: '6px 16px', fontSize: '12px', fontFamily, borderRadius: '4px', cursor: 'pointer', background: t === tab ? palette[0] : 'transparent', color: t === tab ? '#fff' : '#94a3b8', border: t === tab ? 'none' : '1px solid #2d3148' }}>{l}</button>
        ))}
      </div>

      {!hasAirports && (
        <p style={{ fontSize: '12px', color: '#94a3b8', fontFamily, margin: '0', textAlign: 'left' }}>
          {estado} no cuenta con aeropuertos registrados en esta fuente.
        </p>
      )}

      {hasAirports && (
        <div ref={chartRef}>
          {tab === 'evolucion' && (
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={evolucionData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                <YAxis domain={[0, yMaxEvo]} tickFormatter={(v) => fmt(v)} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number, n: string) => [`${fmt(v)} operaciones`, n]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                {airportNames.map((name, idx) => (
                  <Bar key={name} dataKey={name} fill={getColorForIndex(palette, idx)} radius={[3, 3, 0, 0]} name={name} />
                ))}
              </BarChart>
            </ResponsiveContainer>
          )}

          {tab === 'ranking' && (
            <ResponsiveContainer width="100%" height={360}>
              <PieChart>
                <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={70} outerRadius={130} paddingAngle={3}
                  label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`} labelLine={{ stroke: '#4a5568' }}>
                  {pieData.map((_, i) => (<Cell key={i} fill={getColorForIndex(palette, i)} />))}
                </Pie>
                <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number, n: string) => [`${fmt(v)} operaciones`, n]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
              </PieChart>
            </ResponsiveContainer>
          )}
        </div>
      )}

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
        Fuente: DGAC / AFAC (serie referencial sintética para desarrollo) — Operaciones por aeropuerto.
      </p>

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead><tr style={{ borderBottom: '1px solid #2d3148' }}>
            <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>{tab === 'evolucion' ? 'Año' : 'Aeropuerto'}</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>{tab === 'evolucion' ? 'Operaciones por aeropuerto' : 'Operaciones'}</th>
          </tr></thead>
          <tbody>
            {currentTableRows.map((row) => (
              <tr key={row.col1} style={{ borderBottom: '1px solid #1e2235' }}>
                <td style={{ padding: '5px 8px' }}>{row.col1}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.col2}</td>
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
