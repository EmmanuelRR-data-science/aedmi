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
type Tab = 'evolucion' | 'peso' | 'ranking'

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

export default function ExportacionesEstatalChart({ estado }: Props) {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)
  const [tab, setTab] = useState<Tab>('evolucion')

  const { data: indicadores } = useIndicadores('estatal', 'economia')
  const indicador = indicadores?.find((i) => i.clave === 'se.exportaciones_estatal')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>Cargando exportaciones estatales...</div>)
  }

  if (!datos.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>Sin datos de exportaciones disponibles.</div>)
  }

  const allExp = datos
    .filter((d) => d.entidad_clave?.startsWith('exp_usd:'))
    .map((d) => ({
      estado: d.entidad_clave?.replace('exp_usd:', '') ?? '',
      anio: Number(d.periodo),
      value: Number(d.valor),
    }))
    .filter((d) => Number.isFinite(d.value))

  if (!allExp.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>Sin series de exportación válidas para este indicador.</div>)
  }

  const evolucionData = allExp
    .filter((d) => d.estado === estado)
    .sort((a, b) => a.anio - b.anio)
    .slice(-5)
    .map((d) => ({ anio: String(d.anio), value: d.value }))

  if (!evolucionData.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>Sin datos de exportación para {estado}.</div>)
  }

  const latestYear = Math.max(...allExp.map((d) => d.anio))
  const latestData = allExp
    .filter((d) => d.anio === latestYear)
    .sort((a, b) => b.value - a.value)
  const totalNacional = latestData.reduce((acc, d) => acc + d.value, 0)
  const selectedLatest = latestData.find((d) => d.estado === estado)?.value ?? 0
  const resto = Math.max(0, totalNacional - selectedLatest)

  const pieData = [
    { name: estado, value: selectedLatest },
    { name: 'Resto del país', value: resto },
  ]

  const rankingTop10 = latestData.slice(0, 10)
  const selectedRank = latestData.findIndex((d) => d.estado === estado) + 1

  const titles: Record<Tab, string> = {
    evolucion: `Exportaciones — Evolución estatal (${estado})`,
    peso: `Exportaciones — Peso nacional (${estado}, ${latestYear})`,
    ranking: `Exportaciones — Ranking estatal (${latestYear})`,
  }

  const maxEvo = Math.max(...evolucionData.map((d) => d.value))
  const yMaxEvo = Math.ceil(maxEvo * 1.15)
  const maxRank = rankingTop10.length ? Math.max(...rankingTop10.map((d) => d.value)) : 0
  const xMaxRank = Math.ceil(maxRank * 1.15)

  const evolVar = evolucionData.length > 1
    ? ((evolucionData[evolucionData.length - 1].value - evolucionData[0].value) / evolucionData[0].value) * 100
    : 0

  let currentTableRows: Array<{ col1: string; col2: string }>
  if (tab === 'evolucion') {
    currentTableRows = evolucionData.map((d) => ({
      col1: d.anio,
      col2: `$${fmt(d.value)} M USD`,
    }))
  } else if (tab === 'peso') {
    const pctEstado = totalNacional > 0 ? (selectedLatest / totalNacional) * 100 : 0
    const pctResto = 100 - pctEstado
    currentTableRows = [
      { col1: estado, col2: `${pctEstado.toFixed(2)}%` },
      { col1: 'Resto del país', col2: `${pctResto.toFixed(2)}%` },
    ]
  } else {
    currentTableRows = rankingTop10.map((d, idx) => ({
      col1: `${idx + 1}. ${d.estado}`,
      col2: `$${fmt(d.value)} M USD`,
    }))
  }

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>{titles[tab]}</p>
        <button onClick={() => downloadChartAsPng(chartRef, `exportaciones-estatal-${tab}-${estado.toLowerCase().replace(/ /g, '-')}`)} title="Descargar"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>↓ PNG</button>
      </div>

      <div style={{ display: 'flex', gap: '6px', justifyContent: 'center', flexWrap: 'wrap' }}>
        {([['evolucion', 'Evolución estatal'], ['peso', 'Peso nacional'], ['ranking', 'Ranking']] as [Tab, string][]).map(([t, l]) => (
          <button key={t} onClick={() => setTab(t)} style={{ padding: '6px 16px', fontSize: '12px', fontFamily, borderRadius: '4px', cursor: 'pointer', background: t === tab ? palette[0] : 'transparent', color: t === tab ? '#fff' : '#94a3b8', border: t === tab ? 'none' : '1px solid #2d3148' }}>{l}</button>
        ))}
      </div>

      {tab === 'evolucion' && (
        <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
          <div style={{ border: `1px solid ${palette[0]}`, borderRadius: '8px', padding: '10px 14px', background: `${palette[0]}1a` }}>
            <p style={{ margin: 0, color: '#94a3b8', fontSize: '11px', fontFamily }}>Variación 5 años</p>
            <p style={{ margin: '4px 0 0 0', color: palette[0], fontSize: '18px', fontWeight: 700, fontFamily }}>
              {`${evolVar >= 0 ? '+' : ''}${evolVar.toFixed(1)}%`}
            </p>
          </div>
        </div>
      )}

      <div ref={chartRef}>
        {tab === 'evolucion' && (
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={evolucionData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis domain={[0, yMaxEvo]} tickFormatter={(v) => `$${fmt(v)}`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number) => [`$${fmt(v)} M USD`, 'Exportaciones']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} formatter={() => `Exportaciones de ${estado}`} />
              <Bar dataKey="value" fill={palette[0]} radius={[3, 3, 0, 0]} name="Exportaciones" />
            </BarChart>
          </ResponsiveContainer>
        )}

        {tab === 'peso' && (
          <ResponsiveContainer width="100%" height={360}>
            <PieChart>
              <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={70} outerRadius={130} paddingAngle={3}
                label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`} labelLine={{ stroke: '#4a5568' }}>
                {pieData.map((_, i) => (<Cell key={i} fill={i === 0 ? palette[0] : '#2d3148'} />))}
              </Pie>
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number, n: string) => [`$${fmt(v)} M USD`, n]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
            </PieChart>
          </ResponsiveContainer>
        )}

        {tab === 'ranking' && (
          <ResponsiveContainer width="100%" height={Math.max(320, rankingTop10.length * 36)}>
            <BarChart data={rankingTop10} layout="vertical" margin={{ top: 8, right: 70, left: 10, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" horizontal={false} />
              <XAxis type="number" domain={[0, xMaxRank]} tickFormatter={(v) => `$${fmt(v)}`} tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis type="category" dataKey="estado" width={170} tick={{ fontSize: yAxisSize - 1, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number) => [`$${fmt(v)} M USD`, 'Exportaciones']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} formatter={() => `Top 10 exportadores (${latestYear})`} />
              <Bar dataKey="value" radius={[0, 3, 3, 0]} name="Exportaciones">
                {rankingTop10.map((d, i) => (<Cell key={d.estado} fill={d.estado === estado ? (palette[1] ?? palette[0]) : getColorForIndex(palette, i)} />))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {tab === 'ranking' && selectedRank > 10 && (
        <p style={{ fontSize: '12px', color: '#94a3b8', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
          {estado} no aparece en el Top 10. Posición nacional: #{selectedRank}.
        </p>
      )}

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
        Fuente: DataMéxico / Secretaría de Economía — Exportaciones por entidad federativa.
      </p>

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead><tr style={{ borderBottom: '1px solid #2d3148' }}>
            <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>{tab === 'ranking' ? 'Estado' : 'Periodo / Segmento'}</th>
            <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>{tab === 'peso' ? 'Participación' : 'Exportaciones'}</th>
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
