'use client'

import { useRef, useState, type RefObject } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
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
  return (v / 1e6).toFixed(1) + ' M'
}

type Tab = 'historico' | 'genero' | 'edad'

interface Props { estado: string }

export default function DemografiaEstatalChart({ estado }: Props) {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)
  const [tab, setTab] = useState<Tab>('historico')

  const { data: indicadores } = useIndicadores('estatal', 'demografia')
  const indicador = indicadores?.find((i) => i.clave === 'inegi.demografia_estatal')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>Cargando demografía estatal...</div>)
  }
  if (!datos.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>Sin datos disponibles.</div>)
  }

  // Filtrar por estado seleccionado
  const edoDatos = datos.filter((d) => d.entidad_clave?.endsWith(`:${estado}`))

  // Histórico: total por año
  const histData = edoDatos
    .filter((d) => d.entidad_clave?.startsWith('total:'))
    .map((d) => ({ anio: String(d.periodo), valor: Number(d.valor) }))
    .sort((a, b) => Number(a.anio) - Number(b.anio))

  // Género: hombres y mujeres por año
  const genAnios: Record<string, { hombres: number; mujeres: number }> = {}
  edoDatos.filter((d) => d.entidad_clave?.startsWith('hombres:') || d.entidad_clave?.startsWith('mujeres:')).forEach((d) => {
    const anio = String(d.periodo)
    if (!genAnios[anio]) genAnios[anio] = { hombres: 0, mujeres: 0 }
    if (d.entidad_clave?.startsWith('hombres:')) genAnios[anio].hombres = Number(d.valor)
    if (d.entidad_clave?.startsWith('mujeres:')) genAnios[anio].mujeres = Number(d.valor)
  })
  const genData = Object.entries(genAnios).sort(([a], [b]) => Number(a) - Number(b)).map(([anio, v]) => ({ anio, ...v }))

  // Edad: 0-19, 20-64, 65+ por año
  const edadAnios: Record<string, { j: number; ad: number; may: number }> = {}
  edoDatos.filter((d) => d.entidad_clave?.startsWith('0-19:') || d.entidad_clave?.startsWith('20-64:') || d.entidad_clave?.startsWith('65+:')).forEach((d) => {
    const anio = String(d.periodo)
    if (!edadAnios[anio]) edadAnios[anio] = { j: 0, ad: 0, may: 0 }
    if (d.entidad_clave?.startsWith('0-19:')) edadAnios[anio].j = Number(d.valor)
    if (d.entidad_clave?.startsWith('20-64:')) edadAnios[anio].ad = Number(d.valor)
    if (d.entidad_clave?.startsWith('65+:')) edadAnios[anio].may = Number(d.valor)
  })
  const edadData = Object.entries(edadAnios).sort(([a], [b]) => Number(a) - Number(b)).map(([anio, v]) => ({ anio, ...v }))

  const titles: Record<Tab, string> = { historico: `Crecimiento Poblacional — ${estado}`, genero: `Distribución por Género — ${estado}`, edad: `Grupos de Edad — ${estado}` }

  const maxHist = histData.length ? Math.max(...histData.map((d) => d.valor)) : 0
  const yMaxHist = Math.ceil(maxHist * 1.15)
  const maxGen = genData.length ? Math.max(...genData.flatMap((d) => [d.hombres, d.mujeres])) : 0
  const yMaxGen = Math.ceil(maxGen * 1.15)
  const maxEdad = edadData.length ? Math.max(...edadData.flatMap((d) => [d.j, d.ad, d.may])) : 0
  const yMaxEdad = Math.ceil(maxEdad * 1.15)

  if (!edoDatos.length) {
    return (<div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '40px', paddingBottom: '40px' }}>Sin datos demográficos para {estado}. Disponible para estados con datos censales cargados.</div>)
  }

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>{titles[tab]}</p>
        <button onClick={() => downloadChartAsPng(chartRef, `demografia-${estado.toLowerCase().replace(/ /g, '-')}-${tab}`)} title="Descargar" style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>↓ PNG</button>
      </div>
      <div style={{ display: 'flex', gap: '6px', justifyContent: 'center' }}>
        {([['historico', 'Crecimiento'], ['genero', 'Por Género'], ['edad', 'Grupos de Edad']] as [Tab, string][]).map(([t, l]) => (
          <button key={t} onClick={() => setTab(t)} style={{ padding: '6px 16px', fontSize: '12px', fontFamily, borderRadius: '4px', cursor: 'pointer', background: t === tab ? palette[0] : 'transparent', color: t === tab ? '#fff' : '#94a3b8', border: t === tab ? 'none' : '1px solid #2d3148' }}>{l}</button>
        ))}
      </div>
      <div ref={chartRef}>
        {tab === 'historico' && (
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={histData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis domain={[0, yMaxHist]} tickFormatter={fmtM} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number) => [fmt(v) + ' personas', 'Población']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} formatter={() => 'Población total'} />
              <Bar dataKey="valor" fill={palette[0]} radius={[3, 3, 0, 0]} name="Población" />
            </BarChart>
          </ResponsiveContainer>
        )}
        {tab === 'genero' && (
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={genData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis domain={[0, yMaxGen]} tickFormatter={fmtM} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number, n: string) => [fmt(v), n]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
              <Bar dataKey="hombres" fill={palette[0]} radius={[3, 3, 0, 0]} name="Hombres" />
              <Bar dataKey="mujeres" fill={palette[1]} radius={[3, 3, 0, 0]} name="Mujeres" />
            </BarChart>
          </ResponsiveContainer>
        )}
        {tab === 'edad' && (
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={edadData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis domain={[0, yMaxEdad]} tickFormatter={fmtM} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(v: number, n: string) => [fmt(v), n]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
              <Bar dataKey="j" fill={palette[0]} radius={[3, 3, 0, 0]} name="0-19 años" />
              <Bar dataKey="ad" fill={palette[1]} radius={[3, 3, 0, 0]} name="20-64 años" />
              <Bar dataKey="may" fill={palette[2]} radius={[3, 3, 0, 0]} name="65+ años" />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>Fuente: INEGI — Censos de Población y Vivienda</p>
      {indicador && <AnalisisIA graficaId={indicador.id} />}
      {indicador && <AnalisisRevisado graficaId={indicador.id} />}
    </div>
  )
}
