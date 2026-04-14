'use client'

import { useRef, type RefObject } from 'react'
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

const TIPOS = ['Visitantes internacionales', 'Mexicanos al exterior'] as const
type TipoBalanza = (typeof TIPOS)[number]
type ChartDatum = { anio: string } & Partial<Record<TipoBalanza, number>>

export default function BalanzaVisitantesChart() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)

  const { data: indicadores } = useIndicadores('nacional', 'turismo')
  const indicador = indicadores?.find((i) => i.clave === 'inegi.balanza_visitantes')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', height: '320px', display: 'flex', alignItems: 'center',
        justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>
        Cargando balanza de visitantes...
      </div>
    )
  }

  if (!datos.length) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily,
        textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>
        Sin datos disponibles para Balanza de Visitantes.
      </div>
    )
  }

  // Agrupar por año
  const porAnio: Record<string, Record<string, number>> = {}
  for (const d of datos) {
    const anio = String(d.periodo)
    const tipo = d.entidad_clave ?? ''
    if (!porAnio[anio]) porAnio[anio] = {}
    porAnio[anio][tipo] = Number(d.valor)
  }

  const chartData: ChartDatum[] = Object.entries(porAnio)
    .sort(([a], [b]) => Number(a) - Number(b))
    .map(([anio, tipos]) => ({ anio, ...tipos }))

  const maxValor = Math.max(...chartData.flatMap((d) => TIPOS.map((t) => (d[t] as number) ?? 0)))
  const yMax = Math.ceil(maxValor * 1.15)

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
      padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          Balanza de Visitantes Internacionales
        </p>
        <button onClick={() => downloadChartAsPng(chartRef, 'balanza-visitantes')}
          title="Descargar gráfica en alta resolución"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148',
            borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px',
            cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>
          ↓ PNG
        </button>
      </div>

      <div ref={chartRef}>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={chartData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
            <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }}
              axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <YAxis domain={[0, yMax]} tickFormatter={(v) => fmt(v)}
              tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }}
              axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
              formatter={(v: number, name: string) => [fmt(v) + ' miles', name]}
              labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
            <Legend verticalAlign="bottom" align="center"
              wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
            {TIPOS.map((tipo, i) => (
              <Bar key={tipo} dataKey={tipo} fill={palette[i % palette.length] ?? '#0576F3'}
                radius={[3, 3, 0, 0]} name={tipo} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
        Fuente: INEGI / Banxico — Balanza turística de México
      </p>

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #2d3148' }}>
              <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
              {TIPOS.map((t) => (
                <th key={t} style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>{t} (miles)</th>
              ))}
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Saldo</th>
            </tr>
          </thead>
          <tbody>
            {chartData.map((d) => {
              const vis = (d['Visitantes internacionales'] as number) ?? 0
              const mex = (d['Mexicanos al exterior'] as number) ?? 0
              return (
                <tr key={d.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                  <td style={{ padding: '5px 8px' }}>{d.anio}</td>
                  <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(vis)}</td>
                  <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(mex)}</td>
                  <td style={{ padding: '5px 8px', textAlign: 'right', color: vis - mex > 0 ? '#36F48C' : '#fca5a5' }}>
                    {vis - mex > 0 ? '+' : ''}{fmt(vis - mex)}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {indicador && <AnalisisIA graficaId={indicador.id} />}
      {indicador && <AnalisisRevisado graficaId={indicador.id} />}
    </div>
  )
}
