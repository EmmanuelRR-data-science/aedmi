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

function fmtB(v: number): string {
  return '$' + (v / 1e9).toFixed(0) + 'B'
}

type ChartDatum = {
  pais: string
  isMexico: boolean
} & Record<string, number | string | boolean>

export default function TurismoRankingChart() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)

  const { data: indicadores } = useIndicadores('nacional', 'turismo')
  const indicador = indicadores?.find((i) => i.clave === 'wb.turismo_ranking')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', height: '400px', display: 'flex', alignItems: 'center',
        justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>
        Cargando ranking de turismo...
      </div>
    )
  }

  if (!datos.length) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily,
        textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>
        Sin datos disponibles para Ranking de Turismo.
      </div>
    )
  }

  // Obtener los 3 años disponibles
  const years = [...new Set(datos.map((d) => Number(d.periodo)))].sort((a, b) => a - b)
  const recentYears = years.slice(-3)

  // Agrupar por país con valores por año
  const porPais: Record<string, Record<number, number>> = {}
  for (const d of datos) {
    const pais = d.entidad_clave ?? ''
    const anio = Number(d.periodo)
    if (!recentYears.includes(anio)) continue
    if (!porPais[pais]) porPais[pais] = {}
    porPais[pais][anio] = Number(d.valor)
  }

  // Ordenar por el año más reciente (mayor a menor)
  const lastYear = recentYears[recentYears.length - 1]
  const chartData: ChartDatum[] = Object.entries(porPais)
    .map(([pais, vals]) => ({
      pais,
      ...Object.fromEntries(recentYears.map((y) => [String(y), vals[y] ?? 0])),
      isMexico: pais === 'México',
    })) as ChartDatum[]
  chartData.sort((a, b) => Number(b[String(lastYear)] ?? 0) - Number(a[String(lastYear)] ?? 0))

  const maxValor = Math.max(
    ...chartData.flatMap((d) => recentYears.map((y) => Number(d[String(y)] ?? 0)))
  )
  const xMax = Math.ceil(maxValor * 1.15)

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
      padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          Ranking Mundial de Ingresos Turísticos (Top 10)
        </p>
        <button onClick={() => downloadChartAsPng(chartRef, 'ranking-turismo-mundial')}
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
        <ResponsiveContainer width="100%" height={Math.max(400, chartData.length * 50)}>
          <BarChart data={chartData} layout="vertical" margin={{ top: 8, right: 60, left: 10, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" horizontal={false} />
            <XAxis type="number" domain={[0, xMax]} tickFormatter={fmtB}
              tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }}
              axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <YAxis type="category" dataKey="pais" width={130}
              tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }}
              axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
              formatter={(v: number, name: string) => [fmtB(v) + ' USD', name]}
              labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
            <Legend verticalAlign="bottom" align="center"
              wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
            {recentYears.map((y, i) => (
              <Bar key={y} dataKey={String(y)} fill={palette[i % palette.length] ?? '#0576F3'}
                radius={[0, 3, 3, 0]} name={String(y)} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
        Fuente: OMT / World Bank — International tourism receipts (ST.INT.RCPT.CD). México resaltado.
      </p>

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #2d3148' }}>
              <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>País</th>
              {recentYears.map((y) => (
                <th key={y} style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>{y} (USD)</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {chartData.map((d) => (
              <tr key={d.pais} style={{
                borderBottom: '1px solid #1e2235',
                background: d.isMexico ? 'rgba(5,118,243,0.1)' : 'transparent',
              }}>
                <td style={{ padding: '5px 8px', color: d.isMexico ? '#0576F3' : '#94a3b8', fontWeight: d.isMexico ? 600 : 400 }}>
                  {d.pais}
                </td>
                {recentYears.map((y) => (
                  <td key={y} style={{ padding: '5px 8px', textAlign: 'right' }}>
                    {fmtB(Number(d[String(y)] ?? 0))}
                  </td>
                ))}
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
