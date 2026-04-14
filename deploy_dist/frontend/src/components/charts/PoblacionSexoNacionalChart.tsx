'use client'

import { useRef, type RefObject } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, LabelList,
} from 'recharts'
import { useStyleConfig } from '@/hooks/useStyleConfig'
import { useIndicadores, useIndicadorDatos } from '@/hooks/useIndicador'
import AnalisisIA from '@/components/ai/AnalisisIA'
import AnalisisRevisado from '@/components/ai/AnalisisRevisado'
import type { DatoIndicador } from '@/types'

function formatMillones(valor: number): string {
  return new Intl.NumberFormat('es-MX', { maximumFractionDigits: 1 }).format(valor / 1_000_000) + ' M'
}

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

const SEXOS = ['Hombres', 'Mujeres'] as const
type Sexo = typeof SEXOS[number]

export default function PoblacionSexoNacionalChart() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)

  const { data: indicadores } = useIndicadores('nacional', 'demografia')
  const indicador = indicadores?.find((i) => i.clave === 'inegi.poblacion_sexo_nacional')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', height: '320px', display: 'flex', alignItems: 'center',
        justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>
        Cargando datos de población por sexo...
      </div>
    )
  }

  if (!datos.length) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily,
        textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>
        Sin datos disponibles para Distribución por Sexo.
      </div>
    )
  }

  // Agrupar por año
  const porAnio: Record<string, Record<Sexo, number>> = {}
  for (const d of datos) {
    const anio = String(d.periodo)
    const sexo = d.entidad_clave as Sexo
    if (!porAnio[anio]) porAnio[anio] = { Hombres: 0, Mujeres: 0 }
    if (SEXOS.includes(sexo)) porAnio[anio][sexo] = d.valor
  }

  const chartData = Object.entries(porAnio)
    .sort(([a], [b]) => Number(a) - Number(b))
    .map(([anio, sexos]) => ({ anio, ...sexos }))

  const maxValor = Math.max(...chartData.flatMap((d) => SEXOS.map((s) => d[s] ?? 0)))
  const yMax = Math.ceil(maxValor * 1.15)

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
      padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>

      {/* Título + botón descarga */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          Distribución de la Población por Sexo
        </p>
        <button
          onClick={() => downloadChartAsPng(chartRef, 'distribucion-poblacion-sexo')}
          title="Descargar gráfica en alta resolución"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148',
            borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px',
            cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
        >
          ↓ PNG
        </button>
      </div>

      {/* Gráfica */}
      <div ref={chartRef}>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={chartData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
            <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }}
              axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <YAxis domain={[0, yMax]} tickFormatter={formatMillones}
              tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }}
              axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <Tooltip
              contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
              formatter={(value: number, name: string) => [
                new Intl.NumberFormat('es-MX').format(value) + ' personas', name
              ]}
              labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }}
            />
            <Legend verticalAlign="bottom" align="center"
              wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
            {SEXOS.map((sexo, i) => (
              <Bar key={sexo} dataKey={sexo} fill={palette[i % palette.length] ?? '#0576F3'}
                radius={[3, 3, 0, 0]} name={sexo}>
                <LabelList dataKey={sexo} position="top"
                  formatter={(v: number) => formatMillones(v)}
                  style={{ fontSize: xAxisSize - 1, fill: '#64748b', fontFamily }} />
              </Bar>
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Fuente */}
      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
        Fuente: INEGI — Censos y Conteos de Población y Vivienda (series 1002000002 y 1002000003)
      </p>

      {/* Tabla */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #2d3148' }}>
              <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
              {SEXOS.map((s) => (
                <th key={s} style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>{s}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {chartData.map((d) => (
              <tr key={d.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                <td style={{ padding: '5px 8px' }}>{d.anio}</td>
                {SEXOS.map((s) => (
                  <td key={s} style={{ padding: '5px 8px', textAlign: 'right' }}>
                    {new Intl.NumberFormat('es-MX').format(d[s] ?? 0)}
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
