'use client'

import { useRef, type RefObject } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  LabelList,
} from 'recharts'
import { useStyleConfig } from '@/hooks/useStyleConfig'
import { useIndicadores, useIndicadorDatos } from '@/hooks/useIndicador'
import AnalisisIA from '@/components/ai/AnalisisIA'
import AnalisisRevisado from '@/components/ai/AnalisisRevisado'
import type { DatoIndicador } from '@/types'

function formatMillones(valor: number): string {
  return new Intl.NumberFormat('es-MX', { maximumFractionDigits: 1 }).format(
    valor / 1_000_000
  )
}

function downloadChartAsPng(containerRef: RefObject<HTMLDivElement>, filename: string) {
  const container = containerRef.current
  if (!container) return

  const svg = container.querySelector('svg')
  if (!svg) return

  const scale = 3 // alta resolución 3x
  const svgRect = svg.getBoundingClientRect()
  const width = svgRect.width * scale
  const height = svgRect.height * scale

  const svgData = new XMLSerializer().serializeToString(svg)
  const svgBlob = new Blob([svgData], { type: 'image/svg+xml;charset=utf-8' })
  const url = URL.createObjectURL(svgBlob)

  const img = new Image()
  img.onload = () => {
    const canvas = document.createElement('canvas')
    canvas.width = width
    canvas.height = height
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    ctx.fillStyle = '#1a1d27'
    ctx.fillRect(0, 0, width, height)
    ctx.drawImage(img, 0, 0, width, height)

    URL.revokeObjectURL(url)
    const link = document.createElement('a')
    link.download = `${filename}.png`
    link.href = canvas.toDataURL('image/png')
    link.click()
  }
  img.src = url
}

export default function PoblacionNacionalChart() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)

  const { data: indicadores } = useIndicadores('nacional', 'demografia')
  const indicador = indicadores?.find(
    (i) => i.clave === 'inegi.poblacion_total_nacional'
  )

  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos = datosResponse?.datos ?? []

  if (isLoading) {
    return (
      <div
        style={{
          background: '#1a1d27',
          border: '1px solid #2d3148',
          borderRadius: '10px',
          padding: '20px',
          height: '320px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: '#4a5568',
          fontSize: '13px',
          fontFamily,
        }}
      >
        Cargando datos de población...
      </div>
    )
  }

  if (!datos.length) {
    return (
      <div
        style={{
          background: '#1a1d27',
          border: '1px solid #2d3148',
          borderRadius: '10px',
          padding: '20px',
          color: '#4a5568',
          fontSize: '13px',
          fontFamily,
          textAlign: 'center',
          paddingTop: '60px',
          paddingBottom: '60px',
        }}
      >
        Sin datos disponibles para Población Total Nacional.
      </div>
    )
  }

  // Ordenar por período ascendente y preparar datos para recharts
  const chartData = [...datos]
    .filter((d) => Number.isFinite(Number(d.periodo)))
    .sort((a, b) => Number(a.periodo) - Number(b.periodo))
    .map((d: DatoIndicador) => ({
      anio: String(d.periodo),
      poblacion: d.valor,
      label: formatMillones(d.valor) + ' M',
    }))

  if (!chartData.length) {
    return (
      <div
        style={{
          background: '#1a1d27',
          border: '1px solid #2d3148',
          borderRadius: '10px',
          padding: '20px',
          color: '#4a5568',
          fontSize: '13px',
          fontFamily,
          textAlign: 'center',
          paddingTop: '60px',
          paddingBottom: '60px',
        }}
      >
        Sin períodos válidos para Población Total Nacional.
      </div>
    )
  }

  const color = palette[0] ?? palette[1]

  // Calcular dominio Y con 15% de margen sobre el máximo para que las etiquetas no se corten
  const maxValor = Math.max(...chartData.map((d) => d.poblacion))
  const yMax = Math.ceil(maxValor * 1.15)

  return (
    <div
      style={{
        background: '#1a1d27',
        border: '1px solid #2d3148',
        borderRadius: '10px',
        padding: '20px',
        display: 'flex',
        flexDirection: 'column',
        gap: '16px',
      }}
    >
      {/* Título + botón descarga */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p
          style={{
            fontSize: `${titleSize}px`,
            fontFamily,
            color: '#e2e8f0',
            margin: 0,
            fontWeight: 700,
            textAlign: 'center',
          }}
        >
          Crecimiento Poblacional Nacional
        </p>
        <button
          onClick={() => downloadChartAsPng(chartRef, 'crecimiento-poblacional-nacional')}
          title="Descargar gráfica en alta resolución"
          style={{
            position: 'absolute',
            right: 0,
            background: 'transparent',
            border: '1px solid #2d3148',
            borderRadius: '4px',
            color: '#64748b',
            fontSize: '11px',
            fontFamily,
            padding: '4px 10px',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: '4px',
          }}
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
          <XAxis
            dataKey="anio"
            tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }}
            axisLine={{ stroke: '#2d3148' }}
            tickLine={false}
          />
          <YAxis
            domain={[0, yMax]}
            tickFormatter={(v) => formatMillones(v) + ' M'}
            tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }}
            axisLine={{ stroke: '#2d3148' }}
            tickLine={false}
          />
          <Tooltip
            contentStyle={{
              background: '#1a1d27',
              border: '1px solid #2d3148',
              borderRadius: '6px',
              fontFamily,
              fontSize: '12px',
            }}
            formatter={(value: number) => [
              new Intl.NumberFormat('es-MX').format(value) + ' personas',
              'Población',
            ]}
            labelStyle={{ color: '#e2e8f0' }}
            itemStyle={{ color: '#94a3b8' }}
          />
          <Legend
            verticalAlign="bottom"
            align="center"
            wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }}
            formatter={() => 'Población total (personas)'}
          />
          <Bar dataKey="poblacion" fill={color} radius={[3, 3, 0, 0]} name="Población">
            <LabelList
              dataKey="label"
              position="top"
              style={{ fontSize: xAxisSize - 1, fill: '#64748b', fontFamily }}
            />
          </Bar>
        </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Fuente de datos */}
      <p
        style={{
          fontSize: '11px',
          color: '#4a5568',
          fontFamily,
          margin: '-8px 0 0 0',
          textAlign: 'left',
        }}
      >
        Fuente: INEGI — Censos y Conteos de Población y Vivienda (serie 1002000001)
      </p>

      {/* Tabla de datos */}
      <div style={{ overflowX: 'auto' }}>
        <table
          style={{
            width: '100%',
            borderCollapse: 'collapse',
            fontSize: '12px',
            fontFamily,
            color: '#94a3b8',
          }}
        >
          <thead>
            <tr style={{ borderBottom: '1px solid #2d3148' }}>
              <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>
                Población (personas)
              </th>
            </tr>
          </thead>
          <tbody>
            {chartData.map((d) => (
              <tr key={d.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                <td style={{ padding: '5px 8px' }}>{d.anio}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>
                  {new Intl.NumberFormat('es-MX').format(d.poblacion)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Análisis IA */}
      {indicador && <AnalisisIA graficaId={indicador.id} />}
      {indicador && <AnalisisRevisado graficaId={indicador.id} />}
    </div>
  )
}
