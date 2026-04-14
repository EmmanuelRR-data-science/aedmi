'use client'

import { useRef, type RefObject } from 'react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
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

export default function TipoCambioChart() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)

  const { data: indicadores } = useIndicadores('nacional', 'economia')
  const indicador = indicadores?.find((i) => i.clave === 'banxico.tipo_cambio_usd_mxn')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', height: '320px', display: 'flex', alignItems: 'center',
        justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>
        Cargando datos de tipo de cambio...
      </div>
    )
  }

  if (!datos.length) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily,
        textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>
        Sin datos disponibles para Tipo de Cambio.
      </div>
    )
  }

  // Filtrar últimos 5 años y ordenar cronológicamente
  const currentYear = new Date().getFullYear()
  const minYear = currentYear - 5

  const chartData = datos
    .filter((d) => {
      const fecha = String(d.periodo)
      return parseInt(fecha.slice(0, 4)) >= minYear && !d.entidad_clave
    })
    .map((d) => ({
      fecha: String(d.periodo),
      valor: Number(d.valor),
    }))
    .sort((a, b) => a.fecha.localeCompare(b.fecha))

  // Mostrar solo ~1 etiqueta por mes en el eje X para no saturar
  const tickInterval = Math.max(1, Math.floor(chartData.length / 20))

  const maxValor = Math.max(...chartData.map((d) => d.valor))
  const minValor = Math.min(...chartData.map((d) => d.valor))
  const yMax = Math.ceil(maxValor * 1.05)
  const yMin = Math.floor(minValor * 0.95 * 100) / 100

  const color = palette[0] ?? palette[1]

  // Formatear fecha YYYY-MM-DD a Mes corto Año
  function fmtFecha(fecha: string): string {
    const meses = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
    const parts = fecha.split('-')
    if (parts.length >= 2) {
      return `${meses[parseInt(parts[1]) - 1]} ${parts[0].slice(2)}`
    }
    return fecha
  }

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
      padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          Tipo de Cambio USD/MXN (FIX Diario)
        </p>
        <button onClick={() => downloadChartAsPng(chartRef, 'tipo-cambio-usd-mxn')}
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
          <LineChart data={chartData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
            <XAxis dataKey="fecha" tick={{ fontSize: xAxisSize - 1, fontFamily, fill: '#94a3b8' }}
              axisLine={{ stroke: '#2d3148' }} tickLine={false}
              interval={tickInterval} tickFormatter={fmtFecha} />
            <YAxis domain={[yMin, yMax]} tickFormatter={(v) => `$${v.toFixed(2)}`}
              tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }}
              axisLine={{ stroke: '#2d3148' }} tickLine={false} />
            <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
              formatter={(v: number) => [`$${v.toFixed(4)} MXN/USD`, 'FIX']}
              labelFormatter={(label) => {
                const parts = String(label).split('-')
                if (parts.length === 3) {
                  const meses = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
                  return `${parseInt(parts[2])} ${meses[parseInt(parts[1]) - 1]} ${parts[0]}`
                }
                return label
              }}
              labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
            <Legend verticalAlign="bottom" align="center"
              wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }}
              formatter={() => 'Tipo de cambio FIX (pesos por dólar)'} />
            <Line key={`tc-${color}`} type="monotone" dataKey="valor" stroke={color} strokeWidth={1.5}
              dot={false} activeDot={{ r: 4, fill: color }} name="FIX" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
        Fuente: Banxico — Tipo de cambio FIX (serie SF43718). Actualización diaria a las 12:00 hrs MX.
      </p>

      {/* Tabla: solo últimos 20 registros para no saturar */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #2d3148' }}>
              <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Fecha</th>
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>FIX (MXN/USD)</th>
            </tr>
          </thead>
          <tbody>
            {chartData.slice(-20).reverse().map((d) => (
              <tr key={d.fecha} style={{ borderBottom: '1px solid #1e2235' }}>
                <td style={{ padding: '5px 8px' }}>{d.fecha}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>${d.valor.toFixed(4)}</td>
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
