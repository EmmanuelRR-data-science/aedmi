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

function fmtBillones(v: number): string {
  return new Intl.NumberFormat('es-MX', { maximumFractionDigits: 1 }).format(v / 1e12) + ' B'
}

function fmtMiles(v: number): string {
  return '$' + new Intl.NumberFormat('es-MX', { maximumFractionDigits: 0 }).format(v)
}

export default function PIBAnualChart() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRefTotal = useRef<HTMLDivElement>(null)
  const chartRefPC = useRef<HTMLDivElement>(null)

  const { data: indicadores } = useIndicadores('nacional', 'economia')
  const indicador = indicadores?.find((i) => i.clave === 'worldbank.pib_anual_mxn')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', height: '320px', display: 'flex', alignItems: 'center',
        justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>
        Cargando datos de PIB anual...
      </div>
    )
  }

  if (!datos.length) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily,
        textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>
        Sin datos disponibles para PIB Anual.
      </div>
    )
  }

  // Separar PIB Total y Per Cápita
  const totalData = datos
    .filter((d) => d.entidad_clave === 'PIB Total')
    .map((d) => ({ anio: String(d.periodo), valor: d.valor }))
    .sort((a, b) => Number(a.anio) - Number(b.anio))

  const pcData = datos
    .filter((d) => d.entidad_clave === 'PIB Per Cápita')
    .map((d) => ({ anio: String(d.periodo), valor: d.valor }))
    .sort((a, b) => Number(a.anio) - Number(b.anio))

  const maxTotal = Math.max(...totalData.map((d) => d.valor))
  const yMaxTotal = Math.ceil(maxTotal * 1.15)
  const maxPC = Math.max(...pcData.map((d) => d.valor))
  const yMaxPC = Math.ceil(maxPC * 1.15)

  const colorTotal = palette[0] ?? palette[1]
  const colorPC = palette[1] ?? palette[0]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* PIB Total */}
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>

        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
          <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
            PIB Nacional Total (MXN corrientes)
          </p>
          <button onClick={() => downloadChartAsPng(chartRefTotal, 'pib-nacional-total-anual')}
            title="Descargar gráfica en alta resolución"
            style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148',
              borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px',
              cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
            onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
            onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>
            ↓ PNG
          </button>
        </div>

        <div ref={chartRefTotal}>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={totalData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }}
                axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis domain={[0, yMaxTotal]} tickFormatter={fmtBillones}
                tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }}
                axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
                formatter={(v: number) => ['$' + new Intl.NumberFormat('es-MX', { maximumFractionDigits: 0 }).format(v), 'PIB Total']}
                labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center"
                wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }}
                formatter={() => 'PIB Total (pesos corrientes)'} />
              <Bar dataKey="valor" fill={colorTotal} radius={[3, 3, 0, 0]} name="PIB Total" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
          Fuente: World Bank — GDP (current LCU), indicator NY.GDP.MKTP.CN
        </p>
      </div>

      {/* PIB Per Cápita */}
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>

        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
          <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
            PIB Per Cápita (MXN corrientes)
          </p>
          <button onClick={() => downloadChartAsPng(chartRefPC, 'pib-percapita-anual')}
            title="Descargar gráfica en alta resolución"
            style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148',
              borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px',
              cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
            onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
            onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>
            ↓ PNG
          </button>
        </div>

        <div ref={chartRefPC}>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={pcData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }}
                axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis domain={[0, yMaxPC]} tickFormatter={fmtMiles}
                tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }}
                axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
                formatter={(v: number) => [fmtMiles(v) + ' MXN', 'Per Cápita']}
                labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center"
                wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }}
                formatter={() => 'PIB Per Cápita (pesos corrientes por persona)'} />
              <Bar dataKey="valor" fill={colorPC} radius={[3, 3, 0, 0]} name="Per Cápita" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
          Fuente: World Bank — GDP per capita (current LCU), indicator NY.GDP.PCAP.CN
        </p>
      </div>

      {/* Tabla combinada */}
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px' }}>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #2d3148' }}>
                <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>PIB Total (MXN)</th>
                <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>PIB Per Cápita (MXN)</th>
              </tr>
            </thead>
            <tbody>
              {totalData.map((d) => {
                const pc = pcData.find((p) => p.anio === d.anio)
                return (
                  <tr key={d.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{d.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>
                      {'$' + new Intl.NumberFormat('es-MX', { maximumFractionDigits: 0 }).format(d.valor)}
                    </td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>
                      {pc ? fmtMiles(pc.valor) : '—'}
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
    </div>
  )
}
