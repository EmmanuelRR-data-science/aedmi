'use client'

import { useRef, type RefObject } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, Cell,
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

const CURRENT_YEAR = new Date().getFullYear()

export default function PIBProyeccionChart() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRefTotal = useRef<HTMLDivElement>(null)
  const chartRefPC = useRef<HTMLDivElement>(null)

  const { data: indicadores } = useIndicadores('nacional', 'economia')
  const indicador = indicadores?.find((i) => i.clave === 'imf.pib_proyeccion')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', height: '320px', display: 'flex', alignItems: 'center',
        justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>
        Cargando proyecciones del FMI...
      </div>
    )
  }

  if (!datos.length) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily,
        textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>
        Sin datos disponibles para Proyecciones del PIB.
      </div>
    )
  }

  const totalData = datos
    .filter((d) => d.entidad_clave === 'PIB Total MXN')
    .map((d) => ({ anio: String(d.periodo), valor: d.valor, esProyeccion: Number(d.periodo) > CURRENT_YEAR }))
    .sort((a, b) => Number(a.anio) - Number(b.anio))

  const pcData = datos
    .filter((d) => d.entidad_clave === 'PIB Per Cápita MXN')
    .map((d) => ({ anio: String(d.periodo), valor: d.valor, esProyeccion: Number(d.periodo) > CURRENT_YEAR }))
    .sort((a, b) => Number(a.anio) - Number(b.anio))

  const maxTotal = Math.max(...totalData.map((d) => d.valor))
  const yMaxTotal = Math.ceil(maxTotal * 1.15)
  const maxPC = Math.max(...pcData.map((d) => d.valor))
  const yMaxPC = Math.ceil(maxPC * 1.15)

  const colorHistorico = palette[0] ?? palette[1]
  const colorProyeccion = palette[2] ?? palette[0]

  const btnStyle = {
    position: 'absolute' as const, right: 0, background: 'transparent', border: '1px solid #2d3148',
    borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px',
    cursor: 'pointer', display: 'flex' as const, alignItems: 'center' as const, gap: '4px',
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* PIB Total con proyecciones */}
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>

        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
          <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
            Proyección del PIB Nacional (MXN)
          </p>
          <button onClick={() => downloadChartAsPng(chartRefTotal, 'pib-proyeccion-total')}
            title="Descargar gráfica en alta resolución" style={btnStyle}
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
              <YAxis domain={[0, yMaxTotal]}
                tickFormatter={(v) => `$${(v / 1000).toFixed(0)}T`}
                tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }}
                axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
                formatter={(v: number) => [`$${new Intl.NumberFormat('es-MX', { maximumFractionDigits: 0 }).format(v)} miles de millones MXN`, 'PIB']}
                labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center"
                wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }}
                payload={[
                  { value: 'Histórico', type: 'square', color: colorHistorico },
                  { value: 'Proyección FMI', type: 'square', color: colorProyeccion },
                ]} />
              <Bar dataKey="valor" radius={[3, 3, 0, 0]}>
                {totalData.map((d, i) => (
                  <Cell key={i} fill={d.esProyeccion ? colorProyeccion : colorHistorico} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
          Fuente: FMI — World Economic Outlook (NGDPD × TC FIX Banxico). Barras naranjas = proyecciones.
        </p>
      </div>

      {/* PIB Per Cápita con proyecciones */}
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>

        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
          <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
            Proyección del PIB Per Cápita (MXN)
          </p>
          <button onClick={() => downloadChartAsPng(chartRefPC, 'pib-proyeccion-percapita')}
            title="Descargar gráfica en alta resolución" style={btnStyle}
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
              <YAxis domain={[0, yMaxPC]}
                tickFormatter={(v) => `$${new Intl.NumberFormat('es-MX', { maximumFractionDigits: 0 }).format(v)}`}
                tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }}
                axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
                formatter={(v: number) => [`$${new Intl.NumberFormat('es-MX', { maximumFractionDigits: 0 }).format(v)} MXN`, 'Per Cápita']}
                labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
              <Legend verticalAlign="bottom" align="center"
                wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }}
                payload={[
                  { value: 'Histórico', type: 'square', color: colorHistorico },
                  { value: 'Proyección FMI', type: 'square', color: colorProyeccion },
                ]} />
              <Bar dataKey="valor" radius={[3, 3, 0, 0]}>
                {pcData.map((d, i) => (
                  <Cell key={i} fill={d.esProyeccion ? colorProyeccion : colorHistorico} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
          Fuente: FMI — World Economic Outlook (NGDPDPC × TC FIX Banxico). Barras naranjas = proyecciones.
        </p>
      </div>

      {/* Tabla combinada */}
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px' }}>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #2d3148' }}>
                <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>PIB Total (B MXN)</th>
                <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Per Cápita (MXN)</th>
                <th style={{ textAlign: 'center', padding: '6px 8px', color: '#64748b' }}>Tipo</th>
              </tr>
            </thead>
            <tbody>
              {totalData.map((d) => {
                const pc = pcData.find((p) => p.anio === d.anio)
                return (
                  <tr key={d.anio} style={{ borderBottom: '1px solid #1e2235',
                    background: d.esProyeccion ? 'rgba(244,120,6,0.05)' : 'transparent' }}>
                    <td style={{ padding: '5px 8px' }}>{d.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>
                      ${new Intl.NumberFormat('es-MX', { maximumFractionDigits: 1 }).format(d.valor)}
                    </td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>
                      {pc ? `$${new Intl.NumberFormat('es-MX', { maximumFractionDigits: 0 }).format(pc.valor)}` : '—'}
                    </td>
                    <td style={{ padding: '5px 8px', textAlign: 'center' }}>
                      <span style={{
                        fontSize: '10px', padding: '1px 6px', borderRadius: '3px',
                        background: d.esProyeccion ? '#F4780622' : '#0576F322',
                        color: d.esProyeccion ? '#F47806' : '#0576F3',
                      }}>
                        {d.esProyeccion ? 'Proyección' : 'Histórico'}
                      </span>
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
