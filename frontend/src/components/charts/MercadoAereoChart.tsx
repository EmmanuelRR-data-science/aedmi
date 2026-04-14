'use client'

import { useRef, useState, type RefObject } from 'react'
import {
  PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'
import { useStyleConfig, getColorForIndex } from '@/hooks/useStyleConfig'
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

type Tab = 'nacional' | 'internacional'

export default function MercadoAereoChart() {
  const { palette, fontFamily, titleSize, xAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)
  const [tab, setTab] = useState<Tab>('nacional')

  const { data: indicadores } = useIndicadores('nacional', 'conectividad_aerea')
  const indicador = indicadores?.find((i) => i.clave === 'afac.mercado_aereo')
  const { data: datosResponse, isLoading } = useIndicadorDatos(indicador?.id ?? null)
  const datos: DatoIndicador[] = datosResponse?.datos ?? []

  if (isLoading) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', height: '320px', display: 'flex', alignItems: 'center',
        justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>
        Cargando datos de mercado aéreo...
      </div>
    )
  }

  if (!datos.length) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
        padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily,
        textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>
        Sin datos disponibles para Mercado Aéreo.
      </div>
    )
  }

  const aeroData = datos
    .filter((d) => d.entidad_clave?.startsWith('aero:'))
    .map((d) => ({ name: d.entidad_clave?.replace('aero:', '') ?? '', value: Number(d.valor) }))
    .sort((a, b) => b.value - a.value)

  const paisData = datos
    .filter((d) => d.entidad_clave?.startsWith('pais:'))
    .map((d) => ({ name: d.entidad_clave?.replace('pais:', '') ?? '', value: Number(d.valor) }))
    .sort((a, b) => b.value - a.value)

  const activeData = tab === 'nacional' ? aeroData : paisData
  const chartTitle = tab === 'nacional'
    ? 'Participación de Mercado Aéreo — Nacional (por Aerolínea)'
    : 'Participación de Mercado Aéreo — Internacional (por Región)'
  const filename = tab === 'nacional' ? 'mercado-aereo-nacional' : 'mercado-aereo-internacional'

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px',
      padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>

      {/* Título + botón descarga */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          {chartTitle}
        </p>
        <button onClick={() => downloadChartAsPng(chartRef, filename)}
          title="Descargar gráfica en alta resolución"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148',
            borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px',
            cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>
          ↓ PNG
        </button>
      </div>

      {/* Tabs Nacional / Internacional */}
      <div style={{ display: 'flex', gap: '6px', justifyContent: 'center' }}>
        {(['nacional', 'internacional'] as Tab[]).map((t) => (
          <button key={t} onClick={() => setTab(t)}
            style={{
              padding: '6px 18px', fontSize: '12px', fontFamily, borderRadius: '4px', cursor: 'pointer',
              background: t === tab ? palette[0] ?? palette[1] : 'transparent',
              color: t === tab ? '#fff' : '#94a3b8',
              border: t === tab ? 'none' : '1px solid #2d3148',
              textTransform: 'capitalize',
            }}>
            {t === 'nacional' ? 'Nacional (Aerolíneas)' : 'Internacional (Regiones)'}
          </button>
        ))}
      </div>

      {/* Gráfica de pastel */}
      <div ref={chartRef}>
        <ResponsiveContainer width="100%" height={380}>
          <PieChart>
            <Pie data={activeData} dataKey="value" nameKey="name" cx="50%" cy="50%"
              outerRadius={130} paddingAngle={1}
              label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`}
              labelLine={{ stroke: '#4a5568' }}>
              {activeData.map((_, i) => (
                <Cell key={i} fill={getColorForIndex(palette, i)} />
              ))}
            </Pie>
            <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
              formatter={(v: number, name: string) => [`${v.toFixed(1)}%`, name]}
              labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
            <Legend verticalAlign="bottom" align="center"
              wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
          </PieChart>
        </ResponsiveContainer>
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
        Fuente: AFAC / Datatur — Estadísticas de aviación civil (2025)
      </p>

      {/* Tabla */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #2d3148' }}>
              <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>
                {tab === 'nacional' ? 'Aerolínea' : 'Región / País'}
              </th>
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Participación (%)</th>
            </tr>
          </thead>
          <tbody>
            {activeData.map((d) => (
              <tr key={d.name} style={{ borderBottom: '1px solid #1e2235' }}>
                <td style={{ padding: '5px 8px' }}>{d.name}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{d.value.toFixed(1)}%</td>
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
