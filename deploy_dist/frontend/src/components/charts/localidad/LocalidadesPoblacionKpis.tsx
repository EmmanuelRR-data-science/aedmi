'use client'

import { useEffect, useMemo, useRef, useState, type RefObject } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts'
import { useStyleConfig } from '@/hooks/useStyleConfig'
import { useIndicadores, useIndicadorDatos, useOpcionesGeograficas } from '@/hooks/useIndicador'
import type { DatoIndicador } from '@/types'

function fmt(v: number): string {
  return new Intl.NumberFormat('es-MX', { maximumFractionDigits: 0 }).format(v)
}

type SexoKey = 'total' | 'mujeres' | 'hombres'
type SexoPiramide = 'hombres' | 'mujeres'

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

interface LocalidadPoint {
  estado: string
  municipio: string
  localidad: string
  sexo: SexoKey
  periodo: number
  valor: number
}

interface LocalidadPiramidePoint {
  estado: string
  municipio: string
  localidad: string
  sexo: SexoPiramide
  grupo: string
  periodo: number
  valor: number
}

export default function LocalidadesPoblacionKpis() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)
  const growthChartRef = useRef<HTMLDivElement>(null)
  const [selectedEstado, setSelectedEstado] = useState('')
  const [selectedMunicipio, setSelectedMunicipio] = useState('')
  const [searchLocalidad, setSearchLocalidad] = useState('')
  const [selectedLocalidad, setSelectedLocalidad] = useState('')

  const { data: indicadores } = useIndicadores('localidad', 'demografia')
  const indicador = indicadores?.find((i) => i.clave === 'conapo.localidades_poblacion')
  const indicadorPiramide = indicadores?.find((i) => i.clave === 'conapo.localidades_piramide_edad')
  const { data: opcionesEstadosResponse, isLoading: isLoadingEstados } = useOpcionesGeograficas(
    indicador?.id ?? null,
    { enabled: Boolean(indicador?.id) }
  )
  const { data: opcionesMunicipiosResponse, isLoading: isLoadingMunicipios } = useOpcionesGeograficas(
    indicador?.id ?? null,
    { enabled: Boolean(indicador?.id && selectedEstado), estado: selectedEstado || undefined }
  )
  const { data: opcionesLocalidadesResponse, isLoading: isLoadingLocalidades } = useOpcionesGeograficas(
    indicador?.id ?? null,
    {
      enabled: Boolean(indicador?.id && selectedEstado && selectedMunicipio),
      estado: selectedEstado || undefined,
      municipio: selectedMunicipio || undefined,
      q: searchLocalidad || undefined,
    }
  )
  const canLoadLocalidadData = Boolean(selectedEstado && selectedMunicipio && selectedLocalidad)
  const { data: datosResponse, isLoading: isLoadingKpis } = useIndicadorDatos(indicador?.id ?? null, {
    enabled: canLoadLocalidadData,
    entidadPrefix: canLoadLocalidadData
      ? `loc:${selectedEstado}:${selectedMunicipio}:${selectedLocalidad}:`
      : undefined,
  })
  const { data: datosPiramideResponse, isLoading: isLoadingPiramide } = useIndicadorDatos(indicadorPiramide?.id ?? null, {
    enabled: canLoadLocalidadData,
    entidadPrefix: canLoadLocalidadData
      ? `loc_age:${selectedEstado}:${selectedMunicipio}:${selectedLocalidad}:`
      : undefined,
  })
  const datos: DatoIndicador[] = datosResponse?.datos ?? []
  const datosPiramide: DatoIndicador[] = datosPiramideResponse?.datos ?? []
  const isLoading = isLoadingEstados || isLoadingMunicipios || isLoadingLocalidades || isLoadingKpis || isLoadingPiramide

  const parsed: LocalidadPoint[] = useMemo(() => {
    return datos
      .filter((d) => d.entidad_clave?.startsWith('loc:'))
      .map((d) => {
        const parts = String(d.entidad_clave ?? '').split(':')
        if (parts.length < 5) return null
        const sexo = parts[parts.length - 1] as SexoKey
        const localidad = parts[parts.length - 2]
        const municipio = parts[parts.length - 3]
        const estado = parts.slice(1, parts.length - 3).join(':')
        const periodo = Number(d.periodo)
        if (!estado || !municipio || !localidad || !Number.isFinite(periodo)) return null
        if (!['total', 'mujeres', 'hombres'].includes(sexo)) return null
        return { estado, municipio, localidad, sexo, periodo, valor: Number(d.valor) }
      })
      .filter((x): x is LocalidadPoint => Boolean(x))
  }, [datos])

  const parsedPiramide: LocalidadPiramidePoint[] = useMemo(() => {
    return datosPiramide
      .filter((d) => d.entidad_clave?.startsWith('loc_age:'))
      .map((d) => {
        const parts = String(d.entidad_clave ?? '').split(':')
        if (parts.length < 6) return null
        const grupo = parts[parts.length - 1]
        const sexo = parts[parts.length - 2] as SexoPiramide
        const localidad = parts[parts.length - 3]
        const municipio = parts[parts.length - 4]
        const estado = parts.slice(1, parts.length - 4).join(':')
        const periodo = Number(d.periodo)
        if (!estado || !municipio || !localidad || !Number.isFinite(periodo)) return null
        if (!['hombres', 'mujeres'].includes(sexo)) return null
        return { estado, municipio, localidad, sexo, grupo, periodo, valor: Number(d.valor) }
      })
      .filter((x): x is LocalidadPiramidePoint => Boolean(x))
  }, [datosPiramide])

  const estados = useMemo(
    () => (opcionesEstadosResponse?.estados ?? []).slice().sort((a, b) => a.localeCompare(b)),
    [opcionesEstadosResponse]
  )
  const municipios = useMemo(
    () => (opcionesMunicipiosResponse?.municipios ?? []).slice().sort((a, b) => a.localeCompare(b)),
    [opcionesMunicipiosResponse]
  )
  const localidades = useMemo(
    () => (opcionesLocalidadesResponse?.localidades ?? []).slice().sort((a, b) => a.localeCompare(b)),
    [opcionesLocalidadesResponse]
  )

  useEffect(() => {
    if (selectedEstado && !estados.includes(selectedEstado)) setSelectedEstado('')
  }, [estados, selectedEstado])

  useEffect(() => {
    if (selectedMunicipio && !municipios.includes(selectedMunicipio)) setSelectedMunicipio('')
  }, [municipios, selectedMunicipio])

  useEffect(() => {
    if (selectedLocalidad && !localidades.includes(selectedLocalidad)) setSelectedLocalidad('')
  }, [localidades, selectedLocalidad])

  const filteredLocalidades = useMemo(() => {
    return localidades
  }, [localidades, searchLocalidad])

  const selectedRows = parsed.filter(
    (p) => p.estado === selectedEstado && p.municipio === selectedMunicipio && p.localidad === selectedLocalidad
  )
  const latestYear = selectedRows.length ? Math.max(...selectedRows.map((r) => r.periodo)) : null

  const valueFor = (sexo: SexoKey): number | null => {
    const row = selectedRows.find((r) => r.periodo === latestYear && r.sexo === sexo)
    return row ? row.valor : null
  }

  const cards = [
    { label: 'Población total', key: 'total' as SexoKey, sub: latestYear ? `Año ${latestYear}` : '—' },
    { label: 'Mujeres', key: 'mujeres' as SexoKey, sub: latestYear ? `Año ${latestYear}` : '—' },
    { label: 'Hombres', key: 'hombres' as SexoKey, sub: latestYear ? `Año ${latestYear}` : '—' },
  ]

  const selectedPiramideRows = parsedPiramide.filter(
    (p) => p.estado === selectedEstado && p.municipio === selectedMunicipio && p.localidad === selectedLocalidad
  )
  const latestYearPiramide = selectedPiramideRows.length
    ? Math.max(...selectedPiramideRows.map((r) => r.periodo))
    : null

  const youngestToOldest = ['0-4', '5-9', '10-14', '15-24', '25-34', '35-44', '45-54', '55-64', '65-74', '75-84', '85+']
  const expectedTopToBottom = [...youngestToOldest].reverse()
  const gruposDisponibles = [
    ...new Set(
      selectedPiramideRows
        .filter((r) => r.periodo === latestYearPiramide)
        .map((r) => r.grupo)
    ),
  ]
  const gruposOrden = expectedTopToBottom.filter((g) => gruposDisponibles.includes(g))

  const piramideData = gruposOrden.map((grupo) => {
    const h = selectedPiramideRows.find((r) => r.periodo === latestYearPiramide && r.grupo === grupo && r.sexo === 'hombres')?.valor ?? 0
    const m = selectedPiramideRows.find((r) => r.periodo === latestYearPiramide && r.grupo === grupo && r.sexo === 'mujeres')?.valor ?? 0
    return { grupo, hombres: -Math.abs(h), mujeres: Math.abs(m), hombresAbs: h, mujeresAbs: m }
  })
  const maxPiramide = piramideData.length
    ? Math.max(...piramideData.flatMap((d) => [Math.abs(d.hombres), Math.abs(d.mujeres)]))
    : 0
  const xMax = Math.max(1, Math.ceil(maxPiramide * 1.15))
  const hasPiramideData = piramideData.some((d) => d.hombresAbs > 0 || d.mujeresAbs > 0)
  const historicalData = [...new Set(selectedRows.map((r) => r.periodo))]
    .sort((a, b) => a - b)
    .map((anio) => {
      const total = selectedRows.find((r) => r.periodo === anio && r.sexo === 'total')?.valor ?? 0
      const hombres = selectedRows.find((r) => r.periodo === anio && r.sexo === 'hombres')?.valor ?? 0
      const mujeres = selectedRows.find((r) => r.periodo === anio && r.sexo === 'mujeres')?.valor ?? 0
      return { anio, total, hombres, mujeres }
    })
  const hasHistoricalData = historicalData.some((d) => d.total > 0 || d.hombres > 0 || d.mujeres > 0)

  if (isLoading && canLoadLocalidadData) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>
        Cargando datos de localidades...
      </div>
    )
  }

  if (canLoadLocalidadData && !parsed.length) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>
        Sin datos de localidades disponibles.
      </div>
    )
  }

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
        Localidades — Población total y por sexo
      </p>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: '12px' }}>
        <div>
          <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '4px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Estado</label>
          <select value={selectedEstado} onChange={(e) => { setSelectedEstado(e.target.value); setSelectedMunicipio(''); setSelectedLocalidad(''); setSearchLocalidad('') }}
            style={{ width: '100%', background: '#0f1117', border: '1px solid #2d3148', borderRadius: '4px', color: '#e2e8f0', fontSize: '14px', fontFamily, padding: '8px 10px', outline: 'none' }}>
            <option value="">Selecciona un estado...</option>
            {estados.map((e) => <option key={e} value={e}>{e}</option>)}
          </select>
        </div>

        <div>
          <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '4px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Municipio</label>
          <select value={selectedMunicipio} onChange={(e) => { setSelectedMunicipio(e.target.value); setSelectedLocalidad(''); setSearchLocalidad('') }}
            disabled={!selectedEstado}
            style={{ width: '100%', background: '#0f1117', border: '1px solid #2d3148', borderRadius: '4px', color: '#e2e8f0', fontSize: '14px', fontFamily, padding: '8px 10px', outline: 'none' }}>
            <option value="">Selecciona un municipio...</option>
            {municipios.map((m) => <option key={m} value={m}>{m}</option>)}
          </select>
        </div>

        <div style={{ position: 'relative' }}>
          <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '4px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Localidad</label>
          <input
            value={searchLocalidad}
            onChange={(e) => setSearchLocalidad(e.target.value)}
            placeholder="Escribe para buscar localidad..."
            disabled={!selectedEstado || !selectedMunicipio}
            style={{ width: '100%', background: '#0f1117', border: '1px solid #2d3148', borderRadius: '4px', color: '#e2e8f0', fontSize: '14px', fontFamily, padding: '8px 10px', outline: 'none' }}
          />
          {!!filteredLocalidades.length && (
            <div style={{ position: 'absolute', zIndex: 10, top: '62px', left: 0, right: 0, maxHeight: '180px', overflowY: 'auto', background: '#0f1117', border: '1px solid #2d3148', borderRadius: '4px' }}>
              {filteredLocalidades.map((loc) => (
                <button
                  key={loc}
                  onClick={() => { setSelectedLocalidad(loc); setSearchLocalidad(loc) }}
                  style={{ width: '100%', textAlign: 'left', background: loc === selectedLocalidad ? '#1e2235' : 'transparent', border: 'none', color: '#e2e8f0', padding: '8px 10px', cursor: 'pointer', fontFamily, fontSize: '13px' }}
                >
                  {loc}
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      {!canLoadLocalidadData && (
        <div style={{ border: '1px dashed #2d3148', borderRadius: '8px', padding: '16px', color: '#94a3b8', fontSize: '12px', fontFamily, textAlign: 'center' }}>
          Selecciona estado, municipio y localidad para cargar la información.
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '12px' }}>
        {cards.map((card, i) => (
          <div key={card.key} style={{ background: '#1a1d27', border: `1px solid ${palette[i % palette.length]}33`, borderTop: `3px solid ${palette[i % palette.length]}`, borderRadius: '10px', padding: '18px', fontFamily }}>
            <p style={{ fontSize: '11px', color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.08em', margin: '0 0 8px 0' }}>{card.label}</p>
            <p style={{ fontSize: '26px', fontWeight: 300, color: palette[i % palette.length], margin: '0 0 6px 0', lineHeight: 1 }}>
              {valueFor(card.key) === null ? '—' : fmt(valueFor(card.key) as number)}
            </p>
            <p style={{ fontSize: '11px', color: '#4a5568', margin: 0 }}>{card.sub}</p>
          </div>
        ))}
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-6px 0 0 0', textAlign: 'left' }}>
        Fuente: CONAPO (base censal INEGI) — Población por localidad (referencial).
      </p>

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          Distribución de la población ({selectedLocalidad}, {latestYearPiramide ?? 'N/A'})
        </p>
        <button
          onClick={() => downloadChartAsPng(chartRef, `piramide-localidad-${selectedEstado}-${selectedMunicipio}-${selectedLocalidad}`.toLowerCase().replace(/ /g, '-'))}
          title="Descargar gráfica en alta resolución"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
        >
          ↓ PNG
        </button>
      </div>

      {hasPiramideData ? (
        <div ref={chartRef}>
          <ResponsiveContainer width="100%" height={340}>
            <BarChart layout="vertical" data={piramideData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis
                type="number"
                domain={[-xMax, xMax]}
                tickFormatter={(v) => fmt(Math.abs(v))}
                tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }}
                axisLine={{ stroke: '#2d3148' }}
                tickLine={false}
              />
              <YAxis
                dataKey="grupo"
                type="category"
                tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }}
                axisLine={{ stroke: '#2d3148' }}
                tickLine={false}
              />
              <Tooltip
                contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
                formatter={(value: number, name: string) => [fmt(Math.abs(value)), name]}
                labelStyle={{ color: '#e2e8f0' }}
                itemStyle={{ color: '#94a3b8' }}
              />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
              <Bar dataKey="hombres" fill={palette[0] ?? palette[1]} name="Hombres" />
              <Bar dataKey="mujeres" fill={palette[1] ?? palette[0]} name="Mujeres" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
          Sin datos de distribución por edad para {selectedLocalidad}, {selectedMunicipio}.
        </div>
      )}

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #2d3148' }}>
              <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Grupo de edad</th>
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Hombres</th>
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Mujeres</th>
            </tr>
          </thead>
          <tbody>
            {piramideData.map((row) => (
              <tr key={row.grupo} style={{ borderBottom: '1px solid #1e2235' }}>
                <td style={{ padding: '5px 8px' }}>{row.grupo}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.hombresAbs)}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.mujeresAbs)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          Crecimiento histórico ({selectedLocalidad})
        </p>
        <button
          onClick={() => downloadChartAsPng(growthChartRef, `crecimiento-localidad-${selectedEstado}-${selectedMunicipio}-${selectedLocalidad}`.toLowerCase().replace(/ /g, '-'))}
          title="Descargar gráfica en alta resolución"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
        >
          ↓ PNG
        </button>
      </div>

      {hasHistoricalData ? (
        <div ref={growthChartRef}>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={historicalData} margin={{ top: 20, right: 16, left: 8, bottom: 6 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis
                dataKey="anio"
                tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }}
                axisLine={{ stroke: '#2d3148' }}
                tickLine={false}
              />
              <YAxis
                tickFormatter={(v) => fmt(Number(v))}
                tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }}
                axisLine={{ stroke: '#2d3148' }}
                tickLine={false}
              />
              <Tooltip
                contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
                formatter={(value: number, name: string) => [fmt(Number(value)), name]}
                labelStyle={{ color: '#e2e8f0' }}
                itemStyle={{ color: '#94a3b8' }}
              />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
              <Bar dataKey="total" fill={palette[0] ?? palette[1]} name="Total" />
              <Bar dataKey="mujeres" fill={palette[1] ?? palette[0]} name="Mujeres" />
              <Bar dataKey="hombres" fill={palette[2] ?? palette[0]} name="Hombres" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
          Sin datos históricos para {selectedLocalidad}, {selectedMunicipio}.
        </div>
      )}

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #2d3148' }}>
              <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Total</th>
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Mujeres</th>
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Hombres</th>
            </tr>
          </thead>
          <tbody>
            {historicalData.map((row) => (
              <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.total)}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.mujeres)}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.hombres)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
