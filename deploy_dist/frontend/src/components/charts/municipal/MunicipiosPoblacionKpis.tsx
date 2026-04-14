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

type SexoKey = 'total' | 'hombres' | 'mujeres'
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

interface MunicipioPoint {
  estado: string
  municipio: string
  sexo: SexoKey
  periodo: number
  valor: number
}

interface PiramidePoint {
  estado: string
  municipio: string
  sexo: SexoPiramide
  grupo: string
  periodo: number
  valor: number
}

interface ProyeccionPoint {
  estado: string
  municipio: string
  sexo: SexoKey
  periodo: number
  valor: number
}

export default function MunicipiosPoblacionKpis() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)
  const proyChartRef = useRef<HTMLDivElement>(null)
  const [selectedEstado, setSelectedEstado] = useState('')
  const [selectedMunicipio, setSelectedMunicipio] = useState('')

  const { data: indicadores } = useIndicadores('municipal', 'demografia')
  const indicadorKpis = indicadores?.find((i) => i.clave === 'conapo.municipios_poblacion')
  const indicadorPiramide = indicadores?.find((i) => i.clave === 'conapo.municipios_piramide_edad')
  const indicadorProyeccion = indicadores?.find((i) => i.clave === 'conapo.municipios_proyeccion')

  const { data: opcionesEstadosResponse, isLoading: isLoadingEstados } = useOpcionesGeograficas(
    indicadorKpis?.id ?? null,
    { enabled: Boolean(indicadorKpis?.id) }
  )
  const { data: opcionesMunicipiosResponse, isLoading: isLoadingMunicipios } = useOpcionesGeograficas(
    indicadorKpis?.id ?? null,
    { enabled: Boolean(indicadorKpis?.id && selectedEstado), estado: selectedEstado || undefined }
  )

  const canLoadMunicipioData = Boolean(selectedEstado && selectedMunicipio)
  const { data: datosKpiResponse, isLoading: isLoadingKpis } = useIndicadorDatos(indicadorKpis?.id ?? null, {
    enabled: canLoadMunicipioData,
    entidadPrefix: canLoadMunicipioData ? `mun:${selectedEstado}:${selectedMunicipio}:` : undefined,
  })
  const { data: datosPiramideResponse, isLoading: isLoadingPiramide } = useIndicadorDatos(indicadorPiramide?.id ?? null, {
    enabled: canLoadMunicipioData,
    entidadPrefix: canLoadMunicipioData ? `mun_age:${selectedEstado}:${selectedMunicipio}:` : undefined,
  })
  const { data: datosProyeccionResponse, isLoading: isLoadingProyeccion } = useIndicadorDatos(indicadorProyeccion?.id ?? null, {
    enabled: canLoadMunicipioData,
    entidadPrefix: canLoadMunicipioData ? `mun_proy:${selectedEstado}:${selectedMunicipio}:` : undefined,
  })
  const datosKpis: DatoIndicador[] = datosKpiResponse?.datos ?? []
  const datosPiramide: DatoIndicador[] = datosPiramideResponse?.datos ?? []
  const datosProyeccion: DatoIndicador[] = datosProyeccionResponse?.datos ?? []
  const isLoading = isLoadingEstados || isLoadingMunicipios || isLoadingKpis || isLoadingPiramide || isLoadingProyeccion

  const parsed: MunicipioPoint[] = useMemo(() => {
    return datosKpis
      .filter((d) => d.entidad_clave?.startsWith('mun:'))
      .map((d) => {
        const parts = String(d.entidad_clave ?? '').split(':')
        if (parts.length < 4) return null
        const sexo = parts[parts.length - 1] as SexoKey
        const municipio = parts[parts.length - 2]
        const estado = parts.slice(1, parts.length - 2).join(':')
        const periodo = Number(d.periodo)
        if (!estado || !municipio || !Number.isFinite(periodo)) return null
        if (!['total', 'hombres', 'mujeres'].includes(sexo)) return null
        return {
          estado,
          municipio,
          sexo,
          periodo,
          valor: Number(d.valor),
        }
      })
      .filter((x): x is MunicipioPoint => Boolean(x))
  }, [datosKpis])

  const parsedPiramide: PiramidePoint[] = useMemo(() => {
    return datosPiramide
      .filter((d) => d.entidad_clave?.startsWith('mun_age:'))
      .map((d) => {
        const parts = String(d.entidad_clave ?? '').split(':')
        if (parts.length < 5) return null
        const grupo = parts[parts.length - 1]
        const sexo = parts[parts.length - 2] as SexoPiramide
        const municipio = parts[parts.length - 3]
        const estado = parts.slice(1, parts.length - 3).join(':')
        const periodo = Number(d.periodo)
        if (!estado || !municipio || !Number.isFinite(periodo)) return null
        if (!['hombres', 'mujeres'].includes(sexo)) return null
        return { estado, municipio, sexo, grupo, periodo, valor: Number(d.valor) }
      })
      .filter((x): x is PiramidePoint => Boolean(x))
  }, [datosPiramide])

  const parsedProyeccion: ProyeccionPoint[] = useMemo(() => {
    return datosProyeccion
      .filter((d) => d.entidad_clave?.startsWith('mun_proy:'))
      .map((d) => {
        const parts = String(d.entidad_clave ?? '').split(':')
        if (parts.length < 4) return null
        const sexo = parts[parts.length - 1] as SexoKey
        const municipio = parts[parts.length - 2]
        const estado = parts.slice(1, parts.length - 2).join(':')
        const periodo = Number(d.periodo)
        if (!estado || !municipio || !Number.isFinite(periodo)) return null
        if (!['total', 'hombres', 'mujeres'].includes(sexo)) return null
        return { estado, municipio, sexo, periodo, valor: Number(d.valor) }
      })
      .filter((x): x is ProyeccionPoint => Boolean(x))
  }, [datosProyeccion])

  const estados = useMemo(
    () => (opcionesEstadosResponse?.estados ?? []).slice().sort((a, b) => a.localeCompare(b)),
    [opcionesEstadosResponse]
  )
  const municipios = useMemo(
    () => (opcionesMunicipiosResponse?.municipios ?? []).slice().sort((a, b) => a.localeCompare(b)),
    [opcionesMunicipiosResponse]
  )

  useEffect(() => {
    if (selectedEstado && !estados.includes(selectedEstado)) {
      setSelectedEstado('')
    }
  }, [estados, selectedEstado])

  useEffect(() => {
    if (selectedMunicipio && !municipios.includes(selectedMunicipio)) {
      setSelectedMunicipio('')
    }
  }, [municipios, selectedMunicipio])

  const selectedRows = parsed.filter(
    (p) => p.estado === selectedEstado && p.municipio === selectedMunicipio
  )
  const latestYear = selectedRows.length ? Math.max(...selectedRows.map((r) => r.periodo)) : null

  const valueFor = (sexo: SexoKey): number | null => {
    const row = selectedRows.find((r) => r.periodo === latestYear && r.sexo === sexo)
    return row ? row.valor : null
  }

  const cards = [
    { label: 'Población total', key: 'total' as SexoKey, sub: latestYear ? `Año ${latestYear}` : '—' },
    { label: 'Hombres', key: 'hombres' as SexoKey, sub: latestYear ? `Año ${latestYear}` : '—' },
    { label: 'Mujeres', key: 'mujeres' as SexoKey, sub: latestYear ? `Año ${latestYear}` : '—' },
  ]

  const selectedPiramideRows = parsedPiramide.filter(
    (p) => p.estado === selectedEstado && p.municipio === selectedMunicipio
  )
  const latestYearPiramide = selectedPiramideRows.length
    ? Math.max(...selectedPiramideRows.map((r) => r.periodo))
    : null

  // Para que menor edad quede en la parte inferior, se ordena de mayor a menor edad.
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
    return {
      grupo,
      hombres: -Math.abs(h),
      mujeres: Math.abs(m),
      hombresAbs: h,
      mujeresAbs: m,
    }
  })

  const maxPiramide = piramideData.length
    ? Math.max(...piramideData.flatMap((d) => [Math.abs(d.hombres), Math.abs(d.mujeres)]))
    : 0
  const xMax = Math.max(1, Math.ceil(maxPiramide * 1.15))
  const hasPiramideData = piramideData.some((d) => d.hombresAbs > 0 || d.mujeresAbs > 0)

  const selectedProyeccionRows = parsedProyeccion.filter(
    (p) => p.estado === selectedEstado && p.municipio === selectedMunicipio
  )
  const proyeccionAnios = [...new Set(selectedProyeccionRows.map((r) => r.periodo))]
    .sort((a, b) => a - b)
    .slice(-5)
  const proyeccionData = proyeccionAnios.map((anio) => ({
    anio: String(anio),
    total: selectedProyeccionRows.find((r) => r.periodo === anio && r.sexo === 'total')?.valor ?? 0,
    hombres: selectedProyeccionRows.find((r) => r.periodo === anio && r.sexo === 'hombres')?.valor ?? 0,
    mujeres: selectedProyeccionRows.find((r) => r.periodo === anio && r.sexo === 'mujeres')?.valor ?? 0,
  }))
  const maxProy = proyeccionData.length
    ? Math.max(...proyeccionData.flatMap((d) => [d.total, d.hombres, d.mujeres]))
    : 0
  const yMaxProy = Math.max(1, Math.ceil(maxProy * 1.15))
  const hasProyeccionData = proyeccionData.some((d) => d.total > 0 || d.hombres > 0 || d.mujeres > 0)

  if (isLoading && canLoadMunicipioData) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>
        Cargando datos municipales...
      </div>
    )
  }

  if (canLoadMunicipioData && !parsed.length) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>
        Sin datos municipales disponibles.
      </div>
    )
  }

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
        Municipios — Población total y por sexo
      </p>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))', gap: '12px' }}>
        <div>
          <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '4px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
            Estado
          </label>
          <select
            value={selectedEstado}
            onChange={(e) => {
              setSelectedEstado(e.target.value)
              setSelectedMunicipio('')
            }}
            style={{ width: '100%', background: '#0f1117', border: '1px solid #2d3148', borderRadius: '4px', color: '#e2e8f0', fontSize: '14px', fontFamily, padding: '8px 10px', outline: 'none' }}
          >
            <option value="">Selecciona un estado...</option>
            {estados.map((e) => <option key={e} value={e}>{e}</option>)}
          </select>
        </div>

        <div>
          <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '4px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
            Municipio
          </label>
          <select
            value={selectedMunicipio}
            onChange={(e) => setSelectedMunicipio(e.target.value)}
            disabled={!selectedEstado}
            style={{ width: '100%', background: '#0f1117', border: '1px solid #2d3148', borderRadius: '4px', color: '#e2e8f0', fontSize: '14px', fontFamily, padding: '8px 10px', outline: 'none' }}
          >
            <option value="">Selecciona un municipio...</option>
            {municipios.map((m) => <option key={m} value={m}>{m}</option>)}
          </select>
        </div>
      </div>

      {!canLoadMunicipioData && (
        <div style={{ border: '1px dashed #2d3148', borderRadius: '8px', padding: '16px', color: '#94a3b8', fontSize: '12px', fontFamily, textAlign: 'center' }}>
          Selecciona un estado y un municipio para cargar la información.
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '12px' }}>
        {cards.map((card, i) => (
          <div
            key={card.key}
            style={{
              background: '#1a1d27',
              border: `1px solid ${palette[i % palette.length]}33`,
              borderTop: `3px solid ${palette[i % palette.length]}`,
              borderRadius: '10px',
              padding: '18px',
              fontFamily,
            }}
          >
            <p style={{ fontSize: '11px', color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.08em', margin: '0 0 8px 0' }}>
              {card.label}
            </p>
            <p style={{ fontSize: '26px', fontWeight: 300, color: palette[i % palette.length], margin: '0 0 6px 0', lineHeight: 1 }}>
              {valueFor(card.key) === null ? '—' : fmt(valueFor(card.key) as number)}
            </p>
            <p style={{ fontSize: '11px', color: '#4a5568', margin: 0 }}>{card.sub}</p>
          </div>
        ))}
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-6px 0 0 0', textAlign: 'left' }}>
        Fuente: CONAPO — Reconstrucción y proyecciones de la población de los municipios de México (base censal INEGI + proyecciones).
      </p>

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          Distribución de la población ({selectedMunicipio}, {latestYearPiramide ?? 'N/A'})
        </p>
        <button
          onClick={() => downloadChartAsPng(chartRef, `piramide-municipio-${selectedEstado}-${selectedMunicipio}`.toLowerCase().replace(/ /g, '-'))}
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
              <Legend
                verticalAlign="bottom"
                align="center"
                wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }}
              />
              <Bar dataKey="hombres" fill={palette[0] ?? palette[1]} name="Hombres" />
              <Bar dataKey="mujeres" fill={palette[1] ?? palette[0]} name="Mujeres" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
          Sin datos de distribución por edad para {selectedMunicipio}, {selectedEstado}.
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
          Proyección poblacional ({selectedMunicipio}, próximos 5 años)
        </p>
        <button
          onClick={() => downloadChartAsPng(proyChartRef, `proyeccion-municipio-${selectedEstado}-${selectedMunicipio}`.toLowerCase().replace(/ /g, '-'))}
          title="Descargar gráfica en alta resolución"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
        >
          ↓ PNG
        </button>
      </div>

      {hasProyeccionData ? (
        <div ref={proyChartRef}>
          <ResponsiveContainer width="100%" height={340}>
            <BarChart data={proyeccionData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
              <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <YAxis domain={[0, yMaxProy]} tickFormatter={(v) => fmt(v)} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
              <Tooltip
                contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }}
                formatter={(value: number, name: string) => [fmt(value), name]}
                labelStyle={{ color: '#e2e8f0' }}
                itemStyle={{ color: '#94a3b8' }}
              />
              <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
              <Bar dataKey="total" fill={palette[0] ?? palette[1]} name="Total" radius={[3, 3, 0, 0]} />
              <Bar dataKey="hombres" fill={palette[1] ?? palette[0]} name="Hombres" radius={[3, 3, 0, 0]} />
              <Bar dataKey="mujeres" fill={palette[2] ?? palette[0]} name="Mujeres" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
          Sin datos de proyección poblacional para {selectedMunicipio}, {selectedEstado}.
        </div>
      )}

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #2d3148' }}>
              <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Total</th>
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Hombres</th>
              <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Mujeres</th>
            </tr>
          </thead>
          <tbody>
            {proyeccionData.map((row) => (
              <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.total)}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.hombres)}</td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.mujeres)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
