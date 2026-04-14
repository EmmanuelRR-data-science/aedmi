'use client'

import { useQuery } from '@tanstack/react-query'
import { apiFetch } from '@/lib/api'
import { useStyleConfig } from '@/hooks/useStyleConfig'

interface KpiData {
  nombre: string
  valor: number | null
  fecha: string | null
  unidad: string | null
}

interface KpisNacionales {
  'banxico.pib_trimestral_mxn': KpiData
  'banxico.pib_trimestral_usd': KpiData
  'banxico.tipo_cambio_usd_mxn': KpiData
  'banxico.inflacion_inpc_anual': KpiData
}

function formatValor(valor: number | null, unidad: string | null): string {
  if (valor === null) return '—'
  if (unidad === '%') return valor.toFixed(2) + '%'
  // Mostrar cifra completa con separadores de miles, hasta 4 decimales sin ceros finales
  return new Intl.NumberFormat('es-MX', {
    maximumFractionDigits: 4,
    minimumFractionDigits: 0,
  }).format(valor)
}

function formatFecha(fecha: string | null): string {
  if (!fecha) return '—'
  const meses = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']

  // Intentar parsear como fecha ISO (YYYY-MM-DD o YYYY-MM-DDTHH:mm:ss)
  const isoMatch = fecha.match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (isoMatch) {
    const anio = isoMatch[1]
    const mes = meses[parseInt(isoMatch[2]) - 1] ?? isoMatch[2]
    const dia = parseInt(isoMatch[3])
    return `${dia} ${mes} ${anio}`
  }

  // Formato YYYY-MM (mensual sin día)
  const mesMatch = fecha.match(/^(\d{4})-(\d{2})$/)
  if (mesMatch) {
    const mes = meses[parseInt(mesMatch[2]) - 1] ?? mesMatch[2]
    return `${mes} ${mesMatch[1]}`
  }

  return fecha
}

export default function KpiCards() {
  const { fontFamily, palette } = useStyleConfig()

  const { data, isLoading, error } = useQuery<KpisNacionales>({
    queryKey: ['kpis-nacionales'],
    queryFn: () => apiFetch<KpisNacionales>('/indicadores/kpis/nacionales'),
    refetchInterval: 5 * 60 * 1000, // refresca cada 5 min
  })

  const kpiOrder: Array<keyof KpisNacionales> = [
    'banxico.pib_trimestral_mxn',
    'banxico.pib_trimestral_usd',
    'banxico.tipo_cambio_usd_mxn',
    'banxico.inflacion_inpc_anual',
  ]

  if (isLoading) {
    return (
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '16px' }}>
        {[0, 1, 2, 3].map((i) => (
          <div
            key={i}
            style={{
              background: '#1a1d27',
              border: '1px solid #2d3148',
              borderRadius: '10px',
              padding: '20px',
              height: '100px',
              animation: 'pulse 1.5s ease-in-out infinite',
            }}
          />
        ))}
      </div>
    )
  }

  if (error || !data) {
    return (
      <div
        style={{
          background: '#2d1b1b',
          border: '1px solid #7f1d1d',
          borderRadius: '8px',
          padding: '16px',
          color: '#fca5a5',
          fontSize: '13px',
          fontFamily,
        }}
      >
        No se pudieron cargar los KPIs nacionales. Verifica que el ETL haya corrido al menos una vez.
      </div>
    )
  }

  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
        gap: '16px',
      }}
    >
      {kpiOrder.map((clave, i) => {
        const kpi = data[clave]
        const color = palette[i % palette.length]

        return (
          <div
            key={clave}
            style={{
              background: '#1a1d27',
              border: `1px solid ${color}33`,
              borderTop: `3px solid ${color}`,
              borderRadius: '10px',
              padding: '20px',
              fontFamily,
            }}
          >
            <p
              style={{
                fontSize: '11px',
                color: '#64748b',
                textTransform: 'uppercase',
                letterSpacing: '0.08em',
                margin: '0 0 10px 0',
              }}
            >
              {kpi?.nombre ?? clave}
            </p>
            <p
              style={{
                fontSize: '26px',
                fontWeight: 300,
                color: color,
                margin: '0 0 6px 0',
                lineHeight: 1,
              }}
            >
              {formatValor(kpi?.valor ?? null, kpi?.unidad ?? null)}
            </p>
            <p style={{ fontSize: '11px', color: '#4a5568', margin: 0 }}>
              {kpi?.unidad ?? ''}
              {kpi?.fecha ? ` · ${formatFecha(kpi.fecha)}` : ''}
            </p>
          </div>
        )
      })}
    </div>
  )
}
