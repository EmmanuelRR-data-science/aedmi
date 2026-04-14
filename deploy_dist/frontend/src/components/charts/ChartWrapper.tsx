'use client'

import { useStyleConfig } from '@/hooks/useStyleConfig'
import AnalisisIA from '@/components/ai/AnalisisIA'
import AnalisisRevisado from '@/components/ai/AnalisisRevisado'
import type { DatoIndicador } from '@/types'

type ChartType = 'bar' | 'line' | 'pie'

interface ChartWrapperProps {
  indicadorId: number
  title: string
  datos: DatoIndicador[]
  chartType?: ChartType
  unit?: string
}

export default function ChartWrapper({
  indicadorId,
  title,
  datos,
  chartType = 'bar',
  unit,
}: ChartWrapperProps) {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()

  // Transform DatoIndicador[] into recharts-friendly format
  const chartData = datos.map((d) => ({
    name: String(d.periodo),
    value: d.valor,
  }))

  const sharedProps = {
    data: chartData,
    title,
    palette,
    fontFamily,
    titleSize,
    xAxisSize,
    yAxisSize,
    unit,
  }

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
      {/* Chart */}
      <ChartRenderer type={chartType} {...sharedProps} />

      {/* Data table */}
      {datos.length > 0 && (
        <DataTable datos={datos} unit={unit} fontFamily={fontFamily} />
      )}

      {/* AI analysis section */}
      <AnalisisIA graficaId={indicadorId} />

      {/* Revised analysis section */}
      <AnalisisRevisado graficaId={indicadorId} />
    </div>
  )
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

interface ChartRendererProps {
  type: ChartType
  data: { name: string; value: number }[]
  title: string
  palette: string[]
  fontFamily: string
  titleSize: number
  xAxisSize: number
  yAxisSize: number
  unit?: string
}

function ChartRenderer({ type, ...props }: ChartRendererProps) {
  // Dynamic imports to avoid SSR issues with recharts
  if (typeof window === 'undefined') return null

  // We use lazy inline requires to keep the bundle split
  if (type === 'line') {
    const LineChart = require('./LineChart').default
    return <LineChart {...props} />
  }
  if (type === 'pie') {
    const PieChart = require('./PieChart').default
    return <PieChart {...props} />
  }
  const BarChart = require('./BarChart').default
  return <BarChart {...props} />
}

interface DataTableProps {
  datos: DatoIndicador[]
  unit?: string
  fontFamily: string
}

function DataTable({ datos, unit, fontFamily }: DataTableProps) {
  return (
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
            <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Período</th>
            {datos[0]?.entidad_clave && (
              <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Entidad</th>
            )}
            <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>
              Valor{unit ? ` (${unit})` : ''}
            </th>
          </tr>
        </thead>
        <tbody>
          {datos.map((d) => (
            <tr
              key={d.id}
              style={{ borderBottom: '1px solid #1e2235' }}
            >
              <td style={{ padding: '5px 8px' }}>{String(d.periodo)}</td>
              {d.entidad_clave && (
                <td style={{ padding: '5px 8px' }}>{d.entidad_clave}</td>
              )}
              <td style={{ padding: '5px 8px', textAlign: 'right' }}>
                {d.valor.toLocaleString('es-MX')}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
