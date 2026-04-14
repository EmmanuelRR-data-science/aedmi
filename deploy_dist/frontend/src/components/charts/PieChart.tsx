'use client'

import {
  PieChart as RechartsPieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import { getColorForIndex } from '@/hooks/useStyleConfig'
import type { ChartDataPoint } from '@/types'

interface PieChartProps {
  data: ChartDataPoint[]
  title: string
  palette: string[]
  fontFamily: string
  titleSize: number
  xAxisSize: number
  yAxisSize: number
  unit?: string
}

export default function PieChart({
  data,
  title,
  palette,
  fontFamily,
  titleSize,
  xAxisSize,
  unit,
}: PieChartProps) {
  if (!data || data.length === 0) {
    return (
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          height: '200px',
          color: '#4a5568',
          fontSize: '13px',
          fontFamily,
        }}
      >
        No hay datos disponibles para este indicador.
      </div>
    )
  }

  return (
    <div>
      <p
        style={{
          fontSize: `${titleSize}px`,
          fontFamily,
          color: '#e2e8f0',
          margin: '0 0 12px 0',
          fontWeight: 400,
        }}
      >
        {title}
      </p>
      <ResponsiveContainer width="100%" height={280}>
        <RechartsPieChart>
          <Pie
            data={data}
            dataKey="value"
            nameKey="name"
            cx="50%"
            cy="50%"
            outerRadius={100}
            label={({ name, percent }) =>
              `${name}: ${(percent * 100).toFixed(1)}%`
            }
            labelLine={false}
          >
            {data.map((_, i) => (
              <Cell key={`cell-${i}`} fill={getColorForIndex(palette, i)} />
            ))}
          </Pie>
          <Tooltip
            contentStyle={{
              background: '#1a1d27',
              border: '1px solid #2d3148',
              borderRadius: '6px',
              fontFamily,
              fontSize: '12px',
            }}
            formatter={(value: number) => [`${value}${unit ? ` ${unit}` : ''}`, '']}
            labelStyle={{ color: '#e2e8f0' }}
            itemStyle={{ color: '#94a3b8' }}
          />
          <Legend
            wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8' }}
          />
        </RechartsPieChart>
      </ResponsiveContainer>
    </div>
  )
}
