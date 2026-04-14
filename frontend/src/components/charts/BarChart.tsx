'use client'

import {
  BarChart as RechartsBarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import { getColorForIndex } from '@/hooks/useStyleConfig'
import type { ChartDataPoint } from '@/types'

interface BarChartProps {
  data: ChartDataPoint[]
  title: string
  palette: string[]
  fontFamily: string
  titleSize: number
  xAxisSize: number
  yAxisSize: number
  /** Keys to render as bars (defaults to ['value']) */
  dataKeys?: string[]
  unit?: string
}

export default function BarChart({
  data,
  title,
  palette,
  fontFamily,
  titleSize,
  xAxisSize,
  yAxisSize,
  dataKeys = ['value'],
  unit,
}: BarChartProps) {
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
        <RechartsBarChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
          <XAxis
            dataKey="name"
            tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }}
            axisLine={{ stroke: '#2d3148' }}
            tickLine={false}
          />
          <YAxis
            tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }}
            axisLine={{ stroke: '#2d3148' }}
            tickLine={false}
            unit={unit}
          />
          <Tooltip
            contentStyle={{
              background: '#1a1d27',
              border: '1px solid #2d3148',
              borderRadius: '6px',
              fontFamily,
              fontSize: '12px',
            }}
            labelStyle={{ color: '#e2e8f0' }}
            itemStyle={{ color: '#94a3b8' }}
          />
          {dataKeys.length > 1 && (
            <Legend
              wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8' }}
            />
          )}
          {dataKeys.map((key, i) => (
            <Bar
              key={key}
              dataKey={key}
              fill={getColorForIndex(palette, i)}
              radius={[3, 3, 0, 0]}
            />
          ))}
        </RechartsBarChart>
      </ResponsiveContainer>
    </div>
  )
}
