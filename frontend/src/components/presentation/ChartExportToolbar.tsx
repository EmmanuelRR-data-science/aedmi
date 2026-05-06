'use client'

import type { ReactNode, RefObject } from 'react'
import { useStyleConfig } from '@/hooks/useStyleConfig'
import AddToPresentationButton from '@/components/presentation/AddToPresentationButton'
import type { DatoSeriePresentacion, ExcelChartKind } from '@/context/PresentationQueueContext'
import type { TabGeografico } from '@/types'

export type ChartExportToolbarProps = {
  chartRef: RefObject<HTMLDivElement | null>
  title: ReactNode
  indicadorId: number | null | undefined
  nivelGeografico: TabGeografico
  entidadClave: string | null
  titulo: string
  subtitulo: string
  datosSerie?: DatoSeriePresentacion[]
  leyendaFuente?: string
  excelChartKind?: ExcelChartKind
  onDownloadPng: () => void
  pngTitle?: string
}

export default function ChartExportToolbar({
  chartRef,
  title,
  indicadorId,
  nivelGeografico,
  entidadClave,
  titulo,
  subtitulo,
  datosSerie,
  leyendaFuente,
  excelChartKind = 'column',
  onDownloadPng,
  pngTitle = 'Descargar gráfica en alta resolución',
}: ChartExportToolbarProps) {
  const { fontFamily, titleSize } = useStyleConfig()

  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 2fr) minmax(0, 1fr)',
        alignItems: 'center',
        gap: 8,
        width: '100%',
        position: 'relative',
      }}
    >
      <span style={{ minWidth: 0 }} />
      <div
        style={{
          fontSize: `${titleSize}px`,
          fontFamily,
          color: '#e2e8f0',
          fontWeight: 700,
          textAlign: 'center',
          minWidth: 0,
          margin: 0,
        }}
      >
        {title}
      </div>
      <div
        style={{
          display: 'flex',
          justifyContent: 'flex-end',
          alignItems: 'center',
          gap: 8,
          flexShrink: 0,
          zIndex: 2,
        }}
      >
        {indicadorId != null && (
          <AddToPresentationButton
            chartRef={chartRef}
            indicadorId={indicadorId}
            nivelGeografico={nivelGeografico}
            entidadClave={entidadClave}
            titulo={titulo}
            subtitulo={subtitulo}
            datosSerie={datosSerie}
            leyendaFuente={leyendaFuente}
            compact={false}
            excelChartKind={excelChartKind}
          />
        )}
        <button
          type="button"
          onClick={onDownloadPng}
          title={pngTitle}
          style={{
            position: 'relative',
            right: 0,
            background: 'transparent',
            border: '1px solid #2d3148',
            borderRadius: '4px',
            color: '#64748b',
            fontSize: '11px',
            fontFamily,
            padding: '4px 10px',
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: '4px',
            flexShrink: 0,
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.borderColor = '#0576F3'
            e.currentTarget.style.color = '#0576F3'
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.borderColor = '#2d3148'
            e.currentTarget.style.color = '#64748b'
          }}
        >
          ↓ PNG
        </button>
      </div>
    </div>
  )
}
