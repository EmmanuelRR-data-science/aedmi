'use client'

import { useState, useCallback, type RefObject } from 'react'
import { useStyleConfig } from '@/hooks/useStyleConfig'
import {
  PRESENTACION_LOTE_MAX,
  usePresentationQueue,
  type DatoSeriePresentacion,
  type ExcelChartKind,
} from '@/context/PresentationQueueContext'
import type { TabGeografico } from '@/types'

type Props = {
  chartRef: RefObject<HTMLDivElement | null>
  indicadorId: number
  nivelGeografico: TabGeografico
  entidadClave: string | null
  titulo: string
  subtitulo: string
  /** Serie exacta mostrada en pantalla (recomendado para Gamma). */
  datosSerie?: DatoSeriePresentacion[]
  /** Mismo texto que el pie "Fuente:" de la gráfica (PPTX / consistencia). */
  leyendaFuente?: string
  /** @default true */
  compact?: boolean
  excelChartKind?: ExcelChartKind
}

export default function AddToPresentationButton({
  chartRef,
  indicadorId,
  nivelGeografico,
  entidadClave,
  titulo,
  subtitulo,
  datosSerie,
  leyendaFuente,
  compact = true,
  excelChartKind = 'column',
}: Props) {
  const { fontFamily } = useStyleConfig()
  const { addFromRef, items } = usePresentationQueue()
  const [adding, setAdding] = useState(false)
  const [hint, setHint] = useState<string | null>(null)

  const onAdd = useCallback(async () => {
    setHint(null)
    setAdding(true)
    try {
      const res = await addFromRef({
        chartRef,
        indicadorId,
        nivelGeografico,
        entidadClave,
        titulo,
        subtitulo,
        ...(datosSerie?.length ? { datosSerie } : {}),
        ...(leyendaFuente?.trim() ? { leyendaFuente: leyendaFuente.trim() } : {}),
        excelChartKind,
      })
      if (res.ok) {
        setHint('Añadida a la fila')
        window.setTimeout(() => setHint(null), 2500)
      } else {
        setHint(res.reason)
      }
    } finally {
      setAdding(false)
    }
  }, [
    addFromRef,
    chartRef,
    indicadorId,
    nivelGeografico,
    entidadClave,
    titulo,
    subtitulo,
    datosSerie,
    leyendaFuente,
    excelChartKind,
  ])

  const full = items.length >= PRESENTACION_LOTE_MAX
  const disabled = adding || full

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: compact ? 'flex-end' : 'center',
        justifyContent: 'center',
        gap: 2,
        flexShrink: 0,
        zIndex: 2,
      }}
    >
      <button
        type="button"
        onClick={() => void onAdd()}
        disabled={disabled}
        title={
          full
            ? `Límite de ${PRESENTACION_LOTE_MAX} gráficas alcanzado`
            : 'Añadir esta gráfica a la fila (luego podrás exportar PPTX, Excel, ZIP o Gamma desde la barra inferior)'
        }
        style={{
          position: compact ? 'absolute' : 'relative',
          right: compact ? 88 : undefined,
          top: compact ? 0 : undefined,
          background: 'transparent',
          border: '1px solid #2d3148',
          borderRadius: '4px',
          color: full ? '#4a5568' : '#a8b2c3',
          fontSize: '11px',
          fontWeight: 600,
          fontFamily,
          padding: '4px 10px',
          cursor: disabled ? 'not-allowed' : 'pointer',
          display: 'flex',
          alignItems: 'center',
          gap: '4px',
          zIndex: 2,
        }}
        onMouseEnter={(e) => {
          if (!disabled) {
            e.currentTarget.style.borderColor = '#36F48C'
            e.currentTarget.style.color = '#36F48C'
          }
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.borderColor = '#2d3148'
          e.currentTarget.style.color = full ? '#4a5568' : '#a8b2c3'
        }}
      >
        {adding ? '…' : '+ Fila'}
      </button>
      {hint && (
        <span
          style={{
            fontSize: 10,
            color: hint.startsWith('Añadida a la fila') ? '#36F48C' : '#f87171',
            fontFamily,
            maxWidth: 140,
            textAlign: 'right',
            lineHeight: 1.2,
          }}
        >
          {hint}
        </span>
      )}
    </div>
  )
}
