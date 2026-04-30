'use client'

import { useState, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useStyleConfig } from '@/hooks/useStyleConfig'
import { apiFetchBlob, ApiError } from '@/lib/api'
import { textoAnalisisDesdeQueryCache } from '@/lib/analisisCache'
import type { TabGeografico } from '@/types'
import type { DatoSeriePresentacion, ExcelChartKind } from '@/context/PresentationQueueContext'

type Props = {
  indicadorId: number
  nivelGeografico: TabGeografico
  entidadClave: string | null
  titulo: string
  subtitulo: string
  datosSerie?: DatoSeriePresentacion[]
  leyendaFuente?: string
  excelChartKind?: ExcelChartKind
}

function slugBase(titulo: string) {
  return (titulo.trim() || 'aedmi_grafica').replace(/[^\w\d._-]+/g, '_').slice(0, 64) || 'aedmi_grafica'
}

export default function ExportChartExcelButton({
  indicadorId,
  nivelGeografico,
  entidadClave,
  titulo,
  subtitulo,
  datosSerie,
  leyendaFuente,
  excelChartKind = 'column',
}: Props) {
  const { fontFamily } = useStyleConfig()
  const queryClient = useQueryClient()
  const [loading, setLoading] = useState(false)
  const [hint, setHint] = useState<string | null>(null)

  const onExcel = useCallback(async () => {
    if (!datosSerie?.length) {
      setHint('Sin datos de serie para Excel.')
      return
    }
    setHint(null)
    setLoading(true)
    try {
      const textoAnalisis = textoAnalisisDesdeQueryCache(
        queryClient,
        indicadorId,
        entidadClave
      )
      const blob = await apiFetchBlob('/export/presentacion/lote', {
        method: 'POST',
        body: JSON.stringify({
          titulo_presentacion: titulo,
          modo_salida: 'xlsx',
          items: [
            {
              grafica_id: indicadorId,
              titulo,
              nivel_geografico: nivelGeografico,
              entidad_clave: entidadClave,
              imagen_grafica_png_base64: null,
              subtitulo_contexto: subtitulo,
              datos_serie: datosSerie.map((d) => ({
                periodo: d.periodo,
                valor: d.valor,
                entidad_clave: d.entidad_clave ?? null,
                unidad: d.unidad ?? null,
              })),
              excel_chart_kind: excelChartKind,
              ...(textoAnalisis ? { texto_analisis: textoAnalisis } : {}),
              ...(leyendaFuente?.trim() ? { leyenda_fuente: leyendaFuente.trim() } : {}),
            },
          ],
        }),
      })
      const base = slugBase(titulo)
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `${base}.xlsx`
      a.rel = 'noopener'
      a.click()
      URL.revokeObjectURL(url)
      setHint('Excel descargado.')
      window.setTimeout(() => setHint(null), 2500)
    } catch (e) {
      if (e instanceof ApiError) {
        setHint(e.message)
      } else {
        setHint('Error al generar Excel.')
      }
    } finally {
      setLoading(false)
    }
  }, [
    datosSerie,
    entidadClave,
    excelChartKind,
    indicadorId,
    leyendaFuente,
    nivelGeografico,
    queryClient,
    subtitulo,
    titulo,
  ])

  const disabled = loading || !datosSerie?.length

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'flex-end',
        justifyContent: 'center',
        gap: 2,
        flexShrink: 0,
        zIndex: 2,
      }}
    >
      <button
        type="button"
        onClick={() => void onExcel()}
        disabled={disabled}
        title={
          !datosSerie?.length
            ? 'Añade datos visibles (serie) para exportar a Excel'
            : 'Descargar Excel con datos y gráfico nativo'
        }
        style={{
          background: 'transparent',
          border: '1px solid #2d3148',
          borderRadius: '4px',
          color: disabled ? '#4a5568' : '#94a3b8',
          fontSize: '11px',
          fontWeight: 600,
          fontFamily,
          padding: '4px 10px',
          cursor: disabled ? 'not-allowed' : 'pointer',
          display: 'flex',
          alignItems: 'center',
          gap: '4px',
          flexShrink: 0,
        }}
        onMouseEnter={(e) => {
          if (!disabled) {
            e.currentTarget.style.borderColor = '#22c55e'
            e.currentTarget.style.color = '#22c55e'
          }
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.borderColor = '#2d3148'
          e.currentTarget.style.color = disabled ? '#4a5568' : '#94a3b8'
        }}
      >
        {loading ? '…' : '↓ XLSX'}
      </button>
      {hint && (
        <span
          style={{
            fontSize: 10,
            color: hint.includes('descargado') ? '#4ade80' : '#f87171',
            fontFamily,
            maxWidth: 160,
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
