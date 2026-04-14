'use client'

import { useState } from 'react'
import { useAnalisis, useGenerarAnalisisIA } from '@/hooks/useIndicador'
import { useStyleConfig } from '@/hooks/useStyleConfig'

interface AnalisisIAProps {
  graficaId: number
  entidadClave?: string
  contexto?: Record<string, unknown>
  datosFiltrados?: Record<string, unknown>[]
}

export default function AnalisisIA({
  graficaId,
  entidadClave,
  contexto,
  datosFiltrados,
}: AnalisisIAProps) {
  const { fontFamily } = useStyleConfig()
  const { data: analisis, isLoading } = useAnalisis(graficaId, entidadClave)
  const { mutate: generarIA, isPending, error } = useGenerarAnalisisIA(graficaId, entidadClave)
  const [apiError, setApiError] = useState<string | null>(null)

  function handleAnalizar() {
    setApiError(null)
    generarIA(
      {
        entidadClave,
        contexto,
        datosFiltrados,
      },
      {
      onError: (err) => {
        setApiError(err.message ?? 'Error al generar el análisis. Intenta de nuevo.')
      },
      }
    )
  }

  if (isLoading) return null

  return (
    <div
      style={{
        borderTop: '1px solid #2d3148',
        paddingTop: '16px',
        fontFamily,
      }}
    >
      {/* Existing AI analysis */}
      {analisis?.analisis_ia && (
        <div
          style={{
            background: '#0f1117',
            border: '1px solid #2d3148',
            borderRadius: '6px',
            padding: '14px',
            marginBottom: '12px',
          }}
        >
          <p
            style={{
              fontSize: '11px',
              color: '#0576F3',
              textTransform: 'uppercase',
              letterSpacing: '0.08em',
              margin: '0 0 8px 0',
            }}
          >
            Análisis IA
          </p>
          <p
            style={{
              fontSize: '13px',
              color: '#cbd5e1',
              lineHeight: '1.6',
              margin: 0,
              whiteSpace: 'pre-wrap',
            }}
          >
            {analisis.analisis_ia}
          </p>
          {analisis.ia_generado_at && (
            <p
              style={{
                fontSize: '11px',
                color: '#4a5568',
                margin: '8px 0 0 0',
              }}
            >
              Generado: {new Date(analisis.ia_generado_at).toLocaleString('es-MX')}
            </p>
          )}
        </div>
      )}

      {/* Error message */}
      {(error || apiError) && (
        <div
          role="alert"
          style={{
            background: '#2d1b1b',
            border: '1px solid #7f1d1d',
            borderRadius: '6px',
            padding: '10px 12px',
            color: '#fca5a5',
            fontSize: '12px',
            marginBottom: '12px',
          }}
        >
          {apiError ?? (error instanceof Error ? error.message : 'Error al conectar con la IA.')}
        </div>
      )}

      {/* Analyze button */}
      <button
        onClick={handleAnalizar}
        disabled={isPending}
        style={{
          padding: '8px 16px',
          background: isPending ? '#1e3a6e' : '#0576F3',
          color: '#fff',
          border: 'none',
          borderRadius: '6px',
          fontSize: '12px',
          fontFamily: 'inherit',
          cursor: isPending ? 'not-allowed' : 'pointer',
          transition: 'background 0.2s',
        }}
      >
        {isPending
          ? 'Analizando...'
          : analisis?.analisis_ia
          ? 'Regenerar análisis con IA'
          : 'Analizar con IA'}
      </button>
    </div>
  )
}
