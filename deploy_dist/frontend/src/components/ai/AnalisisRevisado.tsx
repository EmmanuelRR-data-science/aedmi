'use client'

import { useState, useEffect } from 'react'
import { useAnalisis, useGuardarAnalisisRevisado } from '@/hooks/useIndicador'
import { useStyleConfig } from '@/hooks/useStyleConfig'

interface AnalisisRevisadoProps {
  graficaId: number
  entidadClave?: string
}

export default function AnalisisRevisado({ graficaId, entidadClave }: AnalisisRevisadoProps) {
  const { fontFamily } = useStyleConfig()
  const { data: analisis, isLoading } = useAnalisis(graficaId, entidadClave)
  const { mutate: guardar, isPending, isSuccess, error } = useGuardarAnalisisRevisado(graficaId, entidadClave)

  const [texto, setTexto] = useState('')
  const [saved, setSaved] = useState(false)

  // Pre-populate textarea when data loads
  useEffect(() => {
    if (analisis?.analisis_revisado) {
      setTexto(analisis.analisis_revisado)
    }
  }, [analisis?.analisis_revisado])

  // Show brief success feedback
  useEffect(() => {
    if (isSuccess) {
      setSaved(true)
      const t = setTimeout(() => setSaved(false), 2500)
      return () => clearTimeout(t)
    }
  }, [isSuccess])

  if (isLoading) return null

  function handleGuardar() {
    guardar(texto)
  }

  return (
    <div
      style={{
        borderTop: '1px solid #2d3148',
        paddingTop: '16px',
        fontFamily,
      }}
    >
      <p
        style={{
          fontSize: '11px',
          color: '#36F48C',
          textTransform: 'uppercase',
          letterSpacing: '0.08em',
          margin: '0 0 8px 0',
        }}
      >
        Análisis revisado
      </p>

      <textarea
        value={texto}
        onChange={(e) => setTexto(e.target.value)}
        placeholder="Escribe aquí tu interpretación experta de los datos..."
        rows={5}
        style={{
          width: '100%',
          background: '#0f1117',
          border: '1px solid #2d3148',
          borderRadius: '6px',
          color: '#e2e8f0',
          fontSize: '13px',
          fontFamily: 'inherit',
          padding: '10px 12px',
          resize: 'vertical',
          outline: 'none',
          lineHeight: '1.6',
          boxSizing: 'border-box',
        }}
      />

      {/* Error */}
      {error && (
        <div
          role="alert"
          style={{
            background: '#2d1b1b',
            border: '1px solid #7f1d1d',
            borderRadius: '6px',
            padding: '8px 12px',
            color: '#fca5a5',
            fontSize: '12px',
            marginTop: '8px',
          }}
        >
          {error instanceof Error ? error.message : 'Error al guardar el análisis.'}
        </div>
      )}

      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '12px',
          marginTop: '10px',
        }}
      >
        <button
          onClick={handleGuardar}
          disabled={isPending || texto.trim() === ''}
          style={{
            padding: '8px 16px',
            background: isPending ? '#1a3d2b' : '#36F48C',
            color: '#0f1117',
            border: 'none',
            borderRadius: '6px',
            fontSize: '12px',
            fontFamily: 'inherit',
            fontWeight: 600,
            cursor: isPending || texto.trim() === '' ? 'not-allowed' : 'pointer',
            transition: 'background 0.2s',
          }}
        >
          {isPending ? 'Guardando...' : 'Guardar análisis'}
        </button>

        {saved && (
          <span style={{ fontSize: '12px', color: '#36F48C' }}>
            ✓ Guardado correctamente
          </span>
        )}

        {analisis?.revisado_at && (
          <span style={{ fontSize: '11px', color: '#4a5568', marginLeft: 'auto' }}>
            Última actualización: {new Date(analisis.revisado_at).toLocaleString('es-MX')}
          </span>
        )}
      </div>
    </div>
  )
}
