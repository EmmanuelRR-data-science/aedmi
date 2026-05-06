'use client'

import { useState, useCallback } from 'react'
import { useStyleConfig } from '@/hooks/useStyleConfig'
import {
  PRESENTACION_LOTE_MAX,
  usePresentationQueue,
} from '@/context/PresentationQueueContext'

const DEFAULT_TITULO = 'AEDMI — Selección de gráficas'

export default function PresentationQueueBar() {
  const { fontFamily } = useStyleConfig()
  const {
    items,
    remove,
    move,
    clear,
    descargarLote,
    descargarLotePptx,
    descargarLoteExcel,
    descargarLoteZip,
    downloading,
    exportingKind,
    lastError,
    lastInfo,
  } = usePresentationQueue()
  const [titulo, setTitulo] = useState(DEFAULT_TITULO)

  const onDescargarGamma = useCallback(() => {
    const t = titulo.trim() || DEFAULT_TITULO
    void descargarLote(t)
  }, [descargarLote, titulo])

  const onDescargarPptx = useCallback(() => {
    const t = titulo.trim() || DEFAULT_TITULO
    void descargarLotePptx(t)
  }, [descargarLotePptx, titulo])

  const onDescargarExcel = useCallback(() => {
    const t = titulo.trim() || DEFAULT_TITULO
    void descargarLoteExcel(t)
  }, [descargarLoteExcel, titulo])

  const onDescargarZip = useCallback(() => {
    const t = titulo.trim() || DEFAULT_TITULO
    void descargarLoteZip(t)
  }, [descargarLoteZip, titulo])

  if (items.length === 0) {
    return (
      <div
        style={{
          position: 'sticky',
          bottom: 0,
          zIndex: 20,
          marginTop: 8,
          padding: '10px 16px',
          background: 'linear-gradient(180deg, rgba(15,17,23,0) 0%, #0f1117 35%)',
          fontFamily,
          fontSize: 12,
          color: '#64748b',
          borderTop: '1px solid #1e2235',
        }}
      >
        Cola vacía. En cada gráfica verás <strong style={{ color: '#94a3b8' }}>+ Fila</strong> y{' '}
        <strong style={{ color: '#94a3b8' }}>↓ PNG</strong>. Al agregar al menos una gráfica a la fila, aquí
        aparecerán las acciones de lote: <strong style={{ color: '#94a3b8' }}>PPTX</strong>,{' '}
        <strong style={{ color: '#94a3b8' }}>Excel</strong>, <strong style={{ color: '#94a3b8' }}>ZIP</strong> y{' '}
        <strong style={{ color: '#94a3b8' }}>Gamma</strong> (máx. {PRESENTACION_LOTE_MAX} gráficas).
      </div>
    )
  }

  return (
    <div
      style={{
        position: 'sticky',
        bottom: 0,
        zIndex: 20,
        marginTop: 8,
        padding: '12px 16px',
        background: '#0f1117',
        borderTop: '1px solid #2d3148',
        fontFamily,
        display: 'flex',
        flexDirection: 'column',
        gap: 10,
        boxShadow: '0 -8px 24px rgba(0,0,0,0.45)',
      }}
    >
      <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 10 }}>
        <span style={{ fontSize: 12, color: '#e2e8f0', fontWeight: 600 }}>
          Presentación ({items.length}/{PRESENTACION_LOTE_MAX})
        </span>
        <input
          type="text"
          value={titulo}
          onChange={(e) => setTitulo(e.target.value)}
          placeholder="Título de la presentación"
          style={{
            flex: '1 1 200px',
            minWidth: 160,
            maxWidth: 400,
            background: '#1a1d27',
            border: '1px solid #2d3148',
            borderRadius: 6,
            color: '#e2e8f0',
            fontSize: 12,
            padding: '6px 10px',
            fontFamily,
          }}
        />
        <button
          type="button"
          onClick={() => {
            if (items.length > 0 && !window.confirm('¿Vaciar toda la cola?')) return
            clear()
          }}
          style={{
            background: 'transparent',
            border: '1px solid #4a2c2c',
            borderRadius: 6,
            color: '#f87171',
            fontSize: 11,
            padding: '6px 10px',
            cursor: 'pointer',
          }}
        >
          Vaciar
        </button>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <span style={{ fontSize: 11, color: '#64748b' }}>Exportación en lote</span>
        <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 8 }}>
          <button
            type="button"
            onClick={() => void onDescargarPptx()}
            disabled={downloading}
            style={{
              background: '#e2e8f0',
              border: '1px solid #cbd5e1',
              borderRadius: 8,
              color: '#0f172a',
              fontWeight: 600,
              fontSize: 12,
              padding: '8px 16px',
              cursor: downloading ? 'wait' : 'pointer',
              opacity: downloading ? 0.7 : 1,
            }}
          >
            {exportingKind === 'pptx' ? 'Generando PPTX…' : 'Descargar PPTX (lote)'}
          </button>
          <button
            type="button"
            onClick={() => void onDescargarExcel()}
            disabled={downloading}
            style={{
              background: '#14532d',
              border: '1px solid #22c55e',
              borderRadius: 8,
              color: '#ecfccb',
              fontWeight: 600,
              fontSize: 12,
              padding: '8px 16px',
              cursor: downloading ? 'wait' : 'pointer',
              opacity: downloading ? 0.7 : 1,
            }}
          >
            {exportingKind === 'xlsx' ? 'Generando Excel…' : 'Descargar Excel (lote)'}
          </button>
          <button
            type="button"
            onClick={() => void onDescargarZip()}
            disabled={downloading}
            style={{
              background: '#1e293b',
              border: '1px solid #475569',
              borderRadius: 8,
              color: '#e2e8f0',
              fontWeight: 600,
              fontSize: 12,
              padding: '8px 16px',
              cursor: downloading ? 'wait' : 'pointer',
              opacity: downloading ? 0.7 : 1,
            }}
          >
            {exportingKind === 'zip' ? 'Generando ZIP…' : 'ZIP (PPTX + Excel)'}
          </button>
          <button
            type="button"
            onClick={() => void onDescargarGamma()}
            disabled={downloading}
            style={{
              background: 'linear-gradient(90deg, #0576F3, #36F48C)',
              border: 'none',
              borderRadius: 8,
              color: '#0f1117',
              fontWeight: 600,
              fontSize: 12,
              padding: '8px 16px',
              cursor: downloading ? 'wait' : 'pointer',
              opacity: downloading ? 0.7 : 1,
            }}
          >
            {exportingKind === 'gamma' ? 'Generando en Gamma…' : 'Generar en Gamma (lote)'}
          </button>
        </div>
      </div>

      {lastError && (
        <p style={{ margin: 0, fontSize: 11, color: '#f87171' }} role="alert">
          {lastError}
        </p>
      )}
      {lastInfo && (
        <p style={{ margin: 0, fontSize: 11, color: '#4ade80' }} role="status">
          {lastInfo}
        </p>
      )}

      <ul
        style={{
          listStyle: 'none',
          margin: 0,
          padding: 0,
          display: 'flex',
          flexDirection: 'column',
          gap: 6,
          maxHeight: 200,
          overflowY: 'auto',
        }}
      >
        {items.map((it, i) => (
          <li
            key={it.id}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 8,
              background: '#1a1d27',
              border: '1px solid #2d3148',
              borderRadius: 6,
              padding: '6px 10px',
              fontSize: 11,
              color: '#94a3b8',
            }}
          >
            <span style={{ color: '#64748b', width: 20 }}>{i + 1}.</span>
            <span style={{ flex: 1, color: '#e2e8f0', minWidth: 0 }}>
              {it.titulo}
              {it.entidadClave && (
                <span style={{ color: '#64748b' }}> — {it.entidadClave}</span>
              )}
            </span>
            <span style={{ color: '#64748b', fontSize: 10, textTransform: 'uppercase' }}>
              {it.nivelGeografico}
            </span>
            <div style={{ display: 'flex', gap: 4 }}>
              <button
                type="button"
                title="Subir"
                onClick={() => move(it.id, 'up')}
                disabled={i === 0}
                style={smallBtn(!!(i === 0))}
              >
                ↑
              </button>
              <button
                type="button"
                title="Bajar"
                onClick={() => move(it.id, 'down')}
                disabled={i === items.length - 1}
                style={smallBtn(i === items.length - 1)}
              >
                ↓
              </button>
              <button
                type="button"
                title="Quitar"
                onClick={() => remove(it.id)}
                style={{ ...smallBtn(false), color: '#f87171' }}
              >
                ✕
              </button>
            </div>
          </li>
        ))}
      </ul>
    </div>
  )
}

function smallBtn(disabled: boolean) {
  return {
    background: 'transparent',
    border: '1px solid #2d3148',
    borderRadius: 4,
    color: '#94a3b8',
    fontSize: 12,
    width: 28,
    height: 28,
    padding: 0,
    cursor: disabled ? 'not-allowed' as const : 'pointer' as const,
    opacity: disabled ? 0.4 : 1,
  }
}
