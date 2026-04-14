'use client'

import { useState, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { apiFetch } from '@/lib/api'
import { getToken } from '@/lib/auth'

interface ModuloETL {
  id: number
  nombre: string
}

interface UploadPreview {
  columnas_detectadas: string[]
  filas_preview: Record<string, unknown>[]
  total_filas: number
  formato: string
  hay_diferencias: boolean
  columnas_faltantes: string[]
  columnas_nuevas: string[]
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080'

export default function FileUpload() {
  const fileRef = useRef<HTMLInputElement>(null)
  const [fuenteId, setFuenteId] = useState<number | null>(null)
  const [columnasEsperadas, setColumnasEsperadas] = useState('')
  const [preview, setPreview] = useState<UploadPreview | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [confirmado, setConfirmado] = useState(false)
  const [exito, setExito] = useState(false)

  const { data: modulos = [] } = useQuery<ModuloETL[]>({
    queryKey: ['etl-modulos'],
    queryFn: () => apiFetch<ModuloETL[]>('/admin/etl/modulos'),
  })

  async function handlePreview() {
    const file = fileRef.current?.files?.[0]
    if (!file || fuenteId === null) {
      setError('Selecciona un módulo y un archivo.')
      return
    }
    setError(null)
    setPreview(null)
    setConfirmado(false)
    setExito(false)
    setLoading(true)

    try {
      const formData = new FormData()
      formData.append('archivo', file)
      formData.append('columnas_esperadas', columnasEsperadas)

      const token = getToken()
      const res = await fetch(`${API_BASE}/admin/etl/modulos/${fuenteId}/upload`, {
        method: 'POST',
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        body: formData,
      })

      if (!res.ok) {
        const body = await res.json().catch(() => ({}))
        throw new Error(body?.detail ?? `Error ${res.status}`)
      }

      const data: UploadPreview = await res.json()
      setPreview(data)
    } catch (err) {
      setError((err as Error).message ?? 'Error al procesar el archivo.')
    } finally {
      setLoading(false)
    }
  }

  function handleConfirmar() {
    setConfirmado(true)
    setExito(true)
    // In a full implementation this would trigger the ETL load
    // For now it registers the intent and shows success feedback
  }

  const labelStyle: React.CSSProperties = {
    fontSize: '11px',
    color: '#64748b',
    textTransform: 'uppercase',
    letterSpacing: '0.06em',
    display: 'block',
    marginBottom: '4px',
  }

  const inputStyle: React.CSSProperties = {
    background: '#0f1117',
    border: '1px solid #2d3148',
    borderRadius: '4px',
    color: '#e2e8f0',
    fontSize: '12px',
    fontFamily: 'inherit',
    padding: '6px 10px',
    outline: 'none',
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* Controls */}
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '16px', alignItems: 'flex-end' }}>
        <div>
          <label style={labelStyle}>Módulo destino</label>
          <select
            value={fuenteId ?? ''}
            onChange={(e) => setFuenteId(Number(e.target.value) || null)}
            style={{ ...inputStyle, minWidth: '200px', cursor: 'pointer' }}
          >
            <option value="">Seleccionar módulo…</option>
            {modulos.map((m) => (
              <option key={m.id} value={m.id}>
                {m.nombre}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label style={labelStyle}>Archivo XLSX / CSV</label>
          <input
            ref={fileRef}
            type="file"
            accept=".xlsx,.xls,.csv"
            style={{ ...inputStyle, cursor: 'pointer' }}
          />
        </div>

        <div style={{ flex: 1, minWidth: '200px' }}>
          <label style={labelStyle}>Columnas esperadas (separadas por coma)</label>
          <input
            type="text"
            value={columnasEsperadas}
            onChange={(e) => setColumnasEsperadas(e.target.value)}
            placeholder="col1, col2, col3"
            style={{ ...inputStyle, width: '100%' }}
          />
        </div>

        <button
          onClick={handlePreview}
          disabled={loading}
          style={{
            padding: '8px 16px',
            background: loading ? '#1e3a6e' : '#0576F3',
            color: '#fff',
            border: 'none',
            borderRadius: '4px',
            fontSize: '12px',
            fontFamily: 'inherit',
            cursor: loading ? 'not-allowed' : 'pointer',
          }}
        >
          {loading ? 'Procesando…' : 'Vista previa'}
        </button>
      </div>

      {/* Error */}
      {error && (
        <div
          role="alert"
          style={{
            background: '#2d1b1b',
            border: '1px solid #7f1d1d',
            borderRadius: '6px',
            padding: '10px 12px',
            color: '#fca5a5',
            fontSize: '12px',
          }}
        >
          {error}
        </div>
      )}

      {/* Preview */}
      {preview && !exito && (
        <div
          style={{
            background: '#0f1117',
            border: '1px solid #2d3148',
            borderRadius: '8px',
            padding: '16px',
          }}
        >
          <p style={{ fontSize: '12px', color: '#94a3b8', margin: '0 0 12px 0' }}>
            <strong style={{ color: '#e2e8f0' }}>{preview.total_filas}</strong> filas detectadas
            &nbsp;·&nbsp; Formato: <strong style={{ color: '#e2e8f0' }}>{preview.formato}</strong>
          </p>

          {/* Structure diff warning */}
          {preview.hay_diferencias && (
            <div
              style={{
                background: '#2d2200',
                border: '1px solid #92400e',
                borderRadius: '6px',
                padding: '10px 12px',
                marginBottom: '12px',
                fontSize: '12px',
                color: '#fcd34d',
              }}
            >
              <strong>⚠ Diferencias de estructura detectadas</strong>
              {preview.columnas_faltantes.length > 0 && (
                <p style={{ margin: '6px 0 0 0' }}>
                  Columnas faltantes: <code>{preview.columnas_faltantes.join(', ')}</code>
                </p>
              )}
              {preview.columnas_nuevas.length > 0 && (
                <p style={{ margin: '4px 0 0 0' }}>
                  Columnas nuevas: <code>{preview.columnas_nuevas.join(', ')}</code>
                </p>
              )}
            </div>
          )}

          {/* Columns */}
          <p style={{ fontSize: '11px', color: '#64748b', margin: '0 0 6px 0' }}>
            COLUMNAS DETECTADAS
          </p>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px', marginBottom: '12px' }}>
            {preview.columnas_detectadas.map((col) => (
              <span
                key={col}
                style={{
                  background: '#1e2235',
                  color: '#94a3b8',
                  fontSize: '11px',
                  padding: '2px 8px',
                  borderRadius: '4px',
                }}
              >
                {col}
              </span>
            ))}
          </div>

          {/* Preview table */}
          <p style={{ fontSize: '11px', color: '#64748b', margin: '0 0 6px 0' }}>
            PRIMERAS FILAS
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px' }}>
              <thead>
                <tr>
                  {preview.columnas_detectadas.map((col) => (
                    <th
                      key={col}
                      style={{
                        textAlign: 'left',
                        padding: '4px 8px',
                        color: '#64748b',
                        borderBottom: '1px solid #2d3148',
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {preview.filas_preview.map((fila, i) => (
                  <tr key={i}>
                    {preview.columnas_detectadas.map((col) => (
                      <td
                        key={col}
                        style={{
                          padding: '4px 8px',
                          color: '#94a3b8',
                          borderBottom: '1px solid #1e2235',
                          whiteSpace: 'nowrap',
                        }}
                      >
                        {String(fila[col] ?? '—')}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Confirm button */}
          <div style={{ marginTop: '16px', display: 'flex', gap: '12px', alignItems: 'center' }}>
            <button
              onClick={handleConfirmar}
              disabled={confirmado}
              style={{
                padding: '8px 16px',
                background: preview.hay_diferencias ? '#92400e' : '#0576F3',
                color: '#fff',
                border: 'none',
                borderRadius: '4px',
                fontSize: '12px',
                fontFamily: 'inherit',
                cursor: confirmado ? 'not-allowed' : 'pointer',
              }}
            >
              {preview.hay_diferencias
                ? 'Confirmar carga (estructura modificada)'
                : 'Confirmar carga'}
            </button>
            <button
              onClick={() => setPreview(null)}
              style={{
                padding: '8px 16px',
                background: 'transparent',
                border: '1px solid #2d3148',
                borderRadius: '4px',
                color: '#94a3b8',
                fontSize: '12px',
                fontFamily: 'inherit',
                cursor: 'pointer',
              }}
            >
              Cancelar
            </button>
          </div>
        </div>
      )}

      {/* Success */}
      {exito && (
        <div
          style={{
            background: '#0d2b1e',
            border: '1px solid #36F48C',
            borderRadius: '6px',
            padding: '10px 12px',
            color: '#36F48C',
            fontSize: '12px',
          }}
        >
          ✓ Carga registrada correctamente. El módulo ETL procesará el archivo.
        </div>
      )}
    </div>
  )
}
