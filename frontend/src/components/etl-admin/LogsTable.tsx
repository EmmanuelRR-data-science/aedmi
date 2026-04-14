'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { apiFetch } from '@/lib/api'

interface ETLLog {
  id: number
  fuente_id: number | null
  fuente_nombre: string | null
  tipo_ejecucion: string
  inicio: string
  fin: string | null
  exitoso: boolean | null
  registros_cargados: number
  errores: number
  mensaje: string | null
  usuario: string | null
}

interface ModuloETL {
  id: number
  nombre: string
}

function ResultBadge({ exitoso }: { exitoso: boolean | null }) {
  if (exitoso === null) {
    return (
      <span style={{ color: '#94a3b8', fontSize: '11px', background: '#1e2235', padding: '2px 8px', borderRadius: '4px' }}>
        En curso
      </span>
    )
  }
  return exitoso ? (
    <span style={{ color: '#36F48C', fontSize: '11px', background: '#0d2b1e', padding: '2px 8px', borderRadius: '4px' }}>
      ✓ OK
    </span>
  ) : (
    <span style={{ color: '#fca5a5', fontSize: '11px', background: '#2d1b1b', padding: '2px 8px', borderRadius: '4px' }}>
      ✗ Error
    </span>
  )
}

export default function LogsTable() {
  const [fuenteId, setFuenteId] = useState<string>('')
  const [fechaDesde, setFechaDesde] = useState<string>('')
  const [exitosoFiltro, setExitosoFiltro] = useState<string>('')

  const { data: modulos = [] } = useQuery<ModuloETL[]>({
    queryKey: ['etl-modulos'],
    queryFn: () => apiFetch<ModuloETL[]>('/admin/etl/modulos'),
  })

  const params = new URLSearchParams()
  if (fuenteId) params.set('fuente_id', fuenteId)
  if (fechaDesde) params.set('fecha_desde', fechaDesde)
  if (exitosoFiltro !== '') params.set('exitoso', exitosoFiltro)
  const query = params.toString() ? `?${params.toString()}` : ''

  const { data: logs = [], isLoading } = useQuery<ETLLog[]>({
    queryKey: ['etl-logs', fuenteId, fechaDesde, exitosoFiltro],
    queryFn: () => apiFetch<ETLLog[]>(`/admin/etl/logs${query}`),
    refetchInterval: 15000,
  })

  const inputStyle: React.CSSProperties = {
    background: '#0f1117',
    border: '1px solid #2d3148',
    borderRadius: '4px',
    color: '#e2e8f0',
    fontSize: '12px',
    fontFamily: 'inherit',
    padding: '5px 8px',
    outline: 'none',
  }

  const thStyle: React.CSSProperties = {
    textAlign: 'left',
    padding: '8px 10px',
    fontSize: '11px',
    color: '#64748b',
    textTransform: 'uppercase',
    letterSpacing: '0.06em',
    borderBottom: '1px solid #2d3148',
    fontWeight: 400,
    whiteSpace: 'nowrap',
  }

  const tdStyle: React.CSSProperties = {
    padding: '8px 10px',
    fontSize: '12px',
    color: '#94a3b8',
    borderBottom: '1px solid #1e2235',
    verticalAlign: 'top',
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
      {/* Filters */}
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px', alignItems: 'flex-end' }}>
        <div>
          <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '3px' }}>
            Módulo
          </label>
          <select
            value={fuenteId}
            onChange={(e) => setFuenteId(e.target.value)}
            style={{ ...inputStyle, minWidth: '160px', cursor: 'pointer' }}
          >
            <option value="">Todos</option>
            {modulos.map((m) => (
              <option key={m.id} value={m.id}>
                {m.nombre}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '3px' }}>
            Desde
          </label>
          <input
            type="date"
            value={fechaDesde}
            onChange={(e) => setFechaDesde(e.target.value)}
            style={inputStyle}
          />
        </div>

        <div>
          <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '3px' }}>
            Resultado
          </label>
          <select
            value={exitosoFiltro}
            onChange={(e) => setExitosoFiltro(e.target.value)}
            style={{ ...inputStyle, cursor: 'pointer' }}
          >
            <option value="">Todos</option>
            <option value="true">Exitoso</option>
            <option value="false">Error</option>
          </select>
        </div>

        <button
          onClick={() => { setFuenteId(''); setFechaDesde(''); setExitosoFiltro('') }}
          style={{
            padding: '5px 12px',
            background: 'transparent',
            border: '1px solid #2d3148',
            borderRadius: '4px',
            color: '#94a3b8',
            fontSize: '11px',
            fontFamily: 'inherit',
            cursor: 'pointer',
          }}
        >
          Limpiar
        </button>
      </div>

      {/* Table */}
      {isLoading ? (
        <p style={{ color: '#4a5568', fontSize: '13px' }}>Cargando logs...</p>
      ) : logs.length === 0 ? (
        <p style={{ color: '#4a5568', fontSize: '13px', padding: '16px 0' }}>
          No hay registros con los filtros seleccionados.
        </p>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr>
                <th style={thStyle}>Módulo</th>
                <th style={thStyle}>Tipo</th>
                <th style={thStyle}>Inicio</th>
                <th style={thStyle}>Fin</th>
                <th style={thStyle}>Resultado</th>
                <th style={thStyle}>Registros</th>
                <th style={thStyle}>Errores</th>
                <th style={thStyle}>Mensaje</th>
                <th style={thStyle}>Usuario</th>
              </tr>
            </thead>
            <tbody>
              {logs.map((log) => (
                <tr
                  key={log.id}
                  style={{
                    background: log.exitoso === false ? 'rgba(127,29,29,0.1)' : 'transparent',
                  }}
                >
                  <td style={{ ...tdStyle, color: '#e2e8f0' }}>{log.fuente_nombre ?? '—'}</td>
                  <td style={tdStyle}>{log.tipo_ejecucion}</td>
                  <td style={{ ...tdStyle, whiteSpace: 'nowrap' }}>
                    {new Date(log.inicio).toLocaleString('es-MX')}
                  </td>
                  <td style={{ ...tdStyle, whiteSpace: 'nowrap' }}>
                    {log.fin ? new Date(log.fin).toLocaleString('es-MX') : '—'}
                  </td>
                  <td style={tdStyle}>
                    <ResultBadge exitoso={log.exitoso} />
                  </td>
                  <td style={{ ...tdStyle, textAlign: 'right' }}>{log.registros_cargados}</td>
                  <td style={{ ...tdStyle, textAlign: 'right', color: log.errores > 0 ? '#fca5a5' : '#94a3b8' }}>
                    {log.errores}
                  </td>
                  <td style={{ ...tdStyle, maxWidth: '280px', wordBreak: 'break-word' }}>
                    {log.mensaje ?? '—'}
                  </td>
                  <td style={tdStyle}>{log.usuario ?? '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
