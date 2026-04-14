'use client'

import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiFetch } from '@/lib/api'

interface ModuloETL {
  id: number
  nombre: string
  fuente: string
  periodicidad: string
  ultima_ejecucion: string | null
  exitoso: boolean | null
  estado: string
}

function StatusBadge({ exitoso }: { exitoso: boolean | null }) {
  if (exitoso === null) {
    return (
      <span style={{ color: '#94a3b8', fontSize: '11px', background: '#1e2235', padding: '2px 8px', borderRadius: '4px' }}>
        Pendiente
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

export default function ModulosList() {
  const queryClient = useQueryClient()
  const [ejecutando, setEjecutando] = useState<number | null>(null)

  const { data: modulos = [], isLoading } = useQuery<ModuloETL[]>({
    queryKey: ['etl-modulos'],
    queryFn: () => apiFetch<ModuloETL[]>('/admin/etl/modulos'),
    refetchInterval: 15000,
  })

  const { mutate: ejecutar } = useMutation({
    mutationFn: (fuente_id: number) =>
      apiFetch(`/admin/etl/modulos/${fuente_id}/ejecutar`, { method: 'POST' }),
    onMutate: (id) => setEjecutando(id),
    onSettled: () => {
      setEjecutando(null)
      queryClient.invalidateQueries({ queryKey: ['etl-modulos'] })
      queryClient.invalidateQueries({ queryKey: ['etl-logs'] })
    },
  })

  const thStyle: React.CSSProperties = {
    textAlign: 'left',
    padding: '8px 12px',
    fontSize: '11px',
    color: '#64748b',
    textTransform: 'uppercase',
    letterSpacing: '0.06em',
    borderBottom: '1px solid #2d3148',
    fontWeight: 400,
  }

  const tdStyle: React.CSSProperties = {
    padding: '10px 12px',
    fontSize: '12px',
    color: '#94a3b8',
    borderBottom: '1px solid #1e2235',
    verticalAlign: 'middle',
  }

  if (isLoading) {
    return <p style={{ color: '#4a5568', fontSize: '13px' }}>Cargando módulos...</p>
  }

  if (modulos.length === 0) {
    return (
      <p style={{ color: '#4a5568', fontSize: '13px', padding: '24px 0' }}>
        No hay módulos ETL registrados aún.
      </p>
    )
  }

  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
        <thead>
          <tr>
            <th style={thStyle}>Módulo</th>
            <th style={thStyle}>Fuente</th>
            <th style={thStyle}>Periodicidad</th>
            <th style={thStyle}>Última ejecución</th>
            <th style={thStyle}>Resultado</th>
            <th style={thStyle}>Estado</th>
            <th style={thStyle}>Acción</th>
          </tr>
        </thead>
        <tbody>
          {modulos.map((m) => (
            <tr
              key={m.id}
              style={{
                background: m.exitoso === false ? 'rgba(127,29,29,0.15)' : 'transparent',
              }}
            >
              <td style={{ ...tdStyle, color: '#e2e8f0' }}>{m.nombre}</td>
              <td style={tdStyle}>
                {m.fuente ? (
                  <a
                    href={m.fuente}
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{ color: '#0576F3', textDecoration: 'none' }}
                  >
                    {m.fuente.length > 40 ? m.fuente.slice(0, 40) + '…' : m.fuente}
                  </a>
                ) : (
                  '—'
                )}
              </td>
              <td style={tdStyle}>{m.periodicidad}</td>
              <td style={tdStyle}>
                {m.ultima_ejecucion
                  ? new Date(m.ultima_ejecucion).toLocaleString('es-MX')
                  : '—'}
              </td>
              <td style={tdStyle}>
                <StatusBadge exitoso={m.exitoso} />
              </td>
              <td style={tdStyle}>
                <span
                  style={{
                    fontSize: '11px',
                    color: '#64748b',
                    background: '#1e2235',
                    padding: '2px 8px',
                    borderRadius: '4px',
                  }}
                >
                  {m.estado}
                </span>
              </td>
              <td style={tdStyle}>
                <button
                  onClick={() => ejecutar(m.id)}
                  disabled={ejecutando === m.id}
                  style={{
                    padding: '5px 12px',
                    background: ejecutando === m.id ? '#1e3a6e' : 'transparent',
                    border: '1px solid #0576F3',
                    borderRadius: '4px',
                    color: '#0576F3',
                    fontSize: '11px',
                    fontFamily: 'inherit',
                    cursor: ejecutando === m.id ? 'not-allowed' : 'pointer',
                  }}
                >
                  {ejecutando === m.id ? 'Ejecutando…' : 'Ejecutar'}
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
