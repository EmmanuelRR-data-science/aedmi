'use client'

import { useEffect, useState } from 'react'
import { useStyleConfig } from '@/hooks/useStyleConfig'
import { getToken } from '@/lib/auth'

interface Props {
  estado: string
}

interface RedCarreteraInfo {
  estado: string
  pdf_url: string
  fuente: string
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080'

export default function RedCarreteraEstatalChart({ estado }: Props) {
  const { fontFamily, titleSize } = useStyleConfig()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [info, setInfo] = useState<RedCarreteraInfo | null>(null)
  const [pngUrl, setPngUrl] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    let objectUrl: string | null = null

    async function load() {
      setLoading(true)
      setError(null)
      setInfo(null)
      if (pngUrl) {
        URL.revokeObjectURL(pngUrl)
        setPngUrl(null)
      }

      try {
        const token = getToken()
        const headers: HeadersInit = token ? { Authorization: `Bearer ${token}` } : {}

        const infoResp = await fetch(`${API_BASE}/infraestructura/red-carretera/${encodeURIComponent(estado)}`, { headers })
        if (!infoResp.ok) throw new Error(`Error ${infoResp.status} al obtener metadatos`)
        const metadata = await infoResp.json() as RedCarreteraInfo
        if (cancelled) return
        setInfo(metadata)

        const pngResp = await fetch(`${API_BASE}/infraestructura/red-carretera/${encodeURIComponent(estado)}/png`, { headers })
        if (!pngResp.ok) throw new Error(`Error ${pngResp.status} al generar PNG`)
        const blob = await pngResp.blob()
        objectUrl = URL.createObjectURL(blob)
        if (cancelled) {
          URL.revokeObjectURL(objectUrl)
          return
        }
        setPngUrl(objectUrl)
      } catch (e) {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : 'Error desconocido')
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    load()
    return () => {
      cancelled = true
      if (objectUrl) URL.revokeObjectURL(objectUrl)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [estado])

  const downloadPng = () => {
    if (!pngUrl) return
    const link = document.createElement('a')
    link.href = pngUrl
    link.download = `red-carretera-${estado.toLowerCase().replace(/ /g, '-')}.png`
    link.click()
  }

  if (loading) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>
        Cargando mapa carretero...
      </div>
    )
  }

  if (error || !info || !pngUrl) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', color: '#4a5568', fontSize: '13px', fontFamily, textAlign: 'center', paddingTop: '60px', paddingBottom: '60px' }}>
        No fue posible cargar la red carretera de {estado}. {error ? `(${error})` : ''}
      </div>
    )
  }

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
        <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
          Red carretera estatal ({estado})
        </p>
        <button onClick={downloadPng} title="Descargar"
          style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
          onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
          onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}>↓ PNG</button>
      </div>

      <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
        <a href={info.pdf_url} target="_blank" rel="noreferrer"
          style={{ textDecoration: 'none', border: '1px solid #2d3148', color: '#94a3b8', padding: '6px 12px', borderRadius: '4px', fontSize: '12px', fontFamily }}>
          Ver PDF original
        </a>
      </div>

      <div style={{ border: '1px solid #2d3148', borderRadius: '8px', overflow: 'hidden', background: '#0f1117' }}>
        <img src={pngUrl} alt={`Mapa carretero de ${estado}`} style={{ width: '100%', height: 'auto', display: 'block' }} />
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
        Fuente: {info.fuente} (PDF oficial convertido a PNG para visualización).
      </p>
    </div>
  )
}
