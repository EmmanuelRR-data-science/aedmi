'use client'

import Image from 'next/image'
import { useRouter } from 'next/navigation'
import { removeToken } from '@/lib/auth'
import { apiFetch } from '@/lib/api'

export default function TopBar() {
  const router = useRouter()

  async function handleLogout() {
    try {
      await apiFetch('/auth/logout', { method: 'POST' })
    } catch {
      // Even if the API call fails, clear the local token
    } finally {
      removeToken()
      router.push('/login')
    }
  }

  return (
    <header
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '12px 24px',
        background: '#1a1d27',
        borderBottom: '1px solid #2d3148',
        height: '64px',
      }}
    >
      {/* Left: logo + title */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
        <Image
          src="/logo/phiqus_logo_positivo.png"
          alt="PhiQus"
          width={100}
          height={32}
          style={{ objectFit: 'contain' }}
          priority
        />
        <span
          style={{
            fontSize: '15px',
            fontWeight: 300,
            color: '#e2e8f0',
            letterSpacing: '0.02em',
          }}
        >
          Aplicación para estudios de Mercado
        </span>
      </div>

      {/* Right: admin link + logout */}
      <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
        <a
          href="/admin/etl"
          style={{
            padding: '8px 14px',
            background: 'transparent',
            border: '1px solid #2d3148',
            borderRadius: '6px',
            color: '#94a3b8',
            fontSize: '12px',
            fontFamily: 'inherit',
            textDecoration: 'none',
            transition: 'border-color 0.2s, color 0.2s',
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.borderColor = '#F47806'
            e.currentTarget.style.color = '#F47806'
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.borderColor = '#2d3148'
            e.currentTarget.style.color = '#94a3b8'
          }}
        >
          Monitor ETL
        </a>
      <button
        onClick={handleLogout}
        style={{
          padding: '8px 16px',
          background: 'transparent',
          border: '1px solid #2d3148',
          borderRadius: '6px',
          color: '#94a3b8',
          fontSize: '13px',
          fontFamily: 'inherit',
          cursor: 'pointer',
          transition: 'border-color 0.2s, color 0.2s',
        }}
        onMouseEnter={(e) => {
          e.currentTarget.style.borderColor = '#0576F3'
          e.currentTarget.style.color = '#0576F3'
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.borderColor = '#2d3148'
          e.currentTarget.style.color = '#94a3b8'
        }}
      >
        Cerrar sesión
      </button>
      </div>
    </header>
  )
}
