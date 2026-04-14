'use client'

import { useState, FormEvent } from 'react'
import { useRouter } from 'next/navigation'
import Image from 'next/image'
import { saveToken } from '@/lib/auth'

export default function LoginPage() {
  const router = useRouter()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    setLoading(true)

    try {
      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080'}/auth/login`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ username, password }),
        }
      )

      if (!res.ok) {
        setError('Credenciales incorrectas. Verifica tu usuario y contraseña.')
        return
      }

      const data = await res.json()
      saveToken(data.access_token)
      router.push('/dashboard')
    } catch {
      setError('No se pudo conectar con el servidor. Intenta de nuevo.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div
      style={{
        minHeight: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        background: '#0f1117',
        fontFamily: 'var(--font-ballinger)',
      }}
    >
      <div
        style={{
          background: '#1a1d27',
          border: '1px solid #2d3148',
          borderRadius: '12px',
          padding: '48px 40px',
          width: '100%',
          maxWidth: '400px',
        }}
      >
        {/* Logo + título */}
        <div style={{ textAlign: 'center', marginBottom: '32px' }}>
          <Image
            src="/logo/phiqus_logo_positivo.png"
            alt="PhiQus"
            width={120}
            height={40}
            style={{ objectFit: 'contain', marginBottom: '16px' }}
          />
          <h1
            style={{
              fontSize: '16px',
              color: '#94a3b8',
              fontWeight: 300,
              margin: 0,
            }}
          >
            Aplicación para estudios de Mercado
          </h1>
        </div>

        <form onSubmit={handleSubmit}>
          <div style={{ marginBottom: '16px' }}>
            <label
              htmlFor="username"
              style={{ display: 'block', fontSize: '13px', color: '#94a3b8', marginBottom: '6px' }}
            >
              Usuario
            </label>
            <input
              id="username"
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              required
              autoComplete="username"
              style={{
                width: '100%',
                padding: '10px 12px',
                background: '#0f1117',
                border: '1px solid #2d3148',
                borderRadius: '6px',
                color: '#e2e8f0',
                fontSize: '14px',
                fontFamily: 'inherit',
                outline: 'none',
              }}
            />
          </div>

          <div style={{ marginBottom: '24px' }}>
            <label
              htmlFor="password"
              style={{ display: 'block', fontSize: '13px', color: '#94a3b8', marginBottom: '6px' }}
            >
              Contraseña
            </label>
            <input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              autoComplete="current-password"
              style={{
                width: '100%',
                padding: '10px 12px',
                background: '#0f1117',
                border: '1px solid #2d3148',
                borderRadius: '6px',
                color: '#e2e8f0',
                fontSize: '14px',
                fontFamily: 'inherit',
                outline: 'none',
              }}
            />
          </div>

          {error && (
            <div
              role="alert"
              style={{
                background: '#2d1b1b',
                border: '1px solid #7f1d1d',
                borderRadius: '6px',
                padding: '10px 12px',
                color: '#fca5a5',
                fontSize: '13px',
                marginBottom: '16px',
              }}
            >
              {error}
            </div>
          )}

          <button
            type="submit"
            disabled={loading}
            style={{
              width: '100%',
              padding: '12px',
              background: loading ? '#1e3a6e' : '#0576F3',
              color: '#fff',
              border: 'none',
              borderRadius: '6px',
              fontSize: '14px',
              fontFamily: 'inherit',
              cursor: loading ? 'not-allowed' : 'pointer',
              transition: 'background 0.2s',
            }}
          >
            {loading ? 'Iniciando sesión...' : 'Iniciar sesión'}
          </button>
        </form>
      </div>
    </div>
  )
}
