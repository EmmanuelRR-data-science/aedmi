'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import { isAuthenticated } from '@/lib/auth'
import TopBar from '@/components/layout/TopBar'
import ModulosList from '@/components/etl-admin/ModulosList'
import FileUpload from '@/components/etl-admin/FileUpload'
import LogsTable from '@/components/etl-admin/LogsTable'

type Section = 'modulos' | 'upload' | 'logs'

export default function ETLAdminPage() {
  const router = useRouter()
  const [mounted, setMounted] = useState(false)
  const [activeSection, setActiveSection] = useState<Section>('modulos')

  useEffect(() => {
    setMounted(true)
    if (!isAuthenticated()) {
      router.replace('/login')
    }
  }, [router])

  if (!mounted) return null

  const sections: { id: Section; label: string }[] = [
    { id: 'modulos', label: 'Módulos ETL' },
    { id: 'upload', label: 'Carga manual' },
    { id: 'logs', label: 'Historial de ejecuciones' },
  ]

  const sectionTitles: Record<Section, string> = {
    modulos: 'Módulos ETL registrados',
    upload: 'Carga manual de archivo',
    logs: 'Historial de ejecuciones',
  }

  return (
    <div style={{ minHeight: '100vh', background: '#0f1117', display: 'flex', flexDirection: 'column' }}>
      <TopBar />

      {/* Sub-navigation */}
      <nav
        style={{
          display: 'flex',
          gap: '4px',
          padding: '8px 24px',
          background: '#1a1d27',
          borderBottom: '1px solid #2d3148',
          alignItems: 'center',
        }}
      >
        <span style={{ fontSize: '11px', color: '#4a5568', marginRight: '8px' }}>
          Monitor ETL
        </span>
        {sections.map((s) => (
          <button
            key={s.id}
            onClick={() => setActiveSection(s.id)}
            style={{
              padding: '6px 16px',
              background: activeSection === s.id ? '#0576F3' : 'transparent',
              color: activeSection === s.id ? '#fff' : '#94a3b8',
              border: activeSection === s.id ? '1px solid #0576F3' : '1px solid transparent',
              borderRadius: '6px',
              fontSize: '12px',
              fontFamily: 'inherit',
              cursor: 'pointer',
            }}
          >
            {s.label}
          </button>
        ))}
      </nav>

      {/* Content */}
      <main style={{ flex: 1, padding: '24px' }}>
        <h2
          style={{
            fontSize: '14px',
            color: '#e2e8f0',
            fontWeight: 300,
            margin: '0 0 20px 0',
            letterSpacing: '0.02em',
          }}
        >
          {sectionTitles[activeSection]}
        </h2>

        <div
          style={{
            background: '#1a1d27',
            border: '1px solid #2d3148',
            borderRadius: '10px',
            padding: '20px',
          }}
        >
          {activeSection === 'modulos' && <ModulosList />}
          {activeSection === 'upload' && <FileUpload />}
          {activeSection === 'logs' && <LogsTable />}
        </div>
      </main>
    </div>
  )
}
