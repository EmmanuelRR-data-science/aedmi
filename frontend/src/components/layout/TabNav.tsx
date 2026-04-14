'use client'

import { TabGeografico } from '@/types'

const TABS: { id: TabGeografico; label: string }[] = [
  { id: 'nacional', label: 'Nacional' },
  { id: 'estatal', label: 'Estatal' },
  { id: 'ciudad', label: 'Ciudades' },
  { id: 'municipal', label: 'Municipios' },
  { id: 'localidad', label: 'Localidades' },
  { id: 'mapa', label: 'Mapa' },
]

interface TabNavProps {
  activeTab: TabGeografico
  onTabChange: (tab: TabGeografico) => void
}

export default function TabNav({ activeTab, onTabChange }: TabNavProps) {
  return (
    <nav
      role="tablist"
      aria-label="Nivel geográfico"
      style={{
        display: 'flex',
        gap: '4px',
        padding: '8px 24px',
        background: '#1a1d27',
        borderBottom: '1px solid #2d3148',
      }}
    >
      {TABS.map((tab) => {
        const isActive = tab.id === activeTab
        return (
          <button
            key={tab.id}
            role="tab"
            aria-selected={isActive}
            onClick={() => onTabChange(tab.id)}
            style={{
              padding: '8px 20px',
              background: isActive ? '#0576F3' : 'transparent',
              color: isActive ? '#fff' : '#94a3b8',
              border: isActive ? '1px solid #0576F3' : '1px solid transparent',
              borderRadius: '6px',
              fontSize: '13px',
              fontFamily: 'inherit',
              cursor: 'pointer',
              transition: 'all 0.2s',
              fontWeight: isActive ? 400 : 300,
            }}
            onMouseEnter={(e) => {
              if (!isActive) {
                e.currentTarget.style.color = '#e2e8f0'
                e.currentTarget.style.borderColor = '#2d3148'
              }
            }}
            onMouseLeave={(e) => {
              if (!isActive) {
                e.currentTarget.style.color = '#94a3b8'
                e.currentTarget.style.borderColor = 'transparent'
              }
            }}
          >
            {tab.label}
          </button>
        )
      })}
    </nav>
  )
}
