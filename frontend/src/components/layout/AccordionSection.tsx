'use client'

import { useState } from 'react'

interface AccordionSectionProps {
  title: string
  defaultOpen?: boolean
  children: React.ReactNode
  accentColor?: string
}

export default function AccordionSection({
  title,
  defaultOpen = true,
  children,
  accentColor = '#0576F3',
}: AccordionSectionProps) {
  const [open, setOpen] = useState(defaultOpen)

  return (
    <section>
      <button
        onClick={() => setOpen((v) => !v)}
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          background: 'transparent',
          border: 'none',
          cursor: 'pointer',
          padding: '0 0 12px 0',
          width: '100%',
          textAlign: 'left',
        }}
        aria-expanded={open}
      >
        {/* Indicador de color */}
        <span
          style={{
            width: '3px',
            height: '14px',
            background: accentColor,
            borderRadius: '2px',
            flexShrink: 0,
          }}
        />
        <span
          style={{
            fontSize: '12px',
            color: '#94a3b8',
            textTransform: 'uppercase',
            letterSpacing: '0.08em',
            fontWeight: 400,
            flex: 1,
          }}
        >
          {title}
        </span>
        {/* Chevron */}
        <span
          style={{
            color: '#4a5568',
            fontSize: '10px',
            transform: open ? 'rotate(180deg)' : 'rotate(0deg)',
            transition: 'transform 0.2s',
          }}
        >
          ▼
        </span>
      </button>

      {open && (
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            gap: '16px',
          }}
        >
          {children}
        </div>
      )}
    </section>
  )
}
