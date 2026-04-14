'use client'

import { useState } from 'react'
import { HexColorPicker } from 'react-colorful'
import { useStyleConfig, DEFAULT_PALETTE } from '@/hooks/useStyleConfig'

const GOOGLE_FONTS = ['Comfortaa', 'Roboto', 'Open Sans', 'Lato']
const ALL_FONTS = ['ballingermono-light', ...GOOGLE_FONTS]

const HEX_REGEX = /^#([0-9A-Fa-f]{3}|[0-9A-Fa-f]{6})$/

function isValidHex(value: string): boolean {
  return HEX_REGEX.test(value)
}

export default function StylePanel() {
  const {
    palette,
    fontFamily,
    titleSize,
    xAxisSize,
    yAxisSize,
    setPaletteColor,
    resetPalette,
    setFontFamily,
    setTitleSize,
    setXAxisSize,
    setYAxisSize,
  } = useStyleConfig()

  const [openPickerIndex, setOpenPickerIndex] = useState<number | null>(null)
  const [hexInputs, setHexInputs] = useState<string[]>([...palette])

  function handleHexInput(index: number, value: string) {
    const next = [...hexInputs]
    next[index] = value
    setHexInputs(next)
    if (isValidHex(value)) {
      setPaletteColor(index, value)
    }
  }

  function handlePickerChange(index: number, color: string) {
    const next = [...hexInputs]
    next[index] = color
    setHexInputs(next)
    setPaletteColor(index, color)
  }

  function handleReset() {
    resetPalette()
    setHexInputs([...DEFAULT_PALETTE])
    setOpenPickerIndex(null)
  }

  const labelStyle: React.CSSProperties = {
    fontSize: '11px',
    color: '#64748b',
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
    marginBottom: '4px',
    display: 'block',
  }

  const inputStyle: React.CSSProperties = {
    background: '#0f1117',
    border: '1px solid #2d3148',
    borderRadius: '4px',
    color: '#e2e8f0',
    fontSize: '12px',
    fontFamily: 'inherit',
    padding: '4px 8px',
    width: '60px',
    outline: 'none',
  }

  return (
    <div
      style={{
        background: '#1a1d27',
        borderBottom: '1px solid #2d3148',
        padding: '12px 24px',
        display: 'flex',
        flexWrap: 'wrap',
        gap: '24px',
        alignItems: 'flex-end',
      }}
    >
      {/* Font family selector */}
      <div>
        <label style={labelStyle}>Fuente</label>
        <select
          value={fontFamily}
          onChange={(e) => setFontFamily(e.target.value)}
          style={{
            ...inputStyle,
            width: 'auto',
            padding: '4px 8px',
            cursor: 'pointer',
          }}
        >
          {ALL_FONTS.map((f) => (
            <option key={f} value={f}>
              {f === 'ballingermono-light' ? 'Ballinger Mono (default)' : f}
            </option>
          ))}
        </select>
      </div>

      {/* Typography sizes */}
      <div>
        <label style={labelStyle}>Título (px)</label>
        <input
          type="number"
          min={8}
          max={32}
          value={titleSize}
          onChange={(e) => setTitleSize(Number(e.target.value))}
          style={inputStyle}
        />
      </div>

      <div>
        <label style={labelStyle}>Eje X (px)</label>
        <input
          type="number"
          min={8}
          max={24}
          value={xAxisSize}
          onChange={(e) => setXAxisSize(Number(e.target.value))}
          style={inputStyle}
        />
      </div>

      <div>
        <label style={labelStyle}>Eje Y (px)</label>
        <input
          type="number"
          min={8}
          max={24}
          value={yAxisSize}
          onChange={(e) => setYAxisSize(Number(e.target.value))}
          style={inputStyle}
        />
      </div>

      {/* Color palette */}
      <div style={{ display: 'flex', gap: '8px', alignItems: 'flex-end' }}>
        <div>
          <label style={labelStyle}>Paleta de colores</label>
          <div style={{ display: 'flex', gap: '6px', alignItems: 'center' }}>
            {palette.map((color, i) => (
              <div key={i} style={{ position: 'relative' }}>
                {/* Color swatch + hex input */}
                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                  <button
                    onClick={() => setOpenPickerIndex(openPickerIndex === i ? null : i)}
                    style={{
                      width: '28px',
                      height: '28px',
                      background: color,
                      border: openPickerIndex === i ? '2px solid #e2e8f0' : '2px solid #2d3148',
                      borderRadius: '4px',
                      cursor: 'pointer',
                      padding: 0,
                    }}
                    title={`Color ${i + 1}: ${color}`}
                    aria-label={`Seleccionar color ${i + 1}`}
                  />
                  <input
                    type="text"
                    value={hexInputs[i] ?? color}
                    onChange={(e) => handleHexInput(i, e.target.value)}
                    maxLength={7}
                    style={{
                      ...inputStyle,
                      width: '64px',
                      borderColor: isValidHex(hexInputs[i] ?? color) ? '#2d3148' : '#7f1d1d',
                    }}
                    aria-label={`Valor HEX color ${i + 1}`}
                  />
                </div>

                {/* Color picker popover */}
                {openPickerIndex === i && (
                  <div
                    style={{
                      position: 'absolute',
                      top: '100%',
                      left: 0,
                      zIndex: 100,
                      marginTop: '4px',
                      background: '#1a1d27',
                      border: '1px solid #2d3148',
                      borderRadius: '8px',
                      padding: '12px',
                    }}
                  >
                    <HexColorPicker
                      color={color}
                      onChange={(c) => handlePickerChange(i, c)}
                    />
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Reset button */}
        <button
          onClick={handleReset}
          style={{
            padding: '6px 12px',
            background: 'transparent',
            border: '1px solid #2d3148',
            borderRadius: '4px',
            color: '#94a3b8',
            fontSize: '11px',
            fontFamily: 'inherit',
            cursor: 'pointer',
            whiteSpace: 'nowrap',
            marginBottom: '2px',
          }}
          title="Restaurar paleta por defecto"
        >
          Restaurar
        </button>
      </div>

      {/* Close picker on outside click */}
      {openPickerIndex !== null && (
        <div
          style={{
            position: 'fixed',
            inset: 0,
            zIndex: 99,
          }}
          onClick={() => setOpenPickerIndex(null)}
        />
      )}
    </div>
  )
}
