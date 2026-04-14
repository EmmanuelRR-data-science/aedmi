import { create } from 'zustand'

export const DEFAULT_PALETTE = ['#0576F3', '#36F48C', '#F47806', '#F3F40B', '#F479F4']
export const DEFAULT_FONT = 'ballingermono-light'
export const DEFAULT_TITLE_SIZE = 14
export const DEFAULT_X_AXIS_SIZE = 11
export const DEFAULT_Y_AXIS_SIZE = 11

interface StyleConfigState {
  palette: string[]
  fontFamily: string
  titleSize: number
  xAxisSize: number
  yAxisSize: number

  setPalette: (palette: string[]) => void
  setPaletteColor: (index: number, color: string) => void
  resetPalette: () => void
  setFontFamily: (font: string) => void
  setTitleSize: (size: number) => void
  setXAxisSize: (size: number) => void
  setYAxisSize: (size: number) => void
}

export const useStyleConfig = create<StyleConfigState>((set) => ({
  palette: [...DEFAULT_PALETTE],
  fontFamily: DEFAULT_FONT,
  titleSize: DEFAULT_TITLE_SIZE,
  xAxisSize: DEFAULT_X_AXIS_SIZE,
  yAxisSize: DEFAULT_Y_AXIS_SIZE,

  setPalette: (palette) => set({ palette }),

  setPaletteColor: (index, color) =>
    set((state) => {
      const next = [...state.palette]
      next[index] = color
      return { palette: next }
    }),

  resetPalette: () => set({ palette: [...DEFAULT_PALETTE] }),

  setFontFamily: (fontFamily) => set({ fontFamily }),

  setTitleSize: (titleSize) => set({ titleSize }),

  setXAxisSize: (xAxisSize) => set({ xAxisSize }),

  setYAxisSize: (yAxisSize) => set({ yAxisSize }),
}))

/**
 * Returns the color for element at position `index` from the active palette,
 * cycling if index >= palette.length.
 * Property 1: color at position i === palette[i % palette.length]
 */
export function getColorForIndex(palette: string[], index: number): string {
  if (palette.length === 0) return '#0576F3'
  return palette[index % palette.length]
}
