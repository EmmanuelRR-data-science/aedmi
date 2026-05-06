// Feature: market-study-app, Propiedad 5 (tipografías independientes)
import { describe, it, expect, beforeEach } from 'vitest'
import { useStyleConfig, DEFAULT_TITLE_SIZE, DEFAULT_X_AXIS_SIZE, DEFAULT_Y_AXIS_SIZE } from '@/hooks/useStyleConfig'

beforeEach(() => {
  useStyleConfig.setState({
    titleSize: DEFAULT_TITLE_SIZE,
    xAxisSize: DEFAULT_X_AXIS_SIZE,
    yAxisSize: DEFAULT_Y_AXIS_SIZE,
  })
})

describe('useStyleConfig: tamaños independientes', () => {
  it('cambiar titleSize no afecta ejes y viceversa', () => {
    const s = useStyleConfig.getState()
    s.setTitleSize(22)
    s.setXAxisSize(7)
    s.setYAxisSize(9)
    const st = useStyleConfig.getState()
    expect(st.titleSize).toBe(22)
    expect(st.xAxisSize).toBe(7)
    expect(st.yAxisSize).toBe(9)
  })
})
