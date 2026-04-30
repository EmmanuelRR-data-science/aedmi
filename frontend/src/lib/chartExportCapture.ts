import { toPng } from 'html-to-image'

function waitFrames(n: number): Promise<void> {
  return new Promise((resolve) => {
    let i = 0
    const step = () => {
      i += 1
      if (i >= n) resolve()
      else requestAnimationFrame(step)
    }
    requestAnimationFrame(step)
  })
}

/**
 * Ajustes temporales para captura legible sobre fondo blanco (Recharts / SVG).
 * Devuelve función para revertir cambios en el DOM visible.
 */
function aplicarEstilosExportacionPptx(root: HTMLElement): () => void {
  const revert: (() => void)[] = []

  root.querySelectorAll('text, tspan').forEach((el) => {
    const prev = el.getAttribute('fill')
    el.setAttribute('fill', '#0f172a')
    revert.push(() => {
      if (prev === null) el.removeAttribute('fill')
      else el.setAttribute('fill', prev)
    })
  })

  root.querySelectorAll('.recharts-cartesian-axis-tick-value, .recharts-label').forEach((el) => {
    const t = el as HTMLElement
    const prevC = t.style.color
    const prevF = t.style.fill
    t.style.color = '#0f172a'
    t.style.fill = '#0f172a'
    revert.push(() => {
      t.style.color = prevC
      t.style.fill = prevF
    })
  })

  root.querySelectorAll('.recharts-cartesian-grid line').forEach((el) => {
    const prev = el.getAttribute('stroke')
    el.setAttribute('stroke', '#cbd5e1')
    revert.push(() => {
      if (prev === null) el.removeAttribute('stroke')
      else el.setAttribute('stroke', prev)
    })
  })

  root.querySelectorAll('.recharts-cartesian-axis-line').forEach((el) => {
    const prev = el.getAttribute('stroke')
    el.setAttribute('stroke', '#64748b')
    revert.push(() => {
      if (prev === null) el.removeAttribute('stroke')
      else el.setAttribute('stroke', prev)
    })
  })

  root.querySelectorAll('.recharts-legend-item-text').forEach((el) => {
    const t = el as HTMLElement
    const prev = t.style.color
    t.style.color = '#0f172a'
    revert.push(() => {
      t.style.color = prev
    })
  })

  const prevBg = root.style.backgroundColor
  root.style.backgroundColor = '#ffffff'
  revert.push(() => {
    root.style.backgroundColor = prevBg
  })

  return () => {
    revert.reverse().forEach((fn) => fn())
  }
}

/** Mayor densidad de píxeles para que al redimensionar en PowerPoint la gráfica no se vea borrosa. */
const PPTX_CAPTURE_PIXEL_RATIO = 4

/**
 * PNG en data URL: fondo blanco, alta resolución (`pixelRatio` 4).
 * Captura el nodo visible en pantalla para que Recharts/SVG rendericen bien.
 */
export async function captureChartPngForPptx(node: HTMLElement): Promise<string> {
  const revert = aplicarEstilosExportacionPptx(node)
  await waitFrames(2)
  await new Promise((r) => setTimeout(r, 80))

  const rect = node.getBoundingClientRect()
  const w = Math.max(1, Math.ceil(rect.width))
  const h = Math.max(1, Math.ceil(rect.height))

  try {
    return await toPng(node, {
      backgroundColor: '#ffffff',
      pixelRatio: PPTX_CAPTURE_PIXEL_RATIO,
      cacheBust: true,
      width: w,
      height: h,
    })
  } finally {
    revert()
  }
}
