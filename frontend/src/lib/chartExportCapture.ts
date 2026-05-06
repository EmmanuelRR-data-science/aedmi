import { toPng } from 'html-to-image'

/** Ticks y etiquetas de ejes X e Y (Recharts/SVG) para exportación PPTX. */
const PPTX_CAPTURE_AXIS_FONT_PX = 15

/** Píxeles mínimos en el lado largo del PNG para incrustar en PPTX sin pixelado al estirar. */
const PPTX_CAPTURE_MIN_LONG_EDGE_PX = 5200
/** Tope para no explotar memoria en equipos modestos (canvas muy grande). */
const PPTX_CAPTURE_PIXEL_RATIO_MAX = 12
const PPTX_CAPTURE_PIXEL_RATIO_MIN = 4

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

  root.querySelectorAll('.recharts-cartesian-axis text, .recharts-cartesian-axis tspan').forEach(
    (el) => {
      const svgEl = el as SVGElement
      const prev = svgEl.getAttribute('font-size')
      const prevStyle = (el as HTMLElement).style?.fontSize
      svgEl.setAttribute('font-size', String(PPTX_CAPTURE_AXIS_FONT_PX))
      if ((el as HTMLElement).style) {
        (el as HTMLElement).style.fontSize = `${PPTX_CAPTURE_AXIS_FONT_PX}px`
      }
      revert.push(() => {
        if (prev === null) svgEl.removeAttribute('font-size')
        else svgEl.setAttribute('font-size', prev)
        if ((el as HTMLElement).style) (el as HTMLElement).style.fontSize = prevStyle
      })
    },
  )

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
    const prevFs = t.style.fontSize
    t.style.color = '#0f172a'
    t.style.fill = '#0f172a'
    t.style.fontSize = `${PPTX_CAPTURE_AXIS_FONT_PX}px`
    revert.push(() => {
      t.style.color = prevC
      t.style.fill = prevF
      t.style.fontSize = prevFs
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

function pixelRatioParaAltaResolucion(widthPx: number, heightPx: number): number {
  const longEdge = Math.max(widthPx, heightPx)
  const target = Math.ceil(PPTX_CAPTURE_MIN_LONG_EDGE_PX / longEdge)
  return Math.min(PPTX_CAPTURE_PIXEL_RATIO_MAX, Math.max(PPTX_CAPTURE_PIXEL_RATIO_MIN, target))
}

/**
 * Mismo enfoque que el botón «↓ PNG» (serializar SVG → raster en canvas), con fondo y escala configurables.
 * Suele verse más nítido que html-to-image sobre el contenedor completo.
 */
function pngDataUrlDesdeSvg(
  svg: SVGElement,
  widthCss: number,
  heightCss: number,
  scale: number,
  backgroundColor: string,
): Promise<string> {
  const w = Math.max(1, Math.ceil(widthCss))
  const h = Math.max(1, Math.ceil(heightCss))
  const svgData = new XMLSerializer().serializeToString(svg)
  const blob = new Blob([svgData], { type: 'image/svg+xml;charset=utf-8' })
  const url = URL.createObjectURL(blob)

  return new Promise((resolve, reject) => {
    const img = new Image()
    img.onload = () => {
      try {
        const canvas = document.createElement('canvas')
        canvas.width = Math.max(1, Math.ceil(w * scale))
        canvas.height = Math.max(1, Math.ceil(h * scale))
        const ctx = canvas.getContext('2d')
        if (!ctx) {
          reject(new Error('canvas 2d'))
          return
        }
        ctx.fillStyle = backgroundColor
        ctx.fillRect(0, 0, canvas.width, canvas.height)
        ctx.drawImage(img, 0, 0, canvas.width, canvas.height)
        resolve(canvas.toDataURL('image/png'))
      } finally {
        URL.revokeObjectURL(url)
      }
    }
    img.onerror = () => {
      URL.revokeObjectURL(url)
      reject(new Error('svg image load'))
    }
    img.src = url
  })
}

/**
 * PNG en data URL: fondo blanco, alta resolución (objetivo ~5200 px en el lado largo; fuente de ejes X/Y 15 px).
 * Prioriza rasterizar el SVG de Recharts como el botón «↓ PNG»; si no hay SVG, usa html-to-image.
 */
export async function captureChartPngForPptx(node: HTMLElement): Promise<string> {
  const revert = aplicarEstilosExportacionPptx(node)
  await waitFrames(2)
  await new Promise((r) => setTimeout(r, 80))

  const rect = node.getBoundingClientRect()
  const w = Math.max(1, Math.ceil(rect.width))
  const h = Math.max(1, Math.ceil(rect.height))
  const pixelRatio = pixelRatioParaAltaResolucion(w, h)

  try {
    const svg = node.querySelector('svg')
    if (svg) {
      const r = svg.getBoundingClientRect()
      const sw = Math.max(1, Math.ceil(r.width))
      const sh = Math.max(1, Math.ceil(r.height))
      const pr = pixelRatioParaAltaResolucion(sw, sh)
      return await pngDataUrlDesdeSvg(svg, sw, sh, pr, '#ffffff')
    }

    return await toPng(node, {
      backgroundColor: '#ffffff',
      pixelRatio,
      cacheBust: true,
      width: w,
      height: h,
    })
  } finally {
    revert()
  }
}
