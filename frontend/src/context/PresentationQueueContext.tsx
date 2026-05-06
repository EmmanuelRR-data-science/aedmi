'use client'

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { apiFetch, apiFetchBlob, ApiError, type GammaPresentacionResponse } from '@/lib/api'
import { captureChartPngForPptx } from '@/lib/chartExportCapture'
import { useStyleConfig } from '@/hooks/useStyleConfig'
import { textoAnalisisDesdeQueryCache } from '@/lib/analisisCache'
import type { TabGeografico } from '@/types'

export const PRESENTACION_LOTE_MAX = 30

export type ExcelChartKind = 'column' | 'line' | 'pie' | 'none'

/** Serie numérica exacta de la gráfica (origen API/dashboard) para el prompt de Gamma. */
export type DatoSeriePresentacion = {
  periodo: string | number
  valor: number
  entidad_clave?: string | null
  unidad?: string | null
}

export type PresentationQueueItem = {
  id: string
  key: string
  indicadorId: number
  nivelGeografico: TabGeografico
  entidadClave: string | null
  titulo: string
  subtitulo: string
  /** data URL o base64 puro; el API acepta ambos */
  imagenPngDataUrl: string
  /** Si existe, se envía al backend para que Gamma no infiera cifras. */
  datosSerie?: DatoSeriePresentacion[]
  /** Mismo texto que el pie "Fuente:" en la UI; evita leyendas corruptas desde BD. */
  leyendaFuente?: string
  /** Tipo de gráfico nativo en Excel al exportar lote o XLSX unitario. */
  excelChartKind?: ExcelChartKind
}

function makeItemKey(
  nivel: TabGeografico,
  indicadorId: number,
  entidadClave: string | null,
  titulo: string
) {
  const e = entidadClave === null || entidadClave === undefined ? '∅' : entidadClave
  return `${nivel}::${indicadorId}::${e}::${titulo.slice(0, 80)}`
}

type Ctx = {
  items: PresentationQueueItem[]
  addFromRef: (args: {
    chartRef: React.RefObject<HTMLDivElement | null>
    indicadorId: number
    nivelGeografico: TabGeografico
    entidadClave: string | null
    titulo: string
    subtitulo: string
    datosSerie?: DatoSeriePresentacion[]
    leyendaFuente?: string
    excelChartKind?: ExcelChartKind
  }) => Promise<{ ok: true } | { ok: false; reason: string }>
  remove: (id: string) => void
  move: (id: string, direction: 'up' | 'down') => void
  clear: () => void
  descargarLote: (tituloPresentacion: string) => Promise<void>
  descargarLotePptx: (tituloPresentacion: string) => Promise<void>
  descargarLoteExcel: (tituloPresentacion: string) => Promise<void>
  descargarLoteZip: (tituloPresentacion: string) => Promise<void>
  downloading: boolean
  /** Qué exportación está en curso, para textos en la barra. */
  exportingKind: 'gamma' | 'pptx' | 'xlsx' | 'zip' | null
  lastError: string | null
  lastInfo: string | null
}

const PresentationQueueContext = createContext<Ctx | null>(null)

export function PresentationQueueProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient()
  const [items, setItems] = useState<PresentationQueueItem[]>([])
  const itemsRef = useRef<PresentationQueueItem[]>([])
  const [exportingKind, setExportingKind] = useState<'gamma' | 'pptx' | 'xlsx' | 'zip' | null>(null)
  const downloading = exportingKind !== null
  const [lastError, setLastError] = useState<string | null>(null)
  const [lastInfo, setLastInfo] = useState<string | null>(null)

  useEffect(() => {
    itemsRef.current = items
  }, [items])

  const addFromRef = useCallback<Ctx['addFromRef']>(async (args) => {
    const {
      chartRef,
      indicadorId,
      nivelGeografico,
      entidadClave,
      titulo,
      subtitulo,
      datosSerie,
      leyendaFuente,
      excelChartKind = 'column',
    } = args
    const key = makeItemKey(nivelGeografico, indicadorId, entidadClave, titulo)
    setLastError(null)
    setLastInfo(null)

    const current = itemsRef.current
    if (current.some((x) => x.key === key)) {
      return { ok: false, reason: 'Esta gráfica ya está en la presentación.' }
    }
    if (current.length >= PRESENTACION_LOTE_MAX) {
      return {
        ok: false,
        reason: `Máximo ${PRESENTACION_LOTE_MAX} gráficas por presentación.`,
      }
    }

    const node = chartRef.current
    if (!node) {
      return { ok: false, reason: 'No hay gráfica visible en pantalla.' }
    }

    let dataUrl: string
    try {
      dataUrl = await captureChartPngForPptx(node)
    } catch {
      return { ok: false, reason: 'No se pudo capturar la gráfica.' }
    }

    const id = `${key}-${Date.now()}`
    const row: PresentationQueueItem = {
      id,
      key,
      indicadorId,
      nivelGeografico,
      entidadClave,
      titulo,
      subtitulo,
      imagenPngDataUrl: dataUrl,
      ...(datosSerie?.length ? { datosSerie } : {}),
      ...(leyendaFuente?.trim() ? { leyendaFuente: leyendaFuente.trim() } : {}),
      excelChartKind,
    }

    let agregada = false
    setItems((prev) => {
      if (prev.some((x) => x.key === key) || prev.length >= PRESENTACION_LOTE_MAX) {
        return prev
      }
      agregada = true
      return [...prev, row]
    })

    if (!agregada) {
      return {
        ok: false,
        reason: 'No se pudo añadir: límite alcanzado o gráfica duplicada.',
      }
    }
    return { ok: true }
  }, [])

  const remove = useCallback((id: string) => {
    setItems((prev) => prev.filter((x) => x.id !== id))
  }, [])

  const move = useCallback((id: string, direction: 'up' | 'down') => {
    setItems((prev) => {
      const idx = prev.findIndex((x) => x.id === id)
      if (idx < 0) return prev
      const j = direction === 'up' ? idx - 1 : idx + 1
      if (j < 0 || j >= prev.length) return prev
      const next = [...prev]
      ;[next[idx], next[j]] = [next[j], next[idx]]
      return next
    })
  }, [])

  const clear = useCallback(() => {
    setItems([])
    setLastError(null)
    setLastInfo(null)
  }, [])

  const descargarLote = useCallback(
    async (tituloPresentacion: string) => {
      if (items.length === 0) {
        setLastError('Añade al menos una gráfica.')
        return
      }
      setExportingKind('gamma')
      setLastError(null)
      setLastInfo(null)
      try {
        const paletaHex = useStyleConfig.getState().palette
        const res = await apiFetch<GammaPresentacionResponse>('/export/presentacion/gamma', {
          method: 'POST',
          body: JSON.stringify({
            titulo_presentacion: tituloPresentacion,
            paleta_hex: paletaHex.length ? paletaHex : undefined,
            items: items.map((it) => {
              const textoAnalisis = textoAnalisisDesdeQueryCache(
                queryClient,
                it.indicadorId,
                it.entidadClave
              )
              return {
                grafica_id: it.indicadorId,
                titulo: it.titulo,
                nivel_geografico: it.nivelGeografico,
                entidad_clave: it.entidadClave,
                imagen_grafica_png_base64: it.imagenPngDataUrl,
                subtitulo_contexto: it.subtitulo,
                excel_chart_kind: it.excelChartKind ?? 'column',
                ...(textoAnalisis ? { texto_analisis: textoAnalisis } : {}),
                ...(it.leyendaFuente ? { leyenda_fuente: it.leyendaFuente } : {}),
                ...(it.datosSerie?.length
                  ? {
                      datos_serie: it.datosSerie.map((d) => ({
                        periodo: d.periodo,
                        valor: d.valor,
                        entidad_clave: d.entidad_clave ?? null,
                        unidad: d.unidad ?? null,
                      })),
                    }
                  : {}),
              }
            }),
          }),
        })
        if (res.export_url) {
          window.open(res.export_url, '_blank', 'noopener,noreferrer')
          setLastInfo(
            'Listo: se abrió el enlace de descarga de Gamma (PPTX/PDF; el enlace caduca en varios días).'
          )
        } else if (res.gamma_url) {
          window.open(res.gamma_url, '_blank', 'noopener,noreferrer')
          setLastInfo('Presentación creada en Gamma; se abrió el editor en una nueva pestaña.')
        } else {
          setLastInfo(
            `Generación completada (id: ${res.generation_id}). No hubo URL de exportación automática.`
          )
        }
      } catch (e) {
        if (e instanceof ApiError) {
          setLastError(e.message)
        } else {
          setLastError('Error al generar la presentación en Gamma.')
        }
      } finally {
        setExportingKind(null)
      }
    },
    [items, queryClient]
  )

  const descargarLotePptx = useCallback(
    async (tituloPresentacion: string) => {
      if (items.length === 0) {
        setLastError('Añade al menos una gráfica.')
        return
      }
      setExportingKind('pptx')
      setLastError(null)
      setLastInfo(null)
      try {
        const blob = await apiFetchBlob('/export/presentacion/lote', {
          method: 'POST',
          body: JSON.stringify({
            titulo_presentacion: tituloPresentacion,
            modo_salida: 'pptx',
            items: items.map((it) => {
              const textoAnalisis = textoAnalisisDesdeQueryCache(
                queryClient,
                it.indicadorId,
                it.entidadClave
              )
              return {
                grafica_id: it.indicadorId,
                titulo: it.titulo,
                nivel_geografico: it.nivelGeografico,
                entidad_clave: it.entidadClave,
                imagen_grafica_png_base64: it.imagenPngDataUrl,
                subtitulo_contexto: it.subtitulo,
                excel_chart_kind: it.excelChartKind ?? 'column',
                ...(textoAnalisis ? { texto_analisis: textoAnalisis } : {}),
                ...(it.leyendaFuente ? { leyenda_fuente: it.leyendaFuente } : {}),
                ...(it.datosSerie?.length
                  ? {
                      datos_serie: it.datosSerie.map((d) => ({
                        periodo: d.periodo,
                        valor: d.valor,
                        entidad_clave: d.entidad_clave ?? null,
                        unidad: d.unidad ?? null,
                      })),
                    }
                  : {}),
              }
            }),
          }),
        })
        const base = (tituloPresentacion.trim() || 'aedmi_lote').replace(/[^\w\d._-]+/g, '_').slice(0, 64)
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = `${base || 'aedmi_lote'}.pptx`
        a.rel = 'noopener'
        a.click()
        URL.revokeObjectURL(url)
        setLastInfo('PPTX descargado (editable en PowerPoint).')
      } catch (e) {
        if (e instanceof ApiError) {
          setLastError(e.message)
        } else {
          setLastError('Error al generar el PPTX.')
        }
      } finally {
        setExportingKind(null)
      }
    },
    [items, queryClient]
  )

  const descargarLoteExcel = useCallback(
    async (tituloPresentacion: string) => {
      if (items.length === 0) {
        setLastError('Añade al menos una gráfica.')
        return
      }
      const sin = items.filter((it) => !it.datosSerie?.length)
      if (sin.length) {
        setLastError(
          'Para Excel, cada gráfica en la cola debe tener datos de serie (usa vistas que envíen la serie al añadir a la cola).'
        )
        return
      }
      setExportingKind('xlsx')
      setLastError(null)
      setLastInfo(null)
      try {
        const blob = await apiFetchBlob('/export/presentacion/lote', {
          method: 'POST',
          body: JSON.stringify({
            titulo_presentacion: tituloPresentacion,
            modo_salida: 'xlsx',
            items: items.map((it) => {
              const textoAnalisis = textoAnalisisDesdeQueryCache(
                queryClient,
                it.indicadorId,
                it.entidadClave
              )
              return {
                grafica_id: it.indicadorId,
                titulo: it.titulo,
                nivel_geografico: it.nivelGeografico,
                entidad_clave: it.entidadClave,
                imagen_grafica_png_base64: null,
                subtitulo_contexto: it.subtitulo,
                excel_chart_kind: it.excelChartKind ?? 'column',
                datos_serie: (it.datosSerie ?? []).map((d) => ({
                  periodo: d.periodo,
                  valor: d.valor,
                  entidad_clave: d.entidad_clave ?? null,
                  unidad: d.unidad ?? null,
                })),
                ...(textoAnalisis ? { texto_analisis: textoAnalisis } : {}),
                ...(it.leyendaFuente ? { leyenda_fuente: it.leyendaFuente } : {}),
              }
            }),
          }),
        })
        const base = (tituloPresentacion.trim() || 'aedmi_lote').replace(/[^\w\d._-]+/g, '_').slice(0, 64)
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = `${base || 'aedmi_lote'}.xlsx`
        a.rel = 'noopener'
        a.click()
        URL.revokeObjectURL(url)
        setLastInfo('Excel descargado (gráficos nativos en cada hoja).')
      } catch (e) {
        if (e instanceof ApiError) {
          setLastError(e.message)
        } else {
          setLastError('Error al generar el Excel.')
        }
      } finally {
        setExportingKind(null)
      }
    },
    [items, queryClient]
  )

  const descargarLoteZip = useCallback(
    async (tituloPresentacion: string) => {
      if (items.length === 0) {
        setLastError('Añade al menos una gráfica.')
        return
      }
      const sin = items.filter((it) => !it.datosSerie?.length)
      if (sin.length) {
        setLastError(
          'Para el ZIP (PPTX+Excel), cada gráfica en la cola debe tener datos de serie.'
        )
        return
      }
      setExportingKind('zip')
      setLastError(null)
      setLastInfo(null)
      try {
        const blob = await apiFetchBlob('/export/presentacion/lote', {
          method: 'POST',
          body: JSON.stringify({
            titulo_presentacion: tituloPresentacion,
            modo_salida: 'zip_pptx_xlsx',
            items: items.map((it) => {
              const textoAnalisis = textoAnalisisDesdeQueryCache(
                queryClient,
                it.indicadorId,
                it.entidadClave
              )
              return {
                grafica_id: it.indicadorId,
                titulo: it.titulo,
                nivel_geografico: it.nivelGeografico,
                entidad_clave: it.entidadClave,
                imagen_grafica_png_base64: it.imagenPngDataUrl,
                subtitulo_contexto: it.subtitulo,
                excel_chart_kind: it.excelChartKind ?? 'column',
                datos_serie: (it.datosSerie ?? []).map((d) => ({
                  periodo: d.periodo,
                  valor: d.valor,
                  entidad_clave: d.entidad_clave ?? null,
                  unidad: d.unidad ?? null,
                })),
                ...(textoAnalisis ? { texto_analisis: textoAnalisis } : {}),
                ...(it.leyendaFuente ? { leyenda_fuente: it.leyendaFuente } : {}),
              }
            }),
          }),
        })
        const base = (tituloPresentacion.trim() || 'aedmi_lote').replace(/[^\w\d._-]+/g, '_').slice(0, 64)
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = `${base || 'aedmi_lote'}.zip`
        a.rel = 'noopener'
        a.click()
        URL.revokeObjectURL(url)
        setLastInfo('ZIP descargado (PPTX + Excel).')
      } catch (e) {
        if (e instanceof ApiError) {
          setLastError(e.message)
        } else {
          setLastError('Error al generar el ZIP.')
        }
      } finally {
        setExportingKind(null)
      }
    },
    [items, queryClient]
  )

  const value = useMemo(
    () => ({
      items,
      addFromRef,
      remove,
      move,
      clear,
      descargarLote,
      descargarLotePptx,
      descargarLoteExcel,
      descargarLoteZip,
      downloading,
      exportingKind,
      lastError,
      lastInfo,
    }),
    [
      items,
      addFromRef,
      remove,
      move,
      clear,
      descargarLote,
      descargarLotePptx,
      descargarLoteExcel,
      descargarLoteZip,
      downloading,
      exportingKind,
      lastError,
      lastInfo,
    ]
  )

  return (
    <PresentationQueueContext.Provider value={value}>
      {children}
    </PresentationQueueContext.Provider>
  )
}

export function usePresentationQueue() {
  const ctx = useContext(PresentationQueueContext)
  if (!ctx) {
    throw new Error('usePresentationQueue debe usarse dentro de PresentationQueueProvider')
  }
  return ctx
}
