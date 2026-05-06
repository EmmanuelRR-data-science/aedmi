import type { QueryClient } from '@tanstack/react-query'
import type { Analisis } from '@/types'

/**
 * Reproduce la prioridad del API: revisado no vacío → IA.
 * Misma lógica que al mostrar análisis en el dashboard.
 */
function textoDesdeFilaAnalisis(a: Analisis | null | undefined): string | undefined {
  if (!a) return undefined
  const r = (a.analisis_revisado ?? '').trim()
  if (r) return r
  const i = (a.analisis_ia ?? '').trim()
  if (i) return i
  return undefined
}

/**
 * Resuelve el cuerpo de análisis desde la caché de React Query (misma clave que `useAnalisis`)
 * y hace **fallback a entidad NULL** si la cola llevó entidad (p. ej. PIB Total) pero el
 * análisis compartido en pantalla se guardó sin entidad.
 */
export function textoAnalisisDesdeQueryCache(
  queryClient: QueryClient,
  indicadorId: number,
  entidadClave: string | null | undefined
): string | undefined {
  const ent = entidadClave === undefined || entidadClave === null ? null : entidadClave
  const aExacto = queryClient.getQueryData<Analisis | null>(['analisis', indicadorId, ent])
  const t1 = textoDesdeFilaAnalisis(aExacto)
  if (t1) return t1
  if (ent != null) {
    const aNull = queryClient.getQueryData<Analisis | null>(['analisis', indicadorId, null])
    return textoDesdeFilaAnalisis(aNull)
  }
  return undefined
}
