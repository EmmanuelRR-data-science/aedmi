'use client'

import { useEffect, useMemo, useRef, useState, type RefObject } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { useIndicadorDatos, useIndicadores } from '@/hooks/useIndicador'
import { useStyleConfig } from '@/hooks/useStyleConfig'
import AnalisisIA from '@/components/ai/AnalisisIA'
import AnalisisRevisado from '@/components/ai/AnalisisRevisado'
import type { DatoIndicador } from '@/types'

function fmt(v: number): string {
  return new Intl.NumberFormat('es-MX', { maximumFractionDigits: 0 }).format(v)
}

function downloadChartAsPng(containerRef: RefObject<HTMLDivElement>, filename: string) {
  const svg = containerRef.current?.querySelector('svg')
  if (!svg) return
  const scale = 3
  const { width: w, height: h } = svg.getBoundingClientRect()
  const blob = new Blob([new XMLSerializer().serializeToString(svg)], { type: 'image/svg+xml;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const img = new Image()
  img.onload = () => {
    const canvas = document.createElement('canvas')
    canvas.width = w * scale
    canvas.height = h * scale
    const ctx = canvas.getContext('2d')
    if (!ctx) return
    ctx.fillStyle = '#1a1d27'
    ctx.fillRect(0, 0, canvas.width, canvas.height)
    ctx.drawImage(img, 0, 0, canvas.width, canvas.height)
    URL.revokeObjectURL(url)
    const link = document.createElement('a')
    link.download = `${filename}.png`
    link.href = canvas.toDataURL('image/png')
    link.click()
  }
  img.src = url
}

type SexoKey = 'total' | 'mujeres' | 'hombres'
type SexoPiramide = 'hombres' | 'mujeres'

interface LocalidadPoint {
  estado: string
  municipio: string
  localidad: string
  sexo: SexoKey
  periodo: number
  valor: number
}

interface LocalidadPiramidePoint {
  estado: string
  municipio: string
  localidad: string
  sexo: SexoPiramide
  grupo: string
  periodo: number
  valor: number
}

type CityId = 'merida' | 'queretaro' | 'cdmx' | 'monterrey' | 'guadalajara' | 'tijuana' | 'cancun'
type IndicadorCiudadId = 'crecimiento' | 'sexo' | 'edad' | 'pea_queretaro' | 'participacion_laboral_queretaro' | 'desocupacion_queretaro' | 'sectorial_empleo_queretaro' | 'pib_total_queretaro' | 'pib_per_capita_queretaro' | 'pib_sector_queretaro' | 'crecimiento_economico_queretaro' | 'exportaciones_estatales_queretaro' | 'importaciones_estatales_queretaro' | 'comercio_internacional_neto_queretaro' | 'balance_comercial_neto_queretaro' | 'ied_anual_queretaro' | 'ied_municipio_queretaro' | 'llegada_total_turistas_queretaro' | 'turismo_nacional_internacional_queretaro' | 'derrama_turistica_queretaro' | 'ocupacion_hotelera_queretaro' | 'crecimiento_habitaciones_ocupadas_queretaro' | 'pasajeros_anuales_aiq_queretaro' | 'crecimiento_anual_pasajeros_aiq_queretaro' | 'vuelos_nacionales_internacionales_aiq_queretaro' | 'historico_toneladas_transportadas_aiq_queretaro' | 'ocupacion_turismo' | 'comercio_internacional' | 'comercio_internacional_mty' | 'export_import_mty' | 'llegadas_pasajeros_mty' | 'visitantes_nac_ext_mty' | 'ocupacion_hotelera_mty' | 'conectividad_aerea' | 'conexiones_aereas' | 'oferta_servicios_turisticos' | 'ocupacion_hotelera_promedio' | 'llegada_pernocta' | 'gasto_promedio_diario' | 'derrama_economica' | 'ingreso_hotelero' | 'establecimientos_turisticos'

interface CityConfig {
  id: CityId
  label: string
  estado: string
  municipio: string
  localidadPreferida: string[]
  indicadores: { id: IndicadorCiudadId; label: string }[]
}

const CITY_CONFIG: CityConfig[] = [
  {
    id: 'merida',
    label: 'Mérida',
    estado: 'Yucatán',
    municipio: 'Mérida',
    localidadPreferida: ['Mérida'],
    indicadores: [
      { id: 'crecimiento', label: 'Crecimiento poblacional anual' },
      { id: 'sexo', label: 'Distribución por sexo' },
      { id: 'edad', label: 'Distribución por edad' },
      { id: 'ocupacion_turismo', label: 'Población ocupada en restaurantes y hoteles' },
      { id: 'comercio_internacional', label: 'Comercio internacional' },
      { id: 'conectividad_aerea', label: 'Conectividad aérea' },
      { id: 'conexiones_aereas', label: 'Conexiones aéreas' },
      { id: 'oferta_servicios_turisticos', label: 'Oferta de servicios turísticos' },
      { id: 'ocupacion_hotelera_promedio', label: 'Ocupación hotelera promedio' },
      { id: 'llegada_pernocta', label: 'Llegada de visitantes con pernocta' },
      { id: 'gasto_promedio_diario', label: 'Gasto promedio diario con pernocta' },
      { id: 'derrama_economica', label: 'Derrama económica estimada' },
      { id: 'ingreso_hotelero', label: 'Ingreso hotelero' },
      { id: 'establecimientos_turisticos', label: 'Establecimientos de servicios turísticos' },
    ],
  },
  {
    id: 'queretaro',
    label: 'Querétaro',
    estado: 'Querétaro',
    municipio: 'Querétaro',
    localidadPreferida: ['Santiago de Querétaro', 'Querétaro'],
    indicadores: [
      { id: 'crecimiento', label: 'Crecimiento poblacional anual' },
      { id: 'sexo', label: 'Distribución por sexo' },
      { id: 'edad', label: 'Distribución por edad' },
      { id: 'pea_queretaro', label: 'Población económicamente activa' },
      { id: 'participacion_laboral_queretaro', label: 'Tasa de participación laboral' },
      { id: 'desocupacion_queretaro', label: 'Tasa de desocupación' },
      { id: 'sectorial_empleo_queretaro', label: 'Composición sectorial del empleo' },
      { id: 'pib_total_queretaro', label: 'PIB estatal total' },
      { id: 'pib_per_capita_queretaro', label: 'PIB per cápita' },
      { id: 'pib_sector_queretaro', label: 'Distribución del PIB por sector' },
      { id: 'crecimiento_economico_queretaro', label: 'Tasa de crecimiento económico' },
      { id: 'exportaciones_estatales_queretaro', label: 'Exportaciones estatales' },
      { id: 'importaciones_estatales_queretaro', label: 'Importaciones estatales' },
      { id: 'comercio_internacional_neto_queretaro', label: 'Comercio internacional neto' },
      { id: 'balance_comercial_neto_queretaro', label: 'Balance comercial neto' },
      { id: 'ied_anual_queretaro', label: 'IED anual' },
      { id: 'ied_municipio_queretaro', label: 'IED por municipio' },
      { id: 'llegada_total_turistas_queretaro', label: 'Llegada total de turistas' },
      { id: 'turismo_nacional_internacional_queretaro', label: 'Turismo nacional vs internacional' },
      { id: 'derrama_turistica_queretaro', label: 'Derrama económica turística' },
      { id: 'ocupacion_hotelera_queretaro', label: 'Ocupación hotelera' },
      { id: 'crecimiento_habitaciones_ocupadas_queretaro', label: 'Crecimiento de habitaciones ocupadas' },
      { id: 'pasajeros_anuales_aiq_queretaro', label: 'Pasajeros anuales AIQ' },
      { id: 'crecimiento_anual_pasajeros_aiq_queretaro', label: 'Crecimiento anual de pasajeros' },
      { id: 'vuelos_nacionales_internacionales_aiq_queretaro', label: '% vuelos nacionales / internacionales' },
      { id: 'historico_toneladas_transportadas_aiq_queretaro', label: 'Histórico de toneladas transportadas' },
    ],
  },
  {
    id: 'cdmx',
    label: 'CDMX',
    estado: 'Ciudad de México',
    municipio: 'Cuauhtémoc',
    localidadPreferida: ['Ciudad de México', 'Cuauhtémoc'],
    indicadores: [],
  },
  {
    id: 'monterrey',
    label: 'Monterrey',
    estado: 'Nuevo León',
    municipio: 'Monterrey',
    localidadPreferida: ['Monterrey'],
    indicadores: [
      { id: 'crecimiento', label: 'Crecimiento poblacional anual' },
      { id: 'sexo', label: 'Distribución por sexo' },
      { id: 'edad', label: 'Distribución por edad' },
      { id: 'comercio_internacional_mty', label: 'Comercio internacional' },
      { id: 'export_import_mty', label: 'Exportaciones e importaciones' },
      { id: 'llegadas_pasajeros_mty', label: 'Llegadas de pasajeros' },
      { id: 'visitantes_nac_ext_mty', label: 'Visitantes nacionales y extranjeros' },
      { id: 'ocupacion_hotelera_mty', label: 'Ocupación hotelera' },
    ],
  },
  {
    id: 'guadalajara',
    label: 'Guadalajara',
    estado: 'Jalisco',
    municipio: 'Guadalajara',
    localidadPreferida: ['Guadalajara'],
    indicadores: [],
  },
  {
    id: 'tijuana',
    label: 'Tijuana',
    estado: 'Baja California',
    municipio: 'Tijuana',
    localidadPreferida: ['Tijuana'],
    indicadores: [],
  },
  {
    id: 'cancun',
    label: 'Cancún',
    estado: 'Quintana Roo',
    municipio: 'Benito Juárez',
    localidadPreferida: ['Cancún'],
    indicadores: [],
  },
]

function clampRange(from: number, to: number): [number, number] {
  return from <= to ? [from, to] : [to, from]
}

export default function CiudadesIndicadoresPanel() {
  const { palette, fontFamily, titleSize, xAxisSize, yAxisSize } = useStyleConfig()
  const chartRef = useRef<HTMLDivElement>(null)
  const [selectedCityId, setSelectedCityId] = useState<CityId>('merida')
  const [selectedIndicador, setSelectedIndicador] = useState<IndicadorCiudadId>('crecimiento')
  const [yearFrom, setYearFrom] = useState<number | null>(null)
  const [yearTo, setYearTo] = useState<number | null>(null)
  const selectedCityConfig = CITY_CONFIG.find((c) => c.id === selectedCityId) ?? CITY_CONFIG[0]

  const { data: indicadores } = useIndicadores('localidad', 'demografia')
  const { data: indicadoresCiudadEconomia } = useIndicadores('ciudad', 'economia')
  const indicadorPob = indicadores?.find((i) => i.clave === 'conapo.localidades_poblacion')
  const indicadorPiramide = indicadores?.find((i) => i.clave === 'conapo.localidades_piramide_edad')
  const indicadorOcupacion = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'inegi.ocupacion_restaurantes_hoteles_merida'
  )
  const indicadorComercio = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'se.comercio_internacional_merida'
  )
  const indicadorComercioMty = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'se.comercio_internacional_monterrey'
  )
  const indicadorExpImpMty = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'se.exportaciones_importaciones_monterrey'
  )
  const { data: indicadoresCiudadConectividad } = useIndicadores('ciudad', 'conectividad_aerea')
  const { data: indicadoresCiudadTurismo } = useIndicadores('ciudad', 'turismo')
  const indicadorConectividad = indicadoresCiudadConectividad?.find(
    (i) => i.clave === 'afac.conectividad_aerea_merida'
  )
  const indicadorLlegadasPasajerosMty = indicadoresCiudadConectividad?.find(
    (i) => i.clave === 'afac.llegadas_pasajeros_monterrey'
  )
  const indicadorConexiones = indicadoresCiudadConectividad?.find(
    (i) => i.clave === 'afac.conexiones_aereas_merida'
  )
  const indicadorOfertaTuristica = indicadoresCiudadTurismo?.find(
    (i) => i.clave === 'sectur.oferta_servicios_turisticos_merida'
  )
  const indicadorOcupacionHotelera = indicadoresCiudadTurismo?.find(
    (i) => i.clave === 'sectur.ocupacion_hotelera_promedio_merida'
  )
  const indicadorLlegadaPernocta = indicadoresCiudadTurismo?.find(
    (i) => i.clave === 'sectur.llegada_visitantes_pernocta_merida'
  )
  const indicadorGastoPromedio = indicadoresCiudadTurismo?.find(
    (i) => i.clave === 'sectur.gasto_promedio_diario_pernocta_merida'
  )
  const indicadorDerrama = indicadoresCiudadTurismo?.find(
    (i) => i.clave === 'sectur.derrama_economica_pernocta_merida'
  )
  const indicadorIngresoHotelero = indicadoresCiudadTurismo?.find(
    (i) => i.clave === 'sectur.ingreso_hotelero_merida'
  )
  const indicadorEstablecimientos = indicadoresCiudadTurismo?.find(
    (i) => i.clave === 'sectur.establecimientos_servicios_turisticos_yucatan'
  )
  const indicadorVisitantesMty = indicadoresCiudadTurismo?.find(
    (i) => i.clave === 'sectur.visitantes_nacionales_extranjeros_monterrey'
  )
  const indicadorOcupacionHoteleraMty = indicadoresCiudadTurismo?.find(
    (i) => i.clave === 'sectur.ocupacion_hotelera_monterrey'
  )
  const indicadorPeaQro = indicadoresCiudadEconomia?.find((i) => i.clave === 'inegi.pea_queretaro')
  const indicadorTplQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'inegi.tasa_participacion_laboral_queretaro'
  )
  const indicadorTdQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'inegi.tasa_desocupacion_queretaro'
  )
  const indicadorSectorialQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'inegi.composicion_sectorial_empleo_queretaro'
  )
  const indicadorPibTotalQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'inegi.pib_estatal_total_queretaro'
  )
  const indicadorPibPerCapitaQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'inegi.pib_per_capita_queretaro'
  )
  const indicadorPibSectorQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'inegi.pib_sector_queretaro'
  )
  const indicadorCrecEcoQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'inegi.tasa_crecimiento_economico_queretaro'
  )
  const indicadorExportQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'se.exportaciones_estatales_queretaro'
  )
  const indicadorImportQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'se.importaciones_estatales_queretaro'
  )
  const indicadorComNetoQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'se.comercio_internacional_neto_queretaro'
  )
  const indicadorBalNetoQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'se.balance_comercial_neto_queretaro'
  )
  const indicadorIedAnualQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'se.ied_anual_queretaro'
  )
  const indicadorIedMunicipioQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'se.ied_municipio_queretaro'
  )
  const indicadorLlegadaTurQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'inegi.llegada_total_turistas_queretaro'
  )
  const indicadorTurNacIntQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'sectur.turismo_nacional_vs_internacional_queretaro'
  )
  const indicadorDerramaTurQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'sectur.derrama_economica_turistica_queretaro'
  )
  const indicadorOcupHotelQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'sectur.ocupacion_hotelera_queretaro'
  )
  const indicadorCrecHabOcupQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'sectur.crecimiento_habitaciones_ocupadas_queretaro'
  )
  const indicadorPasajerosAiqQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'afac.pasajeros_anuales_aiq_queretaro'
  )
  const indicadorCrecPasajerosAiqQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'afac.crecimiento_anual_pasajeros_aiq_queretaro'
  )
  const indicadorVuelosNacIntAiqQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'afac.vuelos_nacionales_internacionales_aiq_queretaro'
  )
  const indicadorToneladasAiqQro = indicadoresCiudadEconomia?.find(
    (i) => i.clave === 'afac.historico_toneladas_transportadas_aiq_queretaro'
  )
  const indicadorActivo = (...ids: IndicadorCiudadId[]) => ids.includes(selectedIndicador)
  const locPrefixBase = `loc:${selectedCityConfig.estado}:${selectedCityConfig.municipio}:`
  const locAgePrefixBase = `loc_age:${selectedCityConfig.estado}:${selectedCityConfig.municipio}:`
  const { data: datosResponse, isLoading: loadingPob } = useIndicadorDatos(indicadorPob?.id ?? null, {
    enabled: indicadorActivo('crecimiento', 'sexo'),
    entidadPrefix: indicadorActivo('crecimiento', 'sexo') ? locPrefixBase : undefined,
  })
  const { data: datosPiramideResponse, isLoading: loadingPiramide } = useIndicadorDatos(indicadorPiramide?.id ?? null, {
    enabled: indicadorActivo('edad'),
    entidadPrefix: indicadorActivo('edad') ? locAgePrefixBase : undefined,
  })
  const { data: datosOcupacionResponse, isLoading: loadingOcupacion } = useIndicadorDatos(
    indicadorOcupacion?.id ?? null,
    { enabled: indicadorActivo('ocupacion_turismo') }
  )
  const { data: datosComercioResponse, isLoading: loadingComercio } = useIndicadorDatos(
    indicadorComercio?.id ?? null,
    { enabled: indicadorActivo('comercio_internacional') }
  )
  const { data: datosComercioMtyResponse, isLoading: loadingComercioMty } = useIndicadorDatos(
    indicadorComercioMty?.id ?? null,
    { enabled: indicadorActivo('comercio_internacional_mty') }
  )
  const { data: datosExpImpMtyResponse, isLoading: loadingExpImpMty } = useIndicadorDatos(
    indicadorExpImpMty?.id ?? null,
    { enabled: indicadorActivo('export_import_mty') }
  )
  const { data: datosConectividadResponse, isLoading: loadingConectividad } = useIndicadorDatos(
    indicadorConectividad?.id ?? null,
    { enabled: indicadorActivo('conectividad_aerea') }
  )
  const { data: datosLlegadasPasajerosMtyResponse, isLoading: loadingLlegadasPasajerosMty } = useIndicadorDatos(
    indicadorLlegadasPasajerosMty?.id ?? null,
    { enabled: indicadorActivo('llegadas_pasajeros_mty') }
  )
  const { data: datosConexionesResponse, isLoading: loadingConexiones } = useIndicadorDatos(
    indicadorConexiones?.id ?? null,
    { enabled: indicadorActivo('conexiones_aereas') }
  )
  const { data: datosOfertaResponse, isLoading: loadingOferta } = useIndicadorDatos(
    indicadorOfertaTuristica?.id ?? null,
    { enabled: indicadorActivo('oferta_servicios_turisticos') }
  )
  const { data: datosOcupacionHoteleraResponse, isLoading: loadingOcupacionHotelera } = useIndicadorDatos(
    indicadorOcupacionHotelera?.id ?? null,
    { enabled: indicadorActivo('ocupacion_hotelera_promedio') }
  )
  const { data: datosLlegadaPernoctaResponse, isLoading: loadingLlegadaPernocta } = useIndicadorDatos(
    indicadorLlegadaPernocta?.id ?? null,
    { enabled: indicadorActivo('llegada_pernocta') }
  )
  const { data: datosGastoPromedioResponse, isLoading: loadingGastoPromedio } = useIndicadorDatos(
    indicadorGastoPromedio?.id ?? null,
    { enabled: indicadorActivo('gasto_promedio_diario') }
  )
  const { data: datosDerramaResponse, isLoading: loadingDerrama } = useIndicadorDatos(
    indicadorDerrama?.id ?? null,
    { enabled: indicadorActivo('derrama_economica') }
  )
  const { data: datosIngresoHoteleroResponse, isLoading: loadingIngresoHotelero } = useIndicadorDatos(
    indicadorIngresoHotelero?.id ?? null,
    { enabled: indicadorActivo('ingreso_hotelero') }
  )
  const { data: datosEstablecimientosResponse, isLoading: loadingEstablecimientos } = useIndicadorDatos(
    indicadorEstablecimientos?.id ?? null,
    { enabled: indicadorActivo('establecimientos_turisticos') }
  )
  const { data: datosVisitantesMtyResponse, isLoading: loadingVisitantesMty } = useIndicadorDatos(
    indicadorVisitantesMty?.id ?? null,
    { enabled: indicadorActivo('visitantes_nac_ext_mty') }
  )
  const { data: datosOcupacionHoteleraMtyResponse, isLoading: loadingOcupacionHoteleraMty } = useIndicadorDatos(
    indicadorOcupacionHoteleraMty?.id ?? null,
    { enabled: indicadorActivo('ocupacion_hotelera_mty') }
  )
  const { data: datosPeaQroResponse, isLoading: loadingPeaQro } = useIndicadorDatos(indicadorPeaQro?.id ?? null, {
    enabled: indicadorActivo('pea_queretaro'),
  })
  const { data: datosTplQroResponse, isLoading: loadingTplQro } = useIndicadorDatos(indicadorTplQro?.id ?? null, {
    enabled: indicadorActivo('participacion_laboral_queretaro'),
  })
  const { data: datosTdQroResponse, isLoading: loadingTdQro } = useIndicadorDatos(indicadorTdQro?.id ?? null, {
    enabled: indicadorActivo('desocupacion_queretaro'),
  })
  const { data: datosSectorialQroResponse, isLoading: loadingSectorialQro } = useIndicadorDatos(
    indicadorSectorialQro?.id ?? null,
    { enabled: indicadorActivo('sectorial_empleo_queretaro') }
  )
  const { data: datosPibTotalQroResponse, isLoading: loadingPibTotalQro } = useIndicadorDatos(
    indicadorPibTotalQro?.id ?? null,
    { enabled: indicadorActivo('pib_total_queretaro') }
  )
  const { data: datosPibPerCapitaQroResponse, isLoading: loadingPibPerCapitaQro } = useIndicadorDatos(
    indicadorPibPerCapitaQro?.id ?? null,
    { enabled: indicadorActivo('pib_per_capita_queretaro') }
  )
  const { data: datosPibSectorQroResponse, isLoading: loadingPibSectorQro } = useIndicadorDatos(
    indicadorPibSectorQro?.id ?? null,
    { enabled: indicadorActivo('pib_sector_queretaro') }
  )
  const { data: datosCrecEcoQroResponse, isLoading: loadingCrecEcoQro } = useIndicadorDatos(
    indicadorCrecEcoQro?.id ?? null,
    { enabled: indicadorActivo('crecimiento_economico_queretaro') }
  )
  const { data: datosExportQroResponse, isLoading: loadingExportQro } = useIndicadorDatos(
    indicadorExportQro?.id ?? null,
    { enabled: indicadorActivo('exportaciones_estatales_queretaro') }
  )
  const { data: datosImportQroResponse, isLoading: loadingImportQro } = useIndicadorDatos(
    indicadorImportQro?.id ?? null,
    { enabled: indicadorActivo('importaciones_estatales_queretaro') }
  )
  const { data: datosComNetoQroResponse, isLoading: loadingComNetoQro } = useIndicadorDatos(
    indicadorComNetoQro?.id ?? null,
    { enabled: indicadorActivo('comercio_internacional_neto_queretaro') }
  )
  const { data: datosBalNetoQroResponse, isLoading: loadingBalNetoQro } = useIndicadorDatos(
    indicadorBalNetoQro?.id ?? null,
    { enabled: indicadorActivo('balance_comercial_neto_queretaro') }
  )
  const { data: datosIedAnualQroResponse, isLoading: loadingIedAnualQro } = useIndicadorDatos(
    indicadorIedAnualQro?.id ?? null,
    { enabled: indicadorActivo('ied_anual_queretaro') }
  )
  const { data: datosIedMunicipioQroResponse, isLoading: loadingIedMunicipioQro } = useIndicadorDatos(
    indicadorIedMunicipioQro?.id ?? null,
    { enabled: indicadorActivo('ied_municipio_queretaro') }
  )
  const { data: datosLlegadaTurQroResponse, isLoading: loadingLlegadaTurQro } = useIndicadorDatos(
    indicadorLlegadaTurQro?.id ?? null,
    { enabled: indicadorActivo('llegada_total_turistas_queretaro') }
  )
  const { data: datosTurNacIntQroResponse, isLoading: loadingTurNacIntQro } = useIndicadorDatos(
    indicadorTurNacIntQro?.id ?? null,
    { enabled: indicadorActivo('turismo_nacional_internacional_queretaro') }
  )
  const { data: datosDerramaTurQroResponse, isLoading: loadingDerramaTurQro } = useIndicadorDatos(
    indicadorDerramaTurQro?.id ?? null,
    { enabled: indicadorActivo('derrama_turistica_queretaro') }
  )
  const { data: datosOcupHotelQroResponse, isLoading: loadingOcupHotelQro } = useIndicadorDatos(
    indicadorOcupHotelQro?.id ?? null,
    { enabled: indicadorActivo('ocupacion_hotelera_queretaro') }
  )
  const { data: datosCrecHabOcupQroResponse, isLoading: loadingCrecHabOcupQro } = useIndicadorDatos(
    indicadorCrecHabOcupQro?.id ?? null,
    { enabled: indicadorActivo('crecimiento_habitaciones_ocupadas_queretaro') }
  )
  const { data: datosPasajerosAiqQroResponse, isLoading: loadingPasajerosAiqQro } = useIndicadorDatos(
    indicadorPasajerosAiqQro?.id ?? null,
    { enabled: indicadorActivo('pasajeros_anuales_aiq_queretaro') }
  )
  const { data: datosCrecPasajerosAiqQroResponse, isLoading: loadingCrecPasajerosAiqQro } = useIndicadorDatos(
    indicadorCrecPasajerosAiqQro?.id ?? null,
    { enabled: indicadorActivo('crecimiento_anual_pasajeros_aiq_queretaro') }
  )
  const { data: datosVuelosNacIntAiqQroResponse, isLoading: loadingVuelosNacIntAiqQro } = useIndicadorDatos(
    indicadorVuelosNacIntAiqQro?.id ?? null,
    { enabled: indicadorActivo('vuelos_nacionales_internacionales_aiq_queretaro') }
  )
  const { data: datosToneladasAiqQroResponse, isLoading: loadingToneladasAiqQro } = useIndicadorDatos(
    indicadorToneladasAiqQro?.id ?? null,
    { enabled: indicadorActivo('historico_toneladas_transportadas_aiq_queretaro') }
  )

  const datos: DatoIndicador[] = datosResponse?.datos ?? []
  const datosPiramide: DatoIndicador[] = datosPiramideResponse?.datos ?? []
  const datosOcupacion: DatoIndicador[] = datosOcupacionResponse?.datos ?? []
  const datosComercio: DatoIndicador[] = datosComercioResponse?.datos ?? []
  const datosComercioMty: DatoIndicador[] = datosComercioMtyResponse?.datos ?? []
  const datosExpImpMty: DatoIndicador[] = datosExpImpMtyResponse?.datos ?? []
  const datosConectividad: DatoIndicador[] = datosConectividadResponse?.datos ?? []
  const datosLlegadasPasajerosMty: DatoIndicador[] = datosLlegadasPasajerosMtyResponse?.datos ?? []
  const datosConexiones: DatoIndicador[] = datosConexionesResponse?.datos ?? []
  const datosOferta: DatoIndicador[] = datosOfertaResponse?.datos ?? []
  const datosOcupacionHotelera: DatoIndicador[] = datosOcupacionHoteleraResponse?.datos ?? []
  const datosLlegadaPernocta: DatoIndicador[] = datosLlegadaPernoctaResponse?.datos ?? []
  const datosGastoPromedio: DatoIndicador[] = datosGastoPromedioResponse?.datos ?? []
  const datosDerrama: DatoIndicador[] = datosDerramaResponse?.datos ?? []
  const datosIngresoHotelero: DatoIndicador[] = datosIngresoHoteleroResponse?.datos ?? []
  const datosEstablecimientos: DatoIndicador[] = datosEstablecimientosResponse?.datos ?? []
  const datosVisitantesMty: DatoIndicador[] = datosVisitantesMtyResponse?.datos ?? []
  const datosOcupacionHoteleraMty: DatoIndicador[] = datosOcupacionHoteleraMtyResponse?.datos ?? []
  const datosPeaQro: DatoIndicador[] = datosPeaQroResponse?.datos ?? []
  const datosTplQro: DatoIndicador[] = datosTplQroResponse?.datos ?? []
  const datosTdQro: DatoIndicador[] = datosTdQroResponse?.datos ?? []
  const datosSectorialQro: DatoIndicador[] = datosSectorialQroResponse?.datos ?? []
  const datosPibTotalQro: DatoIndicador[] = datosPibTotalQroResponse?.datos ?? []
  const datosPibPerCapitaQro: DatoIndicador[] = datosPibPerCapitaQroResponse?.datos ?? []
  const datosPibSectorQro: DatoIndicador[] = datosPibSectorQroResponse?.datos ?? []
  const datosCrecEcoQro: DatoIndicador[] = datosCrecEcoQroResponse?.datos ?? []
  const datosExportQro: DatoIndicador[] = datosExportQroResponse?.datos ?? []
  const datosImportQro: DatoIndicador[] = datosImportQroResponse?.datos ?? []
  const datosComNetoQro: DatoIndicador[] = datosComNetoQroResponse?.datos ?? []
  const datosBalNetoQro: DatoIndicador[] = datosBalNetoQroResponse?.datos ?? []
  const datosIedAnualQro: DatoIndicador[] = datosIedAnualQroResponse?.datos ?? []
  const datosIedMunicipioQro: DatoIndicador[] = datosIedMunicipioQroResponse?.datos ?? []
  const datosLlegadaTurQro: DatoIndicador[] = datosLlegadaTurQroResponse?.datos ?? []
  const datosTurNacIntQro: DatoIndicador[] = datosTurNacIntQroResponse?.datos ?? []
  const datosDerramaTurQro: DatoIndicador[] = datosDerramaTurQroResponse?.datos ?? []
  const datosOcupHotelQro: DatoIndicador[] = datosOcupHotelQroResponse?.datos ?? []
  const datosCrecHabOcupQro: DatoIndicador[] = datosCrecHabOcupQroResponse?.datos ?? []
  const datosPasajerosAiqQro: DatoIndicador[] = datosPasajerosAiqQroResponse?.datos ?? []
  const datosCrecPasajerosAiqQro: DatoIndicador[] = datosCrecPasajerosAiqQroResponse?.datos ?? []
  const datosVuelosNacIntAiqQro: DatoIndicador[] = datosVuelosNacIntAiqQroResponse?.datos ?? []
  const datosToneladasAiqQro: DatoIndicador[] = datosToneladasAiqQroResponse?.datos ?? []
  const isLoading = loadingPob || loadingPiramide || loadingOcupacion || loadingComercio || loadingComercioMty || loadingExpImpMty || loadingConectividad || loadingLlegadasPasajerosMty || loadingConexiones || loadingOferta || loadingOcupacionHotelera || loadingLlegadaPernocta || loadingGastoPromedio || loadingDerrama || loadingIngresoHotelero || loadingEstablecimientos || loadingVisitantesMty || loadingOcupacionHoteleraMty || loadingPeaQro || loadingTplQro || loadingTdQro || loadingSectorialQro || loadingPibTotalQro || loadingPibPerCapitaQro || loadingPibSectorQro || loadingCrecEcoQro || loadingExportQro || loadingImportQro || loadingComNetoQro || loadingBalNetoQro || loadingIedAnualQro || loadingIedMunicipioQro || loadingLlegadaTurQro || loadingTurNacIntQro || loadingDerramaTurQro || loadingOcupHotelQro || loadingCrecHabOcupQro || loadingPasajerosAiqQro || loadingCrecPasajerosAiqQro || loadingVuelosNacIntAiqQro || loadingToneladasAiqQro

  const parsed: LocalidadPoint[] = useMemo(() => {
    return datos
      .filter((d) => d.entidad_clave?.startsWith('loc:'))
      .map((d) => {
        const parts = String(d.entidad_clave ?? '').split(':')
        if (parts.length < 5) return null
        const sexo = parts[parts.length - 1] as SexoKey
        const localidad = parts[parts.length - 2]
        const municipio = parts[parts.length - 3]
        const estado = parts.slice(1, parts.length - 3).join(':')
        const periodo = Number(d.periodo)
        if (!estado || !municipio || !localidad || !Number.isFinite(periodo)) return null
        if (!['total', 'mujeres', 'hombres'].includes(sexo)) return null
        return { estado, municipio, localidad, sexo, periodo, valor: Number(d.valor) }
      })
      .filter((x): x is LocalidadPoint => Boolean(x))
  }, [datos])

  const parsedPiramide: LocalidadPiramidePoint[] = useMemo(() => {
    return datosPiramide
      .filter((d) => d.entidad_clave?.startsWith('loc_age:'))
      .map((d) => {
        const parts = String(d.entidad_clave ?? '').split(':')
        if (parts.length < 6) return null
        const grupo = parts[parts.length - 1]
        const sexo = parts[parts.length - 2] as SexoPiramide
        const localidad = parts[parts.length - 3]
        const municipio = parts[parts.length - 4]
        const estado = parts.slice(1, parts.length - 4).join(':')
        const periodo = Number(d.periodo)
        if (!estado || !municipio || !localidad || !Number.isFinite(periodo)) return null
        if (!['hombres', 'mujeres'].includes(sexo)) return null
        return { estado, municipio, localidad, sexo, grupo, periodo, valor: Number(d.valor) }
      })
      .filter((x): x is LocalidadPiramidePoint => Boolean(x))
  }, [datosPiramide])

  const selectedCity = useMemo(
    () => CITY_CONFIG.find((c) => c.id === selectedCityId) ?? CITY_CONFIG[0],
    [selectedCityId]
  )

  useEffect(() => {
    const allowed = selectedCity.indicadores.map((i) => i.id)
    if (!allowed.length) {
      setSelectedIndicador('crecimiento')
      return
    }
    if (!allowed.includes(selectedIndicador)) setSelectedIndicador(allowed[0])
  }, [selectedCity, selectedIndicador])

  const cityRows = useMemo(() => {
    const base = parsed.filter(
      (r) => r.estado === selectedCity.estado && r.municipio === selectedCity.municipio
    )
    if (!base.length) return []

    const localidades = Array.from(new Set(base.map((r) => r.localidad)))
    const rowsByLocalidad = new Map<string, LocalidadPoint[]>()
    for (const row of base) {
      const curr = rowsByLocalidad.get(row.localidad) ?? []
      curr.push(row)
      rowsByLocalidad.set(row.localidad, curr)
    }

    // Elegimos localidad con mayor cobertura temporal; si hay empate,
    // respetamos preferencias de configuración de ciudad.
    const cobertura = localidades.map((loc) => {
      const rows = rowsByLocalidad.get(loc) ?? []
      const years = new Set(rows.map((r) => r.periodo)).size
      return { loc, years }
    })
    cobertura.sort((a, b) => b.years - a.years || a.loc.localeCompare(b.loc))

    const preferidas = selectedCity.localidadPreferida.filter((loc) => localidades.includes(loc))
    const pickedPreferida = preferidas
      .map((loc) => ({ loc, years: cobertura.find((c) => c.loc === loc)?.years ?? 0 }))
      .sort((a, b) => b.years - a.years)
      .at(0)?.loc

    const picked = pickedPreferida ?? cobertura[0]?.loc ?? localidades.sort((a, b) => a.localeCompare(b))[0]
    return base.filter((r) => r.localidad === picked)
  }, [parsed, selectedCity])

  const cityLocalidad = cityRows[0]?.localidad ?? selectedCity.label
  const cityPiramideRows = useMemo(
    () =>
      parsedPiramide.filter(
        (r) =>
          r.estado === selectedCity.estado &&
          r.municipio === selectedCity.municipio &&
          r.localidad === cityLocalidad
      ),
    [parsedPiramide, selectedCity, cityLocalidad]
  )

  const yearsAvailable = useMemo(() => {
    if (selectedIndicador === 'edad') {
      return [...new Set(cityPiramideRows.map((r) => r.periodo))].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'ocupacion_turismo') {
      const rows = selectedCity.id === 'merida'
        ? datosOcupacion
            .filter((d) => d.entidad_clave === 'ciudad:merida')
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'comercio_internacional') {
      const rows = selectedCity.id === 'merida'
        ? datosComercio
            .filter((d) => d.entidad_clave?.startsWith('ciudad:merida:'))
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'comercio_internacional_mty') {
      const rows = selectedCity.id === 'monterrey'
        ? datosComercioMty
            .filter((d) => d.entidad_clave === 'ciudad:monterrey')
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'export_import_mty') {
      const rows = selectedCity.id === 'monterrey'
        ? datosExpImpMty
            .filter((d) => d.entidad_clave?.startsWith('ciudad:monterrey:'))
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'llegadas_pasajeros_mty') {
      const rows = selectedCity.id === 'monterrey'
        ? datosLlegadasPasajerosMty
            .filter((d) => d.entidad_clave?.startsWith('ciudad:monterrey:'))
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'visitantes_nac_ext_mty') {
      const rows = selectedCity.id === 'monterrey'
        ? datosVisitantesMty
            .filter((d) => d.entidad_clave?.startsWith('ciudad:monterrey:'))
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'ocupacion_hotelera_mty') {
      const rows = selectedCity.id === 'monterrey'
        ? datosOcupacionHoteleraMty
            .filter((d) => d.entidad_clave?.startsWith('ciudad:monterrey:'))
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'conectividad_aerea') {
      const rows = selectedCity.id === 'merida'
        ? datosConectividad
            .filter((d) => d.entidad_clave === 'ciudad:merida')
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'conexiones_aereas') {
      const rows = selectedCity.id === 'merida'
        ? datosConexiones
            .filter((d) => d.entidad_clave?.startsWith('ciudad:merida:origen:'))
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'oferta_servicios_turisticos') {
      const rows = selectedCity.id === 'merida'
        ? datosOferta
            .filter((d) => d.entidad_clave?.startsWith('ciudad:merida:'))
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'ocupacion_hotelera_promedio') {
      const rows = selectedCity.id === 'merida'
        ? datosOcupacionHotelera
            .filter((d) => d.entidad_clave === 'ciudad:merida')
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'llegada_pernocta') {
      const rows = selectedCity.id === 'merida'
        ? datosLlegadaPernocta
            .filter((d) => d.entidad_clave?.startsWith('ciudad:merida:'))
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'gasto_promedio_diario') {
      const rows = selectedCity.id === 'merida'
        ? datosGastoPromedio
            .filter((d) => d.entidad_clave === 'ciudad:merida')
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'derrama_economica') {
      const rows = selectedCity.id === 'merida'
        ? datosDerrama
            .filter((d) => d.entidad_clave === 'ciudad:merida')
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'ingreso_hotelero') {
      const rows = selectedCity.id === 'merida'
        ? datosIngresoHotelero
            .filter((d) => d.entidad_clave === 'ciudad:merida')
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'establecimientos_turisticos') {
      const rows = selectedCity.id === 'merida'
        ? datosEstablecimientos
            .filter((d) => d.entidad_clave === 'ciudad:merida')
            .map((d) => Number(d.periodo))
            .filter((y) => Number.isFinite(y))
        : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'pea_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosPeaQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'participacion_laboral_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosTplQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'desocupacion_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosTdQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'sectorial_empleo_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosSectorialQro
              .filter((d) => d.entidad_clave?.startsWith('ciudad:queretaro:'))
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'pib_total_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosPibTotalQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'pib_per_capita_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosPibPerCapitaQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'pib_sector_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosPibSectorQro
              .filter((d) => d.entidad_clave?.startsWith('ciudad:queretaro:'))
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'crecimiento_economico_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosCrecEcoQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'exportaciones_estatales_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosExportQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'importaciones_estatales_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosImportQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'comercio_internacional_neto_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosComNetoQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'balance_comercial_neto_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosBalNetoQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'ied_anual_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosIedAnualQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'ied_municipio_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosIedMunicipioQro
              .filter((d) => d.entidad_clave?.startsWith('ciudad:queretaro:'))
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'llegada_total_turistas_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosLlegadaTurQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'turismo_nacional_internacional_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosTurNacIntQro
              .filter((d) => d.entidad_clave?.startsWith('ciudad:queretaro:'))
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'derrama_turistica_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosDerramaTurQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'ocupacion_hotelera_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosOcupHotelQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'crecimiento_habitaciones_ocupadas_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosCrecHabOcupQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'pasajeros_anuales_aiq_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosPasajerosAiqQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'crecimiento_anual_pasajeros_aiq_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosCrecPasajerosAiqQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'vuelos_nacionales_internacionales_aiq_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosVuelosNacIntAiqQro
              .filter((d) => d.entidad_clave?.startsWith('ciudad:queretaro:'))
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    if (selectedIndicador === 'historico_toneladas_transportadas_aiq_queretaro') {
      const rows =
        selectedCity.id === 'queretaro'
          ? datosToneladasAiqQro
              .filter((d) => d.entidad_clave === 'ciudad:queretaro')
              .map((d) => Number(d.periodo))
              .filter((y) => Number.isFinite(y))
          : []
      return [...new Set(rows)].sort((a, b) => a - b)
    }
    return [...new Set(cityRows.map((r) => r.periodo))].sort((a, b) => a - b)
  }, [cityRows, cityPiramideRows, selectedIndicador, datosOcupacion, datosComercio, datosComercioMty, datosExpImpMty, datosConectividad, datosLlegadasPasajerosMty, datosVisitantesMty, datosOcupacionHoteleraMty, datosConexiones, datosOferta, datosOcupacionHotelera, datosLlegadaPernocta, datosGastoPromedio, datosDerrama, datosIngresoHotelero, datosEstablecimientos, datosPeaQro, datosTplQro, datosTdQro, datosSectorialQro, datosPibTotalQro, datosPibPerCapitaQro, datosPibSectorQro, datosCrecEcoQro, datosExportQro, datosImportQro, datosComNetoQro, datosBalNetoQro, datosIedAnualQro, datosIedMunicipioQro, datosLlegadaTurQro, datosTurNacIntQro, datosDerramaTurQro, datosOcupHotelQro, datosCrecHabOcupQro, datosPasajerosAiqQro, datosCrecPasajerosAiqQro, datosVuelosNacIntAiqQro, datosToneladasAiqQro, selectedCity.id])

  useEffect(() => {
    if (!yearsAvailable.length) {
      setYearFrom(null)
      setYearTo(null)
      return
    }
    setYearFrom((prev) => (prev && yearsAvailable.includes(prev) ? prev : yearsAvailable[0]))
    setYearTo((prev) =>
      prev && yearsAvailable.includes(prev) ? prev : yearsAvailable[yearsAvailable.length - 1]
    )
  }, [yearsAvailable])

  const range = useMemo(() => {
    if (yearFrom === null || yearTo === null) return null
    const [from, to] = clampRange(yearFrom, yearTo)
    return { from, to }
  }, [yearFrom, yearTo])

  const latestYear = cityRows.length ? Math.max(...cityRows.map((r) => r.periodo)) : null
  const kpiValue = (sexo: SexoKey): number | null => {
    const row = cityRows.find((r) => r.periodo === latestYear && r.sexo === sexo)
    return row ? row.valor : null
  }

  const growthData = useMemo(() => {
    if (!range) return []
    return [...new Set(cityRows.map((r) => r.periodo))]
      .filter((y) => y >= range.from && y <= range.to)
      .sort((a, b) => a - b)
      .map((anio) => ({
        anio,
        total: cityRows.find((r) => r.periodo === anio && r.sexo === 'total')?.valor ?? 0,
      }))
  }, [cityRows, range])

  const sexoData = useMemo(() => {
    if (!range) return []
    return [...new Set(cityRows.map((r) => r.periodo))]
      .filter((y) => y >= range.from && y <= range.to)
      .sort((a, b) => a - b)
      .map((anio) => ({
        anio,
        hombres: cityRows.find((r) => r.periodo === anio && r.sexo === 'hombres')?.valor ?? 0,
        mujeres: cityRows.find((r) => r.periodo === anio && r.sexo === 'mujeres')?.valor ?? 0,
      }))
  }, [cityRows, range])

  const piramideYear = useMemo(() => {
    if (!range || !cityPiramideRows.length) return null
    const inRange = [...new Set(cityPiramideRows.map((r) => r.periodo))]
      .filter((y) => y >= range.from && y <= range.to)
      .sort((a, b) => a - b)
    return inRange.length ? inRange[inRange.length - 1] : null
  }, [cityPiramideRows, range])

  const piramideData = useMemo(() => {
    if (!piramideYear) return []
    const youngestToOldest = ['0-4', '5-9', '10-14', '15-24', '25-34', '35-44', '45-54', '55-64', '65-74', '75-84', '85+']
    const expectedTopToBottom = [...youngestToOldest].reverse()
    const bucket = cityPiramideRows.filter((r) => r.periodo === piramideYear)
    const gruposDisponibles = [...new Set(bucket.map((r) => r.grupo))]
    const gruposOrden = expectedTopToBottom.filter((g) => gruposDisponibles.includes(g))
    return gruposOrden.map((grupo) => {
      const h = bucket.find((r) => r.grupo === grupo && r.sexo === 'hombres')?.valor ?? 0
      const m = bucket.find((r) => r.grupo === grupo && r.sexo === 'mujeres')?.valor ?? 0
      return { grupo, hombres: -Math.abs(h), mujeres: Math.abs(m), hombresAbs: h, mujeresAbs: m }
    })
  }, [cityPiramideRows, piramideYear])

  const xMaxPiramide = useMemo(() => {
    if (!piramideData.length) return 1
    const maxVal = Math.max(...piramideData.flatMap((d) => [Math.abs(d.hombres), Math.abs(d.mujeres)]))
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [piramideData])
  const yMaxGrowth = useMemo(() => {
    const maxVal = growthData.length ? Math.max(...growthData.map((d) => d.total)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [growthData])
  const yMaxSexo = useMemo(() => {
    const maxVal = sexoData.length ? Math.max(...sexoData.flatMap((d) => [d.mujeres, d.hombres])) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [sexoData])
  const entidadBase = `ciudad:${selectedCity.id}:${cityLocalidad}`
  const entidadCrecimiento = `${entidadBase}:crecimiento:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const entidadSexo = `${entidadBase}:sexo:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const entidadEdad = `${entidadBase}:edad:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadCrecimiento = growthData.map((d) => ({ anio: d.anio, total: d.total }))
  const payloadSexo = sexoData.map((d) => ({ anio: d.anio, mujeres: d.mujeres, hombres: d.hombres }))
  const payloadEdad = piramideData.map((d) => ({ grupo: d.grupo, hombres: d.hombresAbs, mujeres: d.mujeresAbs }))
  const peaQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosPeaQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), pea: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.pea) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosPeaQro, selectedCity.id, range])
  const yMaxPeaQro = useMemo(() => {
    const maxVal = peaQroData.length ? Math.max(...peaQroData.map((d) => d.pea)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [peaQroData])
  const entidadPeaQro = `ciudad:${selectedCity.id}:pea_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadPeaQro = peaQroData.map((d) => ({ anio: d.anio, pea_miles: d.pea }))
  const tplQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosTplQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), tasa: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.tasa) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosTplQro, selectedCity.id, range])
  const yMaxTplQro = useMemo(() => {
    const maxVal = tplQroData.length ? Math.max(...tplQroData.map((d) => d.tasa)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [tplQroData])
  const entidadTplQro = `ciudad:${selectedCity.id}:participacion_laboral_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadTplQro = tplQroData.map((d) => ({ anio: d.anio, participacion_pct: d.tasa }))
  const tdQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosTdQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), tasa: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.tasa) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosTdQro, selectedCity.id, range])
  const yMaxTdQro = useMemo(() => {
    const maxVal = tdQroData.length ? Math.max(...tdQroData.map((d) => d.tasa)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [tdQroData])
  const entidadTdQro = `ciudad:${selectedCity.id}:desocupacion_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadTdQro = tdQroData.map((d) => ({ anio: d.anio, desocupacion_pct: d.tasa }))
  const sectorialQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    const toNum = (v: unknown): number => {
      const n = Number(v)
      return Number.isFinite(n) ? n : 0
    }
    const years = [...new Set(
      datosSectorialQro
        .filter((d) => d.entidad_clave?.startsWith('ciudad:queretaro:'))
        .map((d) => Number(d.periodo))
        .filter((y) => Number.isFinite(y) && y >= range.from && y <= range.to)
    )].sort((a, b) => a - b)
    return years.map((anio) => ({
      anio,
      primario:
        toNum(datosSectorialQro.find(
          (d) => d.entidad_clave === 'ciudad:queretaro:primario' && Number(d.periodo) === anio
        )?.valor ?? 0),
      secundario:
        toNum(datosSectorialQro.find(
          (d) => d.entidad_clave === 'ciudad:queretaro:secundario' && Number(d.periodo) === anio
        )?.valor ?? 0),
      terciario:
        toNum(datosSectorialQro.find(
          (d) => d.entidad_clave === 'ciudad:queretaro:terciario' && Number(d.periodo) === anio
        )?.valor ?? 0),
    }))
  }, [datosSectorialQro, selectedCity.id, range])
  const yMaxSectorialQro = useMemo(() => {
    const maxVal = sectorialQroData.length
      ? Math.max(...sectorialQroData.map((d) => d.primario + d.secundario + d.terciario))
      : 0
    return Math.max(1, Math.ceil(maxVal * 1.05))
  }, [sectorialQroData])
  const entidadSectorialQro = `ciudad:${selectedCity.id}:sectorial_empleo_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadSectorialQro = sectorialQroData.map((d) => ({
    anio: d.anio,
    primario_pct: d.primario,
    secundario_pct: d.secundario,
    terciario_pct: d.terciario,
  }))
  const pibTotalQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosPibTotalQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), pib: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.pib) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosPibTotalQro, selectedCity.id, range])
  const yMaxPibTotalQro = useMemo(() => {
    const maxVal = pibTotalQroData.length ? Math.max(...pibTotalQroData.map((d) => d.pib)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [pibTotalQroData])
  const entidadPibTotalQro = `ciudad:${selectedCity.id}:pib_total_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadPibTotalQro = pibTotalQroData.map((d) => ({ anio: d.anio, pib_mdp: d.pib }))
  const pibPerCapitaQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosPibPerCapitaQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), pib_pc: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.pib_pc) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosPibPerCapitaQro, selectedCity.id, range])
  const yMaxPibPerCapitaQro = useMemo(() => {
    const maxVal = pibPerCapitaQroData.length ? Math.max(...pibPerCapitaQroData.map((d) => d.pib_pc)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [pibPerCapitaQroData])
  const entidadPibPerCapitaQro = `ciudad:${selectedCity.id}:pib_per_capita_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadPibPerCapitaQro = pibPerCapitaQroData.map((d) => ({ anio: d.anio, pib_per_capita: d.pib_pc }))
  const pibSectorQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    const toNum = (v: unknown): number => {
      const n = Number(v)
      return Number.isFinite(n) ? n : 0
    }
    const years = [...new Set(
      datosPibSectorQro
        .filter((d) => d.entidad_clave?.startsWith('ciudad:queretaro:'))
        .map((d) => Number(d.periodo))
        .filter((y) => Number.isFinite(y) && y >= range.from && y <= range.to)
    )].sort((a, b) => a - b)
    return years.map((anio) => ({
      anio,
      primario: toNum(
        datosPibSectorQro.find(
          (d) => d.entidad_clave === 'ciudad:queretaro:primario' && Number(d.periodo) === anio
        )?.valor ?? 0
      ),
      secundario: toNum(
        datosPibSectorQro.find(
          (d) => d.entidad_clave === 'ciudad:queretaro:secundario' && Number(d.periodo) === anio
        )?.valor ?? 0
      ),
      terciario: toNum(
        datosPibSectorQro.find(
          (d) => d.entidad_clave === 'ciudad:queretaro:terciario' && Number(d.periodo) === anio
        )?.valor ?? 0
      ),
    }))
  }, [datosPibSectorQro, selectedCity.id, range])
  const yMaxPibSectorQro = useMemo(() => {
    const maxVal = pibSectorQroData.length
      ? Math.max(...pibSectorQroData.map((d) => d.primario + d.secundario + d.terciario))
      : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [pibSectorQroData])
  const entidadPibSectorQro = `ciudad:${selectedCity.id}:pib_sector_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadPibSectorQro = pibSectorQroData.map((d) => ({
    anio: d.anio,
    primario_mdp: d.primario,
    secundario_mdp: d.secundario,
    terciario_mdp: d.terciario,
  }))
  const crecEcoQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosCrecEcoQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), tasa: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.tasa) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosCrecEcoQro, selectedCity.id, range])
  const maxAbsCrecEcoQro = useMemo(() => {
    const maxVal = crecEcoQroData.length
      ? Math.max(...crecEcoQroData.map((d) => Math.abs(d.tasa)))
      : 0
    return Math.max(1, Math.ceil(maxVal * 1.2))
  }, [crecEcoQroData])
  const entidadCrecEcoQro = `ciudad:${selectedCity.id}:crecimiento_economico_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadCrecEcoQro = crecEcoQroData.map((d) => ({ anio: d.anio, crecimiento_pct: d.tasa }))
  const exportQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosExportQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), exportaciones: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.exportaciones) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosExportQro, selectedCity.id, range])
  const yMaxExportQro = useMemo(() => {
    const maxVal = exportQroData.length ? Math.max(...exportQroData.map((d) => d.exportaciones)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [exportQroData])
  const entidadExportQro = `ciudad:${selectedCity.id}:exportaciones_estatales_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadExportQro = exportQroData.map((d) => ({ anio: d.anio, exportaciones_musd: d.exportaciones }))
  const importQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosImportQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), importaciones: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.importaciones) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosImportQro, selectedCity.id, range])
  const yMaxImportQro = useMemo(() => {
    const maxVal = importQroData.length ? Math.max(...importQroData.map((d) => d.importaciones)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [importQroData])
  const entidadImportQro = `ciudad:${selectedCity.id}:importaciones_estatales_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadImportQro = importQroData.map((d) => ({ anio: d.anio, importaciones_musd: d.importaciones }))
  const comNetoQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosComNetoQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), valor: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.valor) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosComNetoQro, selectedCity.id, range])
  const yMaxComNetoQro = useMemo(() => {
    const maxVal = comNetoQroData.length ? Math.max(...comNetoQroData.map((d) => d.valor)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [comNetoQroData])
  const entidadComNetoQro = `ciudad:${selectedCity.id}:comercio_internacional_neto_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadComNetoQro = comNetoQroData.map((d) => ({ anio: d.anio, comercio_neto_musd: d.valor }))
  const balNetoQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosBalNetoQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), valor: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.valor) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosBalNetoQro, selectedCity.id, range])
  const maxAbsBalNetoQro = useMemo(() => {
    const maxVal = balNetoQroData.length ? Math.max(...balNetoQroData.map((d) => Math.abs(d.valor))) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [balNetoQroData])
  const entidadBalNetoQro = `ciudad:${selectedCity.id}:balance_comercial_neto_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadBalNetoQro = balNetoQroData.map((d) => ({ anio: d.anio, balance_neto_musd: d.valor }))
  const iedAnualQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosIedAnualQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), ied: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.ied) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosIedAnualQro, selectedCity.id, range])
  const yMaxIedAnualQro = useMemo(() => {
    const maxVal = iedAnualQroData.length ? Math.max(...iedAnualQroData.map((d) => d.ied)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [iedAnualQroData])
  const entidadIedAnualQro = `ciudad:${selectedCity.id}:ied_anual_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadIedAnualQro = iedAnualQroData.map((d) => ({ anio: d.anio, ied_musd: d.ied }))
  const iedMunicipioQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    const toNum = (v: unknown): number => {
      const n = Number(v)
      return Number.isFinite(n) ? n : 0
    }
    const years = [...new Set(
      datosIedMunicipioQro
        .filter((d) => d.entidad_clave?.startsWith('ciudad:queretaro:'))
        .map((d) => Number(d.periodo))
        .filter((y) => Number.isFinite(y) && y >= range.from && y <= range.to)
    )].sort((a, b) => a - b)
    return years.map((anio) => ({
      anio,
      queretaro: toNum(
        datosIedMunicipioQro.find((d) => d.entidad_clave === 'ciudad:queretaro:queretaro' && Number(d.periodo) === anio)?.valor ?? 0
      ),
      elMarques: toNum(
        datosIedMunicipioQro.find((d) => d.entidad_clave === 'ciudad:queretaro:el_marques' && Number(d.periodo) === anio)?.valor ?? 0
      ),
      sanJuanDelRio: toNum(
        datosIedMunicipioQro.find((d) => d.entidad_clave === 'ciudad:queretaro:san_juan_del_rio' && Number(d.periodo) === anio)?.valor ?? 0
      ),
    }))
  }, [datosIedMunicipioQro, selectedCity.id, range])
  const yMaxIedMunicipioQro = useMemo(() => {
    const maxVal = iedMunicipioQroData.length
      ? Math.max(...iedMunicipioQroData.map((d) => Math.max(d.queretaro, d.elMarques, d.sanJuanDelRio)))
      : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [iedMunicipioQroData])
  const entidadIedMunicipioQro = `ciudad:${selectedCity.id}:ied_municipio_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadIedMunicipioQro = iedMunicipioQroData.map((d) => ({
    anio: d.anio,
    queretaro_musd: d.queretaro,
    el_marques_musd: d.elMarques,
    san_juan_del_rio_musd: d.sanJuanDelRio,
  }))
  const llegadaTurQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosLlegadaTurQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), turistas: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.turistas) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosLlegadaTurQro, selectedCity.id, range])
  const yMaxLlegadaTurQro = useMemo(() => {
    const maxVal = llegadaTurQroData.length ? Math.max(...llegadaTurQroData.map((d) => d.turistas)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [llegadaTurQroData])
  const entidadLlegadaTurQro = `ciudad:${selectedCity.id}:llegada_total_turistas_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadLlegadaTurQro = llegadaTurQroData.map((d) => ({ anio: d.anio, turistas: d.turistas }))
  const turNacIntQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    const toNum = (v: unknown): number => {
      const n = Number(v)
      return Number.isFinite(n) ? n : 0
    }
    const years = [...new Set(
      datosTurNacIntQro
        .filter((d) => d.entidad_clave?.startsWith('ciudad:queretaro:'))
        .map((d) => Number(d.periodo))
        .filter((y) => Number.isFinite(y) && y >= range.from && y <= range.to)
    )].sort((a, b) => a - b)
    return years.map((anio) => ({
      anio,
      nacionales: toNum(
        datosTurNacIntQro.find((d) => d.entidad_clave === 'ciudad:queretaro:nacionales' && Number(d.periodo) === anio)?.valor ?? 0
      ),
      internacionales: toNum(
        datosTurNacIntQro.find((d) => d.entidad_clave === 'ciudad:queretaro:internacionales' && Number(d.periodo) === anio)?.valor ?? 0
      ),
    }))
  }, [datosTurNacIntQro, selectedCity.id, range])
  const yMaxTurNacIntQro = useMemo(() => {
    const maxVal = turNacIntQroData.length
      ? Math.max(...turNacIntQroData.map((d) => Math.max(d.nacionales, d.internacionales)))
      : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [turNacIntQroData])
  const entidadTurNacIntQro = `ciudad:${selectedCity.id}:turismo_nacional_internacional_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadTurNacIntQro = turNacIntQroData.map((d) => ({
    anio: d.anio,
    nacionales: d.nacionales,
    internacionales: d.internacionales,
  }))
  const derramaTurQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosDerramaTurQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), derrama: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.derrama) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosDerramaTurQro, selectedCity.id, range])
  const yMaxDerramaTurQro = useMemo(() => {
    const maxVal = derramaTurQroData.length ? Math.max(...derramaTurQroData.map((d) => d.derrama)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [derramaTurQroData])
  const entidadDerramaTurQro = `ciudad:${selectedCity.id}:derrama_turistica_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadDerramaTurQro = derramaTurQroData.map((d) => ({ anio: d.anio, derrama_millones_mxn: d.derrama }))
  const ocupHotelQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosOcupHotelQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), porcentaje: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.porcentaje) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosOcupHotelQro, selectedCity.id, range])
  const yMaxOcupHotelQro = useMemo(() => {
    const maxVal = ocupHotelQroData.length ? Math.max(...ocupHotelQroData.map((d) => d.porcentaje)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.1))
  }, [ocupHotelQroData])
  const entidadOcupHotelQro = `ciudad:${selectedCity.id}:ocupacion_hotelera_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadOcupHotelQro = ocupHotelQroData.map((d) => ({ anio: d.anio, ocupacion_pct: d.porcentaje }))
  const crecHabOcupQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosCrecHabOcupQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), crecimiento: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.crecimiento) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosCrecHabOcupQro, selectedCity.id, range])
  const maxAbsCrecHabOcupQro = useMemo(() => {
    const maxVal = crecHabOcupQroData.length ? Math.max(...crecHabOcupQroData.map((d) => Math.abs(d.crecimiento))) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [crecHabOcupQroData])
  const entidadCrecHabOcupQro = `ciudad:${selectedCity.id}:crecimiento_habitaciones_ocupadas_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadCrecHabOcupQro = crecHabOcupQroData.map((d) => ({ anio: d.anio, crecimiento_pct: d.crecimiento }))
  const pasajerosAiqQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosPasajerosAiqQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), pasajeros: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.pasajeros) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosPasajerosAiqQro, selectedCity.id, range])
  const yMaxPasajerosAiqQro = useMemo(() => {
    const maxVal = pasajerosAiqQroData.length ? Math.max(...pasajerosAiqQroData.map((d) => d.pasajeros)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [pasajerosAiqQroData])
  const entidadPasajerosAiqQro = `ciudad:${selectedCity.id}:pasajeros_anuales_aiq_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadPasajerosAiqQro = pasajerosAiqQroData.map((d) => ({ anio: d.anio, pasajeros: d.pasajeros }))
  const crecPasajerosAiqQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosCrecPasajerosAiqQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), crecimiento: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.crecimiento) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosCrecPasajerosAiqQro, selectedCity.id, range])
  const maxAbsCrecPasajerosAiqQro = useMemo(() => {
    const maxVal = crecPasajerosAiqQroData.length ? Math.max(...crecPasajerosAiqQroData.map((d) => Math.abs(d.crecimiento))) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [crecPasajerosAiqQroData])
  const entidadCrecPasajerosAiqQro = `ciudad:${selectedCity.id}:crecimiento_anual_pasajeros_aiq_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadCrecPasajerosAiqQro = crecPasajerosAiqQroData.map((d) => ({ anio: d.anio, crecimiento_pct: d.crecimiento }))
  const vuelosNacIntAiqQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    const years = [...new Set(
      datosVuelosNacIntAiqQro
        .filter((d) => d.entidad_clave?.startsWith('ciudad:queretaro:'))
        .map((d) => Number(d.periodo))
        .filter((y) => Number.isFinite(y) && y >= range.from && y <= range.to)
    )].sort((a, b) => a - b)
    return years.map((anio) => ({
      anio,
      nacionales:
        Number(
          datosVuelosNacIntAiqQro.find(
            (d) => d.entidad_clave === 'ciudad:queretaro:nacionales' && Number(d.periodo) === anio
          )?.valor ?? 0
        ),
      internacionales:
        Number(
          datosVuelosNacIntAiqQro.find(
            (d) => d.entidad_clave === 'ciudad:queretaro:internacionales' && Number(d.periodo) === anio
          )?.valor ?? 0
        ),
    }))
  }, [datosVuelosNacIntAiqQro, selectedCity.id, range])
  const entidadVuelosNacIntAiqQro = `ciudad:${selectedCity.id}:vuelos_nacionales_internacionales_aiq_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadVuelosNacIntAiqQro = vuelosNacIntAiqQroData.map((d) => ({
    anio: d.anio,
    nacionales_pct: Number(d.nacionales.toFixed(2)),
    internacionales_pct: Number(d.internacionales.toFixed(2)),
  }))
  const toneladasAiqQroData = useMemo(() => {
    if (selectedCity.id !== 'queretaro' || !range) return []
    return datosToneladasAiqQro
      .filter((d) => d.entidad_clave === 'ciudad:queretaro')
      .map((d) => ({ anio: Number(d.periodo), toneladas: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.toneladas) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosToneladasAiqQro, selectedCity.id, range])
  const yMaxToneladasAiqQro = useMemo(() => {
    const maxVal = toneladasAiqQroData.length ? Math.max(...toneladasAiqQroData.map((d) => d.toneladas)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [toneladasAiqQroData])
  const entidadToneladasAiqQro = `ciudad:${selectedCity.id}:historico_toneladas_transportadas_aiq_queretaro:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadToneladasAiqQro = toneladasAiqQroData.map((d) => ({ anio: d.anio, toneladas: d.toneladas }))
  const ocupacionData = useMemo(() => {
    if (selectedCity.id !== 'merida' || !range) return []
    return datosOcupacion
      .filter((d) => d.entidad_clave === 'ciudad:merida')
      .map((d) => ({ anio: Number(d.periodo), valor: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosOcupacion, selectedCity.id, range])
  const yMaxOcupacion = useMemo(() => {
    const maxVal = ocupacionData.length ? Math.max(...ocupacionData.map((d) => d.valor)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [ocupacionData])
  const entidadOcupacion = `ciudad:${selectedCity.id}:ocupacion_turismo:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadOcupacion = ocupacionData.map((d) => ({ anio: d.anio, valor: d.valor }))
  const comercioData = useMemo(() => {
    if (selectedCity.id !== 'merida' || !range) return []
    const years = [...new Set(
      datosComercio
        .filter((d) => d.entidad_clave?.startsWith('ciudad:merida:'))
        .map((d) => Number(d.periodo))
        .filter((y) => Number.isFinite(y) && y >= range.from && y <= range.to)
    )].sort((a, b) => a - b)
    return years.map((anio) => ({
      anio,
      importaciones:
        datosComercio.find(
          (d) => d.entidad_clave === 'ciudad:merida:importaciones' && Number(d.periodo) === anio
        )?.valor ?? 0,
      exportaciones:
        datosComercio.find(
          (d) => d.entidad_clave === 'ciudad:merida:exportaciones' && Number(d.periodo) === anio
        )?.valor ?? 0,
    }))
  }, [datosComercio, selectedCity.id, range])
  const yMaxComercio = useMemo(() => {
    const maxVal = comercioData.length
      ? Math.max(...comercioData.flatMap((d) => [d.importaciones, d.exportaciones]))
      : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [comercioData])
  const entidadComercio = `ciudad:${selectedCity.id}:comercio_internacional:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadComercio = comercioData.map((d) => ({
    anio: d.anio,
    importaciones: d.importaciones,
    exportaciones: d.exportaciones,
  }))
  const comercioMtyData = useMemo(() => {
    if (selectedCity.id !== 'monterrey' || !range) return []
    return datosComercioMty
      .filter((d) => d.entidad_clave === 'ciudad:monterrey')
      .map((d) => ({ anio: Number(d.periodo), valor: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.valor) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosComercioMty, selectedCity.id, range])
  const yMaxComercioMty = useMemo(() => {
    const maxVal = comercioMtyData.length ? Math.max(...comercioMtyData.map((d) => d.valor)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [comercioMtyData])
  const entidadComercioMty = `ciudad:${selectedCity.id}:comercio_internacional_mty:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadComercioMty = comercioMtyData.map((d) => ({ anio: d.anio, valor_musd: d.valor }))
  const expImpMtyData = useMemo(() => {
    if (selectedCity.id !== 'monterrey' || !range) return []
    const years = [...new Set(
      datosExpImpMty
        .filter((d) => d.entidad_clave?.startsWith('ciudad:monterrey:'))
        .map((d) => Number(d.periodo))
        .filter((y) => Number.isFinite(y) && y >= range.from && y <= range.to)
    )].sort((a, b) => a - b)
    return years.map((anio) => ({
      anio,
      exportaciones:
        datosExpImpMty.find(
          (d) => d.entidad_clave === 'ciudad:monterrey:exportaciones' && Number(d.periodo) === anio
        )?.valor ?? 0,
      importaciones:
        datosExpImpMty.find(
          (d) => d.entidad_clave === 'ciudad:monterrey:importaciones' && Number(d.periodo) === anio
        )?.valor ?? 0,
    }))
  }, [datosExpImpMty, selectedCity.id, range])
  const yMaxExpImpMty = useMemo(() => {
    const maxVal = expImpMtyData.length
      ? Math.max(...expImpMtyData.flatMap((d) => [d.exportaciones, d.importaciones]))
      : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [expImpMtyData])
  const entidadExpImpMty = `ciudad:${selectedCity.id}:export_import_mty:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadExpImpMty = expImpMtyData.map((d) => ({
    anio: d.anio,
    exportaciones: d.exportaciones,
    importaciones: d.importaciones,
  }))
  const llegadasPasajerosMtyData = useMemo(() => {
    if (selectedCity.id !== 'monterrey' || !range) return []
    const years = [...new Set(
      datosLlegadasPasajerosMty
        .filter((d) => d.entidad_clave?.startsWith('ciudad:monterrey:'))
        .map((d) => Number(d.periodo))
        .filter((y) => Number.isFinite(y) && y >= range.from && y <= range.to)
    )].sort((a, b) => a - b)
    return years.map((anio) => ({
      anio,
      nacionales:
        datosLlegadasPasajerosMty.find(
          (d) => d.entidad_clave === 'ciudad:monterrey:nacionales' && Number(d.periodo) === anio
        )?.valor ?? 0,
      internacionales:
        datosLlegadasPasajerosMty.find(
          (d) => d.entidad_clave === 'ciudad:monterrey:internacionales' && Number(d.periodo) === anio
        )?.valor ?? 0,
    }))
  }, [datosLlegadasPasajerosMty, selectedCity.id, range])
  const yMaxLlegadasPasajerosMty = useMemo(() => {
    const maxVal = llegadasPasajerosMtyData.length
      ? Math.max(...llegadasPasajerosMtyData.flatMap((d) => [d.nacionales, d.internacionales]))
      : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [llegadasPasajerosMtyData])
  const entidadLlegadasPasajerosMty = `ciudad:${selectedCity.id}:llegadas_pasajeros:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadLlegadasPasajerosMty = llegadasPasajerosMtyData.map((d) => ({
    anio: d.anio,
    nacionales: d.nacionales,
    internacionales: d.internacionales,
  }))
  const visitantesNacExtMtyData = useMemo(() => {
    if (selectedCity.id !== 'monterrey' || !range) return []
    const years = [...new Set(
      datosVisitantesMty
        .filter((d) => d.entidad_clave?.startsWith('ciudad:monterrey:'))
        .map((d) => Number(d.periodo))
        .filter((y) => Number.isFinite(y) && y >= range.from && y <= range.to)
    )].sort((a, b) => a - b)
    return years.map((anio) => {
      const nacionales = Number(
        datosVisitantesMty.find(
          (d) => d.entidad_clave === 'ciudad:monterrey:nacionales' && Number(d.periodo) === anio
        )?.valor ?? 0
      )
      const extranjeros = Number(
        datosVisitantesMty.find(
          (d) => d.entidad_clave === 'ciudad:monterrey:extranjeros' && Number(d.periodo) === anio
        )?.valor ?? 0
      )
      const total = nacionales + extranjeros
      // Si la fuente trae porcentajes explícitos, los usamos; si no, los calculamos.
      const pctNacionalesFuente = Number(
        datosVisitantesMty.find(
          (d) => d.entidad_clave === 'ciudad:monterrey:pct_nacionales' && Number(d.periodo) === anio
        )?.valor ?? Number.NaN
      )
      const pctExtranjerosFuente = Number(
        datosVisitantesMty.find(
          (d) => d.entidad_clave === 'ciudad:monterrey:pct_extranjeros' && Number(d.periodo) === anio
        )?.valor ?? Number.NaN
      )
      const pctNacionales = Number.isFinite(pctNacionalesFuente)
        ? pctNacionalesFuente
        : total > 0 ? (nacionales / total) * 100 : 0
      const pctExtranjeros = Number.isFinite(pctExtranjerosFuente)
        ? pctExtranjerosFuente
        : total > 0 ? (extranjeros / total) * 100 : 0
      return {
        anio,
        nacionales: Number.isFinite(nacionales) ? nacionales : 0,
        extranjeros: Number.isFinite(extranjeros) ? extranjeros : 0,
        pctNacionales: Number.isFinite(pctNacionales) ? pctNacionales : 0,
        pctExtranjeros: Number.isFinite(pctExtranjeros) ? pctExtranjeros : 0,
      }
    })
  }, [datosVisitantesMty, selectedCity.id, range])
  const yMaxVisitantesMty = useMemo(() => {
    const maxVal = visitantesNacExtMtyData.length
      ? Math.max(...visitantesNacExtMtyData.flatMap((d) => [d.nacionales, d.extranjeros]))
      : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [visitantesNacExtMtyData])
  const entidadVisitantesMty = `ciudad:${selectedCity.id}:visitantes_nac_ext:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadVisitantesMty = visitantesNacExtMtyData.map((d) => ({
    anio: d.anio,
    nacionales: d.nacionales,
    extranjeros: d.extranjeros,
    pct_nacionales: Number(d.pctNacionales.toFixed(2)),
    pct_extranjeros: Number(d.pctExtranjeros.toFixed(2)),
  }))
  const ocupacionHoteleraMtyData = useMemo(() => {
    if (selectedCity.id !== 'monterrey' || !range) return []
    const years = [...new Set(
      datosOcupacionHoteleraMty
        .filter((d) => d.entidad_clave?.startsWith('ciudad:monterrey:'))
        .map((d) => Number(d.periodo))
        .filter((y) => Number.isFinite(y) && y >= range.from && y <= range.to)
    )].sort((a, b) => a - b)
    return years.map((anio) => {
      const cuartosDisponibles = Number(
        datosOcupacionHoteleraMty.find(
          (d) =>
            d.entidad_clave === 'ciudad:monterrey:cuartos_disponibles' &&
            Number(d.periodo) === anio
        )?.valor ?? 0
      )
      const cuartosOcupados = Number(
        datosOcupacionHoteleraMty.find(
          (d) => d.entidad_clave === 'ciudad:monterrey:cuartos_ocupados' && Number(d.periodo) === anio
        )?.valor ?? 0
      )
      const ocupacionFuente = Number(
        datosOcupacionHoteleraMty.find(
          (d) => d.entidad_clave === 'ciudad:monterrey:ocupacion_pct' && Number(d.periodo) === anio
        )?.valor ?? Number.NaN
      )
      const ocupacionCalculada =
        cuartosDisponibles > 0 ? (cuartosOcupados / cuartosDisponibles) * 100 : 0
      return {
        anio,
        ocupacionPct: Number.isFinite(ocupacionFuente) ? ocupacionFuente : ocupacionCalculada,
        cuartosDisponibles: Number.isFinite(cuartosDisponibles) ? cuartosDisponibles : 0,
        cuartosOcupados: Number.isFinite(cuartosOcupados) ? cuartosOcupados : 0,
      }
    })
  }, [datosOcupacionHoteleraMty, selectedCity.id, range])
  const entidadOcupacionHoteleraMty = `ciudad:${selectedCity.id}:ocupacion_hotelera_mty:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadOcupacionHoteleraMty = ocupacionHoteleraMtyData.map((d) => ({
    anio: d.anio,
    ocupacion_pct: d.ocupacionPct,
    cuartos_disponibles: d.cuartosDisponibles,
    cuartos_ocupados: d.cuartosOcupados,
  }))
  const conectividadData = useMemo(() => {
    if (selectedCity.id !== 'merida' || !range) return []
    return datosConectividad
      .filter((d) => d.entidad_clave === 'ciudad:merida')
      .map((d) => ({ anio: Number(d.periodo), valor: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosConectividad, selectedCity.id, range])
  const yMaxConectividad = useMemo(() => {
    const maxVal = conectividadData.length ? Math.max(...conectividadData.map((d) => d.valor)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [conectividadData])
  const entidadConectividad = `ciudad:${selectedCity.id}:conectividad_aerea:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadConectividad = conectividadData.map((d) => ({ anio: d.anio, operaciones: d.valor }))
  const conexionesData = useMemo(() => {
    if (selectedCity.id !== 'merida' || !range) return []
    const bucket = datosConexiones
      .filter((d) => d.entidad_clave?.startsWith('ciudad:merida:origen:'))
      .map((d) => ({
        origen: String(d.entidad_clave).replace('ciudad:merida:origen:', ''),
        anio: Number(d.periodo),
        valor: Number(d.valor),
      }))
      .filter((d) => Number.isFinite(d.anio) && d.anio >= range.from && d.anio <= range.to)

    const acc = new Map<string, number>()
    for (const row of bucket) {
      acc.set(row.origen, (acc.get(row.origen) ?? 0) + row.valor)
    }
    return [...acc.entries()]
      .map(([origen, vuelos]) => ({ origen, vuelos }))
      .sort((a, b) => b.vuelos - a.vuelos)
      .slice(0, 12)
  }, [datosConexiones, selectedCity.id, range])
  const yMaxConexiones = useMemo(() => {
    const maxVal = conexionesData.length ? Math.max(...conexionesData.map((d) => d.vuelos)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [conexionesData])
  const entidadConexiones = `ciudad:${selectedCity.id}:conexiones_aereas:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadConexiones = conexionesData.map((d) => ({ origen: d.origen, vuelos: d.vuelos }))
  const ofertaData = useMemo(() => {
    if (selectedCity.id !== 'merida' || !range) return []
    const years = [...new Set(
      datosOferta
        .filter((d) => d.entidad_clave?.startsWith('ciudad:merida:'))
        .map((d) => Number(d.periodo))
        .filter((y) => Number.isFinite(y) && y >= range.from && y <= range.to)
    )].sort((a, b) => a - b)
    return years.map((anio) => ({
      anio,
      servicios:
        datosOferta.find(
          (d) => d.entidad_clave === 'ciudad:merida:servicios' && Number(d.periodo) === anio
        )?.valor ?? 0,
      ventas:
        datosOferta.find(
          (d) => d.entidad_clave === 'ciudad:merida:ventas' && Number(d.periodo) === anio
        )?.valor ?? 0,
    }))
  }, [datosOferta, selectedCity.id, range])
  const yMaxOferta = useMemo(() => {
    const maxVal = ofertaData.length
      ? Math.max(...ofertaData.flatMap((d) => [d.servicios, d.ventas]))
      : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [ofertaData])
  const entidadOferta = `ciudad:${selectedCity.id}:oferta_servicios_turisticos:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadOferta = ofertaData.map((d) => ({ anio: d.anio, servicios: d.servicios, ventas: d.ventas }))
  const ocupacionHoteleraData = useMemo(() => {
    if (selectedCity.id !== 'merida' || !range) return []
    return datosOcupacionHotelera
      .filter((d) => d.entidad_clave === 'ciudad:merida')
      .map((d) => ({ anio: Number(d.periodo), porcentaje: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.porcentaje) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosOcupacionHotelera, selectedCity.id, range])
  const entidadOcupacionHotelera = `ciudad:${selectedCity.id}:ocupacion_hotelera_promedio:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadOcupacionHotelera = ocupacionHoteleraData.map((d) => ({ anio: d.anio, porcentaje: d.porcentaje }))
  const llegadaPernoctaData = useMemo(() => {
    if (selectedCity.id !== 'merida' || !range) return []
    const years = [...new Set(
      datosLlegadaPernocta
        .filter((d) => d.entidad_clave?.startsWith('ciudad:merida:'))
        .map((d) => Number(d.periodo))
        .filter((y) => Number.isFinite(y) && y >= range.from && y <= range.to)
    )].sort((a, b) => a - b)
    return years.map((anio) => ({
      anio,
      nacionales:
        datosLlegadaPernocta.find(
          (d) => d.entidad_clave === 'ciudad:merida:nacionales' && Number(d.periodo) === anio
        )?.valor ?? 0,
      internacionales:
        datosLlegadaPernocta.find(
          (d) => d.entidad_clave === 'ciudad:merida:internacionales' && Number(d.periodo) === anio
        )?.valor ?? 0,
    }))
  }, [datosLlegadaPernocta, selectedCity.id, range])
  const yMaxLlegadaPernocta = useMemo(() => {
    const maxVal = llegadaPernoctaData.length
      ? Math.max(...llegadaPernoctaData.flatMap((d) => [d.nacionales, d.internacionales]))
      : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [llegadaPernoctaData])
  const entidadLlegadaPernocta = `ciudad:${selectedCity.id}:llegada_pernocta:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadLlegadaPernocta = llegadaPernoctaData.map((d) => ({
    anio: d.anio,
    nacionales: d.nacionales,
    internacionales: d.internacionales,
  }))
  const gastoPromedioData = useMemo(() => {
    if (selectedCity.id !== 'merida' || !range) return []
    return datosGastoPromedio
      .filter((d) => d.entidad_clave === 'ciudad:merida')
      .map((d) => ({ anio: Number(d.periodo), valor: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.valor) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosGastoPromedio, selectedCity.id, range])
  const yMaxGastoPromedio = useMemo(() => {
    const maxVal = gastoPromedioData.length ? Math.max(...gastoPromedioData.map((d) => d.valor)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [gastoPromedioData])
  const entidadGastoPromedio = `ciudad:${selectedCity.id}:gasto_promedio_diario:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadGastoPromedio = gastoPromedioData.map((d) => ({ anio: d.anio, gasto_mxn: d.valor }))
  const derramaData = useMemo(() => {
    if (selectedCity.id !== 'merida' || !range) return []
    return datosDerrama
      .filter((d) => d.entidad_clave === 'ciudad:merida')
      .map((d) => ({ anio: Number(d.periodo), valor: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.valor) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosDerrama, selectedCity.id, range])
  const yMaxDerrama = useMemo(() => {
    const maxVal = derramaData.length ? Math.max(...derramaData.map((d) => d.valor)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [derramaData])
  const entidadDerrama = `ciudad:${selectedCity.id}:derrama_economica:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadDerrama = derramaData.map((d) => ({ anio: d.anio, derrama_millones_mxn: d.valor }))
  const ingresoHoteleroData = useMemo(() => {
    if (selectedCity.id !== 'merida' || !range) return []
    return datosIngresoHotelero
      .filter((d) => d.entidad_clave === 'ciudad:merida')
      .map((d) => ({ anio: Number(d.periodo), valor: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.valor) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosIngresoHotelero, selectedCity.id, range])
  const yMaxIngresoHotelero = useMemo(() => {
    const maxVal = ingresoHoteleroData.length ? Math.max(...ingresoHoteleroData.map((d) => d.valor)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [ingresoHoteleroData])
  const entidadIngresoHotelero = `ciudad:${selectedCity.id}:ingreso_hotelero:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadIngresoHotelero = ingresoHoteleroData.map((d) => ({ anio: d.anio, ingreso_millones_mxn: d.valor }))
  const establecimientosData = useMemo(() => {
    if (selectedCity.id !== 'merida' || !range) return []
    return datosEstablecimientos
      .filter((d) => d.entidad_clave === 'ciudad:merida')
      .map((d) => ({ anio: Number(d.periodo), valor: Number(d.valor) }))
      .filter((d) => Number.isFinite(d.anio) && Number.isFinite(d.valor) && d.anio >= range.from && d.anio <= range.to)
      .sort((a, b) => a.anio - b.anio)
  }, [datosEstablecimientos, selectedCity.id, range])
  const yMaxEstablecimientos = useMemo(() => {
    const maxVal = establecimientosData.length ? Math.max(...establecimientosData.map((d) => d.valor)) : 0
    return Math.max(1, Math.ceil(maxVal * 1.15))
  }, [establecimientosData])
  const entidadEstablecimientos = `ciudad:${selectedCity.id}:establecimientos_turisticos:${range?.from ?? 'na'}-${range?.to ?? 'na'}`
  const payloadEstablecimientos = establecimientosData.map((d) => ({ anio: d.anio, establecimientos: d.valor }))

  if (isLoading) {
    return (
      <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', height: '320px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#4a5568', fontSize: '13px', fontFamily }}>
        Cargando sección de ciudades...
      </div>
    )
  }

  return (
    <div style={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '10px', padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
        Ciudades
      </p>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '12px' }}>
        <div>
          <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '4px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Ciudad</label>
          <select
            value={selectedCityId}
            onChange={(e) => setSelectedCityId(e.target.value as CityId)}
            style={{ width: '100%', background: '#0f1117', border: '1px solid #2d3148', borderRadius: '4px', color: '#e2e8f0', fontSize: '14px', fontFamily, padding: '8px 10px', outline: 'none' }}
          >
            {CITY_CONFIG.map((city) => (
              <option key={city.id} value={city.id}>{city.label}</option>
            ))}
          </select>
        </div>

        <div>
          <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '4px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Indicador</label>
          <select
            value={selectedIndicador}
            onChange={(e) => setSelectedIndicador(e.target.value as IndicadorCiudadId)}
            disabled={!selectedCity.indicadores.length}
            style={{ width: '100%', background: '#0f1117', border: '1px solid #2d3148', borderRadius: '4px', color: '#e2e8f0', fontSize: '14px', fontFamily, padding: '8px 10px', outline: 'none', opacity: selectedCity.indicadores.length ? 1 : 0.6 }}
          >
            {selectedCity.indicadores.length ? (
              selectedCity.indicadores.map((ind) => (
                <option key={ind.id} value={ind.id}>{ind.label}</option>
              ))
            ) : (
              <option value="crecimiento">Sin indicadores configurados</option>
            )}
          </select>
        </div>

        <div>
          <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '4px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Año inicial</label>
          <select
            value={yearFrom ?? ''}
            onChange={(e) => setYearFrom(Number(e.target.value))}
            disabled={!yearsAvailable.length}
            style={{ width: '100%', background: '#0f1117', border: '1px solid #2d3148', borderRadius: '4px', color: '#e2e8f0', fontSize: '14px', fontFamily, padding: '8px 10px', outline: 'none' }}
          >
            {yearsAvailable.map((y) => <option key={y} value={y}>{y}</option>)}
          </select>
        </div>

        <div>
          <label style={{ fontSize: '11px', color: '#64748b', display: 'block', marginBottom: '4px', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Año final</label>
          <select
            value={yearTo ?? ''}
            onChange={(e) => setYearTo(Number(e.target.value))}
            disabled={!yearsAvailable.length}
            style={{ width: '100%', background: '#0f1117', border: '1px solid #2d3148', borderRadius: '4px', color: '#e2e8f0', fontSize: '14px', fontFamily, padding: '8px 10px', outline: 'none' }}
          >
            {yearsAvailable.map((y) => <option key={y} value={y}>{y}</option>)}
          </select>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '12px' }}>
        {[
          { label: 'Población total', value: kpiValue('total') },
          { label: 'Mujeres', value: kpiValue('mujeres') },
          { label: 'Hombres', value: kpiValue('hombres') },
        ].map((card, i) => (
          <div key={card.label} style={{ background: '#1a1d27', border: `1px solid ${palette[i % palette.length]}33`, borderTop: `3px solid ${palette[i % palette.length]}`, borderRadius: '10px', padding: '18px', fontFamily }}>
            <p style={{ fontSize: '11px', color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.08em', margin: '0 0 8px 0' }}>{card.label}</p>
            <p style={{ fontSize: '26px', fontWeight: 300, color: palette[i % palette.length], margin: '0 0 6px 0', lineHeight: 1 }}>
              {card.value === null ? '—' : fmt(card.value)}
            </p>
            <p style={{ fontSize: '11px', color: '#4a5568', margin: 0 }}>{latestYear ? `Año ${latestYear}` : '—'}</p>
          </div>
        ))}
      </div>

      <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-6px 0 0 0', textAlign: 'left' }}>
        Ciudad seleccionada: {selectedCity.label} ({selectedCity.estado}, {selectedCity.municipio}, localidad: {cityLocalidad}).
      </p>

      {!selectedCity.indicadores.length ? (
        <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
          Esta ciudad aún no tiene indicadores específicos configurados. Ya está lista la estructura para agregarlos.
        </div>
      ) : selectedIndicador === 'crecimiento' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Crecimiento poblacional anual
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-crecimiento`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {growthData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <BarChart data={growthData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxGrowth]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Total']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="total" fill={palette[0] ?? palette[1]} name="Total" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de crecimiento para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: CONAPO (base censal INEGI) — Serie anual de población por localidad.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Población total</th>
                </tr>
              </thead>
              <tbody>
                {growthData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.total)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorPob && (
            <AnalisisIA
              graficaId={indicadorPob.id}
              entidadClave={entidadCrecimiento}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Crecimiento poblacional anual',
                rango: range,
                localidad: cityLocalidad,
              }}
              datosFiltrados={payloadCrecimiento}
            />
          )}
          {indicadorPob && <AnalisisRevisado graficaId={indicadorPob.id} entidadClave={entidadCrecimiento} />}
        </>
      ) : selectedIndicador === 'sexo' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Distribución por sexo
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-sexo`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {sexoData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={sexoData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxSexo]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number, name: string) => [fmt(Number(value)), name]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="mujeres" stroke={palette[1] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Mujeres" />
                  <Line type="monotone" dataKey="hombres" stroke={palette[0] ?? palette[1]} strokeWidth={2} dot={{ r: 3 }} name="Hombres" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de distribución por sexo para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: CONAPO (base censal INEGI) — Serie anual por sexo para la localidad seleccionada.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Mujeres</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Hombres</th>
                </tr>
              </thead>
              <tbody>
                {sexoData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.mujeres)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.hombres)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorPob && (
            <AnalisisIA
              graficaId={indicadorPob.id}
              entidadClave={entidadSexo}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Distribución por sexo',
                rango: range,
                localidad: cityLocalidad,
              }}
              datosFiltrados={payloadSexo}
            />
          )}
          {indicadorPob && <AnalisisRevisado graficaId={indicadorPob.id} entidadClave={entidadSexo} />}
        </>
      ) : selectedIndicador === 'edad' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Distribución por edad ({piramideYear ?? 'N/A'})
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-edad`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {piramideData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={340}>
                <BarChart layout="vertical" data={piramideData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis type="number" domain={[-xMaxPiramide, xMaxPiramide]} tickFormatter={(v) => fmt(Math.abs(Number(v)))} tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis dataKey="grupo" type="category" tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number, name: string) => [fmt(Math.abs(Number(value))), name]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="hombres" fill={palette[0] ?? palette[1]} name="Hombres" />
                  <Bar dataKey="mujeres" fill={palette[1] ?? palette[0]} name="Mujeres" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de distribución por edad para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: CONAPO (base censal INEGI) — Pirámide poblacional de la localidad seleccionada.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Grupo de edad</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Hombres</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Mujeres</th>
                </tr>
              </thead>
              <tbody>
                {piramideData.map((row) => (
                  <tr key={row.grupo} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.grupo}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.hombresAbs)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.mujeresAbs)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p style={{ fontSize: '11px', color: '#4a5568', margin: 0 }}>
            Para pirámide se usa el último año disponible dentro del rango seleccionado.
          </p>
          {indicadorPiramide && (
            <AnalisisIA
              graficaId={indicadorPiramide.id}
              entidadClave={entidadEdad}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Distribución por edad',
                rango: range,
                localidad: cityLocalidad,
                anio_piramide: piramideYear,
              }}
              datosFiltrados={payloadEdad}
            />
          )}
          {indicadorPiramide && <AnalisisRevisado graficaId={indicadorPiramide.id} entidadClave={entidadEdad} />}
        </>
      ) : selectedIndicador === 'pea_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Población económicamente activa
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-pea`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {peaQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={peaQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxPeaQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'PEA (miles de personas)']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="pea" stroke={palette[0] ?? palette[1]} strokeWidth={2} dot={{ r: 3 }} name="PEA (miles de personas)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de PEA para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: INEGI — ENOE 15 años y más (datos abiertos). Serie referencial para Querétaro (ZM).
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>PEA (miles de personas)</th>
                </tr>
              </thead>
              <tbody>
                {peaQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.pea)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorPeaQro && (
            <AnalisisIA
              graficaId={indicadorPeaQro.id}
              entidadClave={entidadPeaQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Población económicamente activa',
                rango: range,
                unidad: 'Miles de personas',
              }}
              datosFiltrados={payloadPeaQro}
            />
          )}
          {indicadorPeaQro && <AnalisisRevisado graficaId={indicadorPeaQro.id} entidadClave={entidadPeaQro} />}
        </>
      ) : selectedIndicador === 'participacion_laboral_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Tasa de participación laboral
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-participacion-laboral`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {tplQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={tplQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxTplQro]} tickFormatter={(v) => `${Number(v).toFixed(1)}%`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [`${Number(value).toFixed(1)}%`, 'Participación laboral']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="tasa" stroke={palette[0] ?? palette[1]} strokeWidth={2} dot={{ r: 3 }} name="Participación laboral (%)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de participación laboral para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: INEGI — ENOE 15 años y más (tabulados), población económicamente activa respecto a la población de 15 años y más.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Participación laboral (%)</th>
                </tr>
              </thead>
              <tbody>
                {tplQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.tasa.toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorTplQro && (
            <AnalisisIA
              graficaId={indicadorTplQro.id}
              entidadClave={entidadTplQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Tasa de participación laboral',
                rango: range,
                unidad: 'Porcentaje',
              }}
              datosFiltrados={payloadTplQro}
            />
          )}
          {indicadorTplQro && <AnalisisRevisado graficaId={indicadorTplQro.id} entidadClave={entidadTplQro} />}
        </>
      ) : selectedIndicador === 'desocupacion_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Tasa de desocupación
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-desocupacion`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {tdQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={tdQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxTdQro]} tickFormatter={(v) => `${Number(v).toFixed(1)}%`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [`${Number(value).toFixed(1)}%`, 'Desocupación']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="tasa" stroke={palette[2] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Desocupación (%)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de desocupación para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: INEGI — ENOE 15 años y más (tabulados), porcentaje de la PEA en condición de desocupación.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Desocupación (%)</th>
                </tr>
              </thead>
              <tbody>
                {tdQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.tasa.toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorTdQro && (
            <AnalisisIA
              graficaId={indicadorTdQro.id}
              entidadClave={entidadTdQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Tasa de desocupación',
                rango: range,
                unidad: 'Porcentaje',
              }}
              datosFiltrados={payloadTdQro}
            />
          )}
          {indicadorTdQro && <AnalisisRevisado graficaId={indicadorTdQro.id} entidadClave={entidadTdQro} />}
        </>
      ) : selectedIndicador === 'sectorial_empleo_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Composición sectorial del empleo
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-sectorial-empleo`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {sectorialQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={340}>
                <BarChart data={sectorialQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxSectorialQro]} tickFormatter={(v) => `${Number(v).toFixed(0)}%`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number, name: string) => [`${Number(value).toFixed(1)}%`, name]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="primario" stackId="empleo" fill={palette[0] ?? palette[1]} name="Primario" />
                  <Bar dataKey="secundario" stackId="empleo" fill={palette[1] ?? palette[0]} name="Secundario" />
                  <Bar dataKey="terciario" stackId="empleo" fill={palette[2] ?? palette[0]} name="Terciario" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de composición sectorial para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: INEGI — ENOE 15 años y más (tabulados), distribución porcentual del empleo por sector.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Primario (%)</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Secundario (%)</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Terciario (%)</th>
                </tr>
              </thead>
              <tbody>
                {sectorialQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.primario.toFixed(1)}%</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.secundario.toFixed(1)}%</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.terciario.toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorSectorialQro && (
            <AnalisisIA
              graficaId={indicadorSectorialQro.id}
              entidadClave={entidadSectorialQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Composición sectorial del empleo',
                rango: range,
                unidad: 'Porcentaje',
              }}
              datosFiltrados={payloadSectorialQro}
            />
          )}
          {indicadorSectorialQro && <AnalisisRevisado graficaId={indicadorSectorialQro.id} entidadClave={entidadSectorialQro} />}
        </>
      ) : selectedIndicador === 'pib_total_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              PIB estatal total
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-pib-total`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {pibTotalQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={pibTotalQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxPibTotalQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'PIB (MDP)']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="pib" stroke={palette[0] ?? palette[1]} strokeWidth={2} dot={{ r: 3 }} name="PIB estatal total (Millones de pesos)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de PIB para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: INEGI tabulados — PIB estatal total anual de Querétaro a precios corrientes.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>PIB (Millones de pesos)</th>
                </tr>
              </thead>
              <tbody>
                {pibTotalQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.pib)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorPibTotalQro && (
            <AnalisisIA
              graficaId={indicadorPibTotalQro.id}
              entidadClave={entidadPibTotalQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'PIB estatal total',
                rango: range,
                unidad: 'Millones de pesos',
              }}
              datosFiltrados={payloadPibTotalQro}
            />
          )}
          {indicadorPibTotalQro && <AnalisisRevisado graficaId={indicadorPibTotalQro.id} entidadClave={entidadPibTotalQro} />}
        </>
      ) : selectedIndicador === 'pib_per_capita_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              PIB per cápita
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-pib-per-capita`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {pibPerCapitaQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={pibPerCapitaQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxPibPerCapitaQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'PIB per cápita']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="pib_pc" stroke={palette[1] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="PIB per cápita (Pesos por habitante)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de PIB per cápita para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: INEGI tabulados — PIB per cápita anual de Querétaro a precios corrientes.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>PIB per cápita (Pesos por habitante)</th>
                </tr>
              </thead>
              <tbody>
                {pibPerCapitaQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.pib_pc)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorPibPerCapitaQro && (
            <AnalisisIA
              graficaId={indicadorPibPerCapitaQro.id}
              entidadClave={entidadPibPerCapitaQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'PIB per cápita',
                rango: range,
                unidad: 'Pesos por habitante',
              }}
              datosFiltrados={payloadPibPerCapitaQro}
            />
          )}
          {indicadorPibPerCapitaQro && <AnalisisRevisado graficaId={indicadorPibPerCapitaQro.id} entidadClave={entidadPibPerCapitaQro} />}
        </>
      ) : selectedIndicador === 'pib_sector_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Distribución del PIB por sector
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-pib-sector`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {pibSectorQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={340}>
                <BarChart data={pibSectorQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxPibSectorQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number, name: string) => [fmt(Number(value)), name]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="primario" stackId="pib" fill={palette[0] ?? palette[1]} name="Primario (MDP)" />
                  <Bar dataKey="secundario" stackId="pib" fill={palette[1] ?? palette[0]} name="Secundario (MDP)" />
                  <Bar dataKey="terciario" stackId="pib" fill={palette[2] ?? palette[0]} name="Terciario (MDP)" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de PIB por sector para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: INEGI — PIB por sector (tabulados), serie anual para Querétaro.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Primario (MDP)</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Secundario (MDP)</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Terciario (MDP)</th>
                </tr>
              </thead>
              <tbody>
                {pibSectorQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.primario)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.secundario)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.terciario)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorPibSectorQro && (
            <AnalisisIA
              graficaId={indicadorPibSectorQro.id}
              entidadClave={entidadPibSectorQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Distribución del PIB por sector',
                rango: range,
                unidad: 'Millones de pesos',
              }}
              datosFiltrados={payloadPibSectorQro}
            />
          )}
          {indicadorPibSectorQro && <AnalisisRevisado graficaId={indicadorPibSectorQro.id} entidadClave={entidadPibSectorQro} />}
        </>
      ) : selectedIndicador === 'crecimiento_economico_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Tasa de crecimiento económico
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-crecimiento-economico`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {crecEcoQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={crecEcoQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[-maxAbsCrecEcoQro, maxAbsCrecEcoQro]} tickFormatter={(v) => `${Number(v).toFixed(1)}%`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [`${Number(value).toFixed(1)}%`, 'Crecimiento económico']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="tasa" stroke={palette[2] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Crecimiento económico (%)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de crecimiento económico para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: INEGI — PIB (tabulados), variación anual del PIB estatal de Querétaro.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Crecimiento económico (%)</th>
                </tr>
              </thead>
              <tbody>
                {crecEcoQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.tasa.toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorCrecEcoQro && (
            <AnalisisIA
              graficaId={indicadorCrecEcoQro.id}
              entidadClave={entidadCrecEcoQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Tasa de crecimiento económico',
                rango: range,
                unidad: 'Porcentaje',
              }}
              datosFiltrados={payloadCrecEcoQro}
            />
          )}
          {indicadorCrecEcoQro && <AnalisisRevisado graficaId={indicadorCrecEcoQro.id} entidadClave={entidadCrecEcoQro} />}
        </>
      ) : selectedIndicador === 'exportaciones_estatales_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Exportaciones estatales
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-exportaciones-estatales`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {exportQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={exportQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxExportQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Exportaciones']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="exportaciones" stroke={palette[5] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Exportaciones estatales (MUSD)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de exportaciones estatales para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Secretaría de Economía / datos.gob.mx — serie anual de exportaciones estatales (Querétaro).
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Exportaciones (MUSD)</th>
                </tr>
              </thead>
              <tbody>
                {exportQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.exportaciones)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorExportQro && (
            <AnalisisIA
              graficaId={indicadorExportQro.id}
              entidadClave={entidadExportQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Exportaciones estatales',
                rango: range,
                unidad: 'Millones USD',
              }}
              datosFiltrados={payloadExportQro}
            />
          )}
          {indicadorExportQro && <AnalisisRevisado graficaId={indicadorExportQro.id} entidadClave={entidadExportQro} />}
        </>
      ) : selectedIndicador === 'importaciones_estatales_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Importaciones estatales
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-importaciones-estatales`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {importQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={importQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxImportQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Importaciones']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="importaciones" stroke={palette[6] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Importaciones estatales (MUSD)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de importaciones estatales para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Secretaría de Economía / datos.gob.mx — serie anual de importaciones estatales (Querétaro).
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Importaciones (MUSD)</th>
                </tr>
              </thead>
              <tbody>
                {importQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.importaciones)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorImportQro && (
            <AnalisisIA
              graficaId={indicadorImportQro.id}
              entidadClave={entidadImportQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Importaciones estatales',
                rango: range,
                unidad: 'Millones USD',
              }}
              datosFiltrados={payloadImportQro}
            />
          )}
          {indicadorImportQro && <AnalisisRevisado graficaId={indicadorImportQro.id} entidadClave={entidadImportQro} />}
        </>
      ) : selectedIndicador === 'comercio_internacional_neto_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Comercio internacional neto
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-comercio-internacional-neto`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {comNetoQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={comNetoQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxComNetoQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Comercio internacional neto']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="valor" stroke={palette[2] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Comercio internacional neto (MUSD)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de comercio internacional neto para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Secretaría de Economía / datos.gob.mx — comercio internacional total anual (exportaciones + importaciones) de Querétaro.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Comercio internacional neto (MUSD)</th>
                </tr>
              </thead>
              <tbody>
                {comNetoQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.valor)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorComNetoQro && (
            <AnalisisIA
              graficaId={indicadorComNetoQro.id}
              entidadClave={entidadComNetoQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Comercio internacional neto',
                rango: range,
                unidad: 'Millones USD',
              }}
              datosFiltrados={payloadComNetoQro}
            />
          )}
          {indicadorComNetoQro && <AnalisisRevisado graficaId={indicadorComNetoQro.id} entidadClave={entidadComNetoQro} />}
        </>
      ) : selectedIndicador === 'balance_comercial_neto_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Balance comercial neto
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-balance-comercial-neto`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {balNetoQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={balNetoQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[-maxAbsBalNetoQro, maxAbsBalNetoQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Balance comercial neto']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="valor" stroke={palette[3] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Balance comercial neto (MUSD)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de balance comercial neto para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Secretaría de Economía / datos.gob.mx — balance comercial anual (exportaciones - importaciones) de Querétaro.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Balance comercial neto (MUSD)</th>
                </tr>
              </thead>
              <tbody>
                {balNetoQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.valor)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorBalNetoQro && (
            <AnalisisIA
              graficaId={indicadorBalNetoQro.id}
              entidadClave={entidadBalNetoQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Balance comercial neto',
                rango: range,
                unidad: 'Millones USD',
              }}
              datosFiltrados={payloadBalNetoQro}
            />
          )}
          {indicadorBalNetoQro && <AnalisisRevisado graficaId={indicadorBalNetoQro.id} entidadClave={entidadBalNetoQro} />}
        </>
      ) : selectedIndicador === 'ied_anual_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              IED anual
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-ied-anual`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {iedAnualQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={iedAnualQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxIedAnualQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'IED anual']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="ied" stroke={palette[4] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="IED anual (MUSD)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de IED anual para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Secretaría de Economía / datos.gob.mx — inversión extranjera directa anual de Querétaro.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>IED anual (MUSD)</th>
                </tr>
              </thead>
              <tbody>
                {iedAnualQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.ied)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorIedAnualQro && (
            <AnalisisIA
              graficaId={indicadorIedAnualQro.id}
              entidadClave={entidadIedAnualQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'IED anual',
                rango: range,
                unidad: 'Millones USD',
              }}
              datosFiltrados={payloadIedAnualQro}
            />
          )}
          {indicadorIedAnualQro && <AnalisisRevisado graficaId={indicadorIedAnualQro.id} entidadClave={entidadIedAnualQro} />}
        </>
      ) : selectedIndicador === 'ied_municipio_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              IED por municipio
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-ied-por-municipio`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {iedMunicipioQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <BarChart data={iedMunicipioQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxIedMunicipioQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number, name: string) => [fmt(Number(value)), name]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="queretaro" fill={palette[0] ?? palette[1]} name="Querétaro" />
                  <Bar dataKey="elMarques" fill={palette[1] ?? palette[0]} name="El Marqués" />
                  <Bar dataKey="sanJuanDelRio" fill={palette[2] ?? palette[0]} name="San Juan del Río" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de IED por municipio para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Secretaría de Economía / datos.gob.mx — IED anual por municipio en Querétaro.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Querétaro</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>El Marqués</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>San Juan del Río</th>
                </tr>
              </thead>
              <tbody>
                {iedMunicipioQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.queretaro)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.elMarques)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.sanJuanDelRio)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorIedMunicipioQro && (
            <AnalisisIA
              graficaId={indicadorIedMunicipioQro.id}
              entidadClave={entidadIedMunicipioQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'IED por municipio',
                rango: range,
                unidad: 'Millones USD',
              }}
              datosFiltrados={payloadIedMunicipioQro}
            />
          )}
          {indicadorIedMunicipioQro && <AnalisisRevisado graficaId={indicadorIedMunicipioQro.id} entidadClave={entidadIedMunicipioQro} />}
        </>
      ) : selectedIndicador === 'llegada_total_turistas_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Llegada total de turistas
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-llegada-total-turistas`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {llegadaTurQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={llegadaTurQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxLlegadaTurQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Turistas']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="turistas" stroke={palette[5] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Llegada total de turistas" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de llegada total de turistas para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: INEGI — Turismo (tabulados), llegada total anual de turistas en Querétaro.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Turistas</th>
                </tr>
              </thead>
              <tbody>
                {llegadaTurQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.turistas)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorLlegadaTurQro && (
            <AnalisisIA
              graficaId={indicadorLlegadaTurQro.id}
              entidadClave={entidadLlegadaTurQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Llegada total de turistas',
                rango: range,
                unidad: 'Turistas',
              }}
              datosFiltrados={payloadLlegadaTurQro}
            />
          )}
          {indicadorLlegadaTurQro && <AnalisisRevisado graficaId={indicadorLlegadaTurQro.id} entidadClave={entidadLlegadaTurQro} />}
        </>
      ) : selectedIndicador === 'turismo_nacional_internacional_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Turismo nacional vs internacional
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-turismo-nacional-vs-internacional`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {turNacIntQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={turNacIntQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxTurNacIntQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number, name: string) => [fmt(Number(value)), name]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="nacionales" stroke={palette[0] ?? palette[1]} strokeWidth={2} dot={{ r: 3 }} name="Nacionales" />
                  <Line type="monotone" dataKey="internacionales" stroke={palette[1] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Internacionales" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de turismo nacional vs internacional para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Datatur / Sectur — visitantes nacionales e internacionales en Querétaro.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Nacionales</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Internacionales</th>
                </tr>
              </thead>
              <tbody>
                {turNacIntQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.nacionales)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.internacionales)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorTurNacIntQro && (
            <AnalisisIA
              graficaId={indicadorTurNacIntQro.id}
              entidadClave={entidadTurNacIntQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Turismo nacional vs internacional',
                rango: range,
                unidad: 'Turistas',
              }}
              datosFiltrados={payloadTurNacIntQro}
            />
          )}
          {indicadorTurNacIntQro && <AnalisisRevisado graficaId={indicadorTurNacIntQro.id} entidadClave={entidadTurNacIntQro} />}
        </>
      ) : selectedIndicador === 'derrama_turistica_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Derrama económica turística
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-derrama-economica-turistica`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {derramaTurQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={derramaTurQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxDerramaTurQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Derrama turística']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="derrama" stroke={palette[6] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Derrama turística (Millones MXN)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de derrama económica turística para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Datatur / Sectur — derrama económica turística anual de Querétaro.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Derrama (Millones MXN)</th>
                </tr>
              </thead>
              <tbody>
                {derramaTurQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.derrama)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorDerramaTurQro && (
            <AnalisisIA
              graficaId={indicadorDerramaTurQro.id}
              entidadClave={entidadDerramaTurQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Derrama económica turística',
                rango: range,
                unidad: 'Millones MXN',
              }}
              datosFiltrados={payloadDerramaTurQro}
            />
          )}
          {indicadorDerramaTurQro && <AnalisisRevisado graficaId={indicadorDerramaTurQro.id} entidadClave={entidadDerramaTurQro} />}
        </>
      ) : selectedIndicador === 'ocupacion_hotelera_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Ocupación hotelera
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-ocupacion-hotelera`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {ocupHotelQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={ocupHotelQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxOcupHotelQro]} tickFormatter={(v) => `${Number(v).toFixed(1)}%`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [`${Number(value).toFixed(1)}%`, 'Ocupación hotelera']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="porcentaje" stroke={palette[2] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Ocupación hotelera (%)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de ocupación hotelera para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Datatur / Sectur — ocupación hotelera anual de Querétaro.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Ocupación hotelera (%)</th>
                </tr>
              </thead>
              <tbody>
                {ocupHotelQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.porcentaje.toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorOcupHotelQro && (
            <AnalisisIA
              graficaId={indicadorOcupHotelQro.id}
              entidadClave={entidadOcupHotelQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Ocupación hotelera',
                rango: range,
                unidad: 'Porcentaje',
              }}
              datosFiltrados={payloadOcupHotelQro}
            />
          )}
          {indicadorOcupHotelQro && <AnalisisRevisado graficaId={indicadorOcupHotelQro.id} entidadClave={entidadOcupHotelQro} />}
        </>
      ) : selectedIndicador === 'crecimiento_habitaciones_ocupadas_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Crecimiento de habitaciones ocupadas
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-crecimiento-habitaciones-ocupadas`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {crecHabOcupQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={crecHabOcupQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[-maxAbsCrecHabOcupQro, maxAbsCrecHabOcupQro]} tickFormatter={(v) => `${Number(v).toFixed(1)}%`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [`${Number(value).toFixed(1)}%`, 'Crecimiento']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="crecimiento" stroke={palette[3] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Crecimiento de habitaciones ocupadas (%)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de crecimiento de habitaciones ocupadas para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Datatur / Sectur — variación anual de habitaciones ocupadas en Querétaro.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Crecimiento (%)</th>
                </tr>
              </thead>
              <tbody>
                {crecHabOcupQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.crecimiento.toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorCrecHabOcupQro && (
            <AnalisisIA
              graficaId={indicadorCrecHabOcupQro.id}
              entidadClave={entidadCrecHabOcupQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Crecimiento de habitaciones ocupadas',
                rango: range,
                unidad: 'Porcentaje',
              }}
              datosFiltrados={payloadCrecHabOcupQro}
            />
          )}
          {indicadorCrecHabOcupQro && <AnalisisRevisado graficaId={indicadorCrecHabOcupQro.id} entidadClave={entidadCrecHabOcupQro} />}
        </>
      ) : selectedIndicador === 'pasajeros_anuales_aiq_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Pasajeros anuales AIQ
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-pasajeros-anuales-aiq`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {pasajerosAiqQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={pasajerosAiqQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxPasajerosAiqQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Pasajeros']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="pasajeros" stroke={palette[4] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Pasajeros anuales AIQ" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de pasajeros anuales AIQ para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Aeropuerto Intercontinental de Querétaro (AIQ) — estadísticas anuales de pasajeros.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Pasajeros</th>
                </tr>
              </thead>
              <tbody>
                {pasajerosAiqQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.pasajeros)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorPasajerosAiqQro && (
            <AnalisisIA
              graficaId={indicadorPasajerosAiqQro.id}
              entidadClave={entidadPasajerosAiqQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Pasajeros anuales AIQ',
                rango: range,
                unidad: 'Pasajeros',
              }}
              datosFiltrados={payloadPasajerosAiqQro}
            />
          )}
          {indicadorPasajerosAiqQro && <AnalisisRevisado graficaId={indicadorPasajerosAiqQro.id} entidadClave={entidadPasajerosAiqQro} />}
        </>
      ) : selectedIndicador === 'crecimiento_anual_pasajeros_aiq_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Crecimiento anual de pasajeros
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-crecimiento-anual-pasajeros-aiq`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {crecPasajerosAiqQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={crecPasajerosAiqQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[-maxAbsCrecPasajerosAiqQro, maxAbsCrecPasajerosAiqQro]} tickFormatter={(v) => `${Number(v).toFixed(1)}%`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <ReferenceLine y={0} stroke={palette[7] ?? palette[0]} strokeDasharray="4 4" />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [`${Number(value).toFixed(1)}%`, 'Crecimiento']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="crecimiento" stroke={palette[5] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Crecimiento anual de pasajeros (%)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de crecimiento anual de pasajeros para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Aeropuerto Intercontinental de Querétaro (AIQ) — variación anual de pasajeros.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Crecimiento (%)</th>
                </tr>
              </thead>
              <tbody>
                {crecPasajerosAiqQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.crecimiento.toFixed(2)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorCrecPasajerosAiqQro && (
            <AnalisisIA
              graficaId={indicadorCrecPasajerosAiqQro.id}
              entidadClave={entidadCrecPasajerosAiqQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Crecimiento anual de pasajeros',
                rango: range,
                unidad: 'Porcentaje',
              }}
              datosFiltrados={payloadCrecPasajerosAiqQro}
            />
          )}
          {indicadorCrecPasajerosAiqQro && <AnalisisRevisado graficaId={indicadorCrecPasajerosAiqQro.id} entidadClave={entidadCrecPasajerosAiqQro} />}
        </>
      ) : selectedIndicador === 'vuelos_nacionales_internacionales_aiq_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              % vuelos nacionales / internacionales
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-vuelos-nacionales-internacionales-aiq`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {vuelosNacIntAiqQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={vuelosNacIntAiqQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, 100]} tickFormatter={(v) => `${Number(v).toFixed(0)}%`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number, name: string) => [`${Number(value).toFixed(1)}%`, name]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="nacionales" stroke={palette[0] ?? palette[1]} strokeWidth={2} dot={{ r: 3 }} name="Vuelos nacionales (%)" />
                  <Line type="monotone" dataKey="internacionales" stroke={palette[1] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Vuelos internacionales (%)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de % vuelos nacionales / internacionales para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Aeropuerto Intercontinental de Querétaro (AIQ) — participación anual de vuelos nacionales e internacionales.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Nacionales (%)</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Internacionales (%)</th>
                </tr>
              </thead>
              <tbody>
                {vuelosNacIntAiqQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.nacionales.toFixed(2)}%</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.internacionales.toFixed(2)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorVuelosNacIntAiqQro && (
            <AnalisisIA
              graficaId={indicadorVuelosNacIntAiqQro.id}
              entidadClave={entidadVuelosNacIntAiqQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: '% vuelos nacionales / internacionales',
                rango: range,
                unidad: 'Porcentaje',
              }}
              datosFiltrados={payloadVuelosNacIntAiqQro}
            />
          )}
          {indicadorVuelosNacIntAiqQro && <AnalisisRevisado graficaId={indicadorVuelosNacIntAiqQro.id} entidadClave={entidadVuelosNacIntAiqQro} />}
        </>
      ) : selectedIndicador === 'historico_toneladas_transportadas_aiq_queretaro' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Histórico de toneladas transportadas
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-historico-toneladas-transportadas-aiq`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {toneladasAiqQroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={toneladasAiqQroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxToneladasAiqQro]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Toneladas']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="toneladas" stroke={palette[6] ?? palette[0]} strokeWidth={2} dot={{ r: 3 }} name="Toneladas transportadas" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de histórico de toneladas transportadas para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Aeropuerto Intercontinental de Querétaro (AIQ) — toneladas transportadas por año.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Toneladas</th>
                </tr>
              </thead>
              <tbody>
                {toneladasAiqQroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.toneladas)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorToneladasAiqQro && (
            <AnalisisIA
              graficaId={indicadorToneladasAiqQro.id}
              entidadClave={entidadToneladasAiqQro}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Histórico de toneladas transportadas',
                rango: range,
                unidad: 'Toneladas',
              }}
              datosFiltrados={payloadToneladasAiqQro}
            />
          )}
          {indicadorToneladasAiqQro && <AnalisisRevisado graficaId={indicadorToneladasAiqQro.id} entidadClave={entidadToneladasAiqQro} />}
        </>
      ) : selectedIndicador === 'comercio_internacional' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Comercio internacional
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-comercio-internacional`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {comercioData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <BarChart data={comercioData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxComercio]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number, name: string) => [fmt(Number(value)), name]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="importaciones" fill={palette[0] ?? palette[1]} name="Importaciones (MUSD)" />
                  <Bar dataKey="exportaciones" fill={palette[1] ?? palette[0]} name="Exportaciones (MUSD)" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de comercio internacional para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: DataMéxico — Perfil geo de Mérida (importaciones y exportaciones anuales).
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Importaciones (MUSD)</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Exportaciones (MUSD)</th>
                </tr>
              </thead>
              <tbody>
                {comercioData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.importaciones)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.exportaciones)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorComercio && (
            <AnalisisIA
              graficaId={indicadorComercio.id}
              entidadClave={entidadComercio}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Comercio internacional',
                rango: range,
              }}
              datosFiltrados={payloadComercio}
            />
          )}
          {indicadorComercio && (
            <AnalisisRevisado graficaId={indicadorComercio.id} entidadClave={entidadComercio} />
          )}
        </>
      ) : selectedIndicador === 'comercio_internacional_mty' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Comercio internacional (Monterrey)
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-comercio-internacional`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {comercioMtyData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <BarChart data={comercioMtyData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxComercioMty]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Flujo (MUSD)']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="valor" fill={palette[0] ?? palette[1]} name="Flujo internacional (MUSD)" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de comercio internacional para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: datos.gob.mx — Inversión Extranjera Directa (proxy para Monterrey / Nuevo León).
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Flujo (MUSD)</th>
                </tr>
              </thead>
              <tbody>
                {comercioMtyData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.valor)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorComercioMty && (
            <AnalisisIA
              graficaId={indicadorComercioMty.id}
              entidadClave={entidadComercioMty}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Comercio internacional',
                rango: range,
              }}
              datosFiltrados={payloadComercioMty}
            />
          )}
          {indicadorComercioMty && (
            <AnalisisRevisado graficaId={indicadorComercioMty.id} entidadClave={entidadComercioMty} />
          )}
        </>
      ) : selectedIndicador === 'export_import_mty' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Exportaciones e importaciones (Monterrey)
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-exportaciones-importaciones`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {expImpMtyData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <BarChart data={expImpMtyData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxExpImpMty]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number, name: string) => [fmt(Number(value)), name]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="exportaciones" fill={palette[0] ?? palette[1]} name="Exportaciones (MUSD)" />
                  <Bar dataKey="importaciones" fill={palette[1] ?? palette[0]} name="Importaciones (MUSD)" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de exportaciones e importaciones para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: datos.gob.mx — Serie anual referencial para Monterrey / Nuevo León.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Exportaciones (MUSD)</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Importaciones (MUSD)</th>
                </tr>
              </thead>
              <tbody>
                {expImpMtyData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.exportaciones)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.importaciones)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorExpImpMty && (
            <AnalisisIA
              graficaId={indicadorExpImpMty.id}
              entidadClave={entidadExpImpMty}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Exportaciones e importaciones',
                rango: range,
              }}
              datosFiltrados={payloadExpImpMty}
            />
          )}
          {indicadorExpImpMty && (
            <AnalisisRevisado graficaId={indicadorExpImpMty.id} entidadClave={entidadExpImpMty} />
          )}
        </>
      ) : selectedIndicador === 'llegadas_pasajeros_mty' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Llegadas de pasajeros (Monterrey)
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-llegadas-pasajeros`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {llegadasPasajerosMtyData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <BarChart data={llegadasPasajerosMtyData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxLlegadasPasajerosMty]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number, name: string) => [fmt(Number(value)), name]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="nacionales" fill={palette[0] ?? palette[1]} name="Nacionales" />
                  <Bar dataKey="internacionales" fill={palette[1] ?? palette[0]} name="Internacionales" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de llegadas de pasajeros para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: OMA/AFAC — Llegadas anuales de pasajeros en Monterrey.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Nacionales</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Internacionales</th>
                </tr>
              </thead>
              <tbody>
                {llegadasPasajerosMtyData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.nacionales)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.internacionales)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorLlegadasPasajerosMty && (
            <AnalisisIA
              graficaId={indicadorLlegadasPasajerosMty.id}
              entidadClave={entidadLlegadasPasajerosMty}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Llegadas de pasajeros',
                rango: range,
              }}
              datosFiltrados={payloadLlegadasPasajerosMty}
            />
          )}
          {indicadorLlegadasPasajerosMty && (
            <AnalisisRevisado
              graficaId={indicadorLlegadasPasajerosMty.id}
              entidadClave={entidadLlegadasPasajerosMty}
            />
          )}
        </>
      ) : selectedIndicador === 'visitantes_nac_ext_mty' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Visitantes nacionales y extranjeros (Monterrey)
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-visitantes-nac-ext`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {visitantesNacExtMtyData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <BarChart data={visitantesNacExtMtyData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxVisitantesMty]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number, name: string) => [fmt(Number(value)), name]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="nacionales" fill={palette[0] ?? palette[1]} name="Nacionales" />
                  <Bar dataKey="extranjeros" fill={palette[1] ?? palette[0]} name="Extranjeros" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de visitantes nacionales y extranjeros para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: SECTUR/Datatur — Visitantes nacionales y extranjeros en Monterrey.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Nacionales</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Extranjeros</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>% Nacionales</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>% Extranjeros</th>
                </tr>
              </thead>
              <tbody>
                {visitantesNacExtMtyData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.nacionales)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.extranjeros)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.pctNacionales.toFixed(1)}%</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.pctExtranjeros.toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorVisitantesMty && (
            <AnalisisIA
              graficaId={indicadorVisitantesMty.id}
              entidadClave={entidadVisitantesMty}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Visitantes nacionales y extranjeros',
                rango: range,
              }}
              datosFiltrados={payloadVisitantesMty}
            />
          )}
          {indicadorVisitantesMty && (
            <AnalisisRevisado graficaId={indicadorVisitantesMty.id} entidadClave={entidadVisitantesMty} />
          )}
        </>
      ) : selectedIndicador === 'ocupacion_hotelera_mty' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Ocupación hotelera (Monterrey)
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-ocupacion-hotelera`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {ocupacionHoteleraMtyData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={ocupacionHoteleraMtyData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, 100]} tickFormatter={(v) => `${Number(v)}%`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [`${Number(value).toFixed(1)}%`, 'Ocupación']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="ocupacionPct" stroke={palette[0] ?? palette[1]} strokeWidth={2} dot={{ r: 3 }} name="% Ocupación hotelera" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de ocupación hotelera para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: SECTUR/Datatur — Ocupación hotelera y cuartos en Monterrey.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Ocupación (%)</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Cuartos disponibles</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Cuartos ocupados</th>
                </tr>
              </thead>
              <tbody>
                {ocupacionHoteleraMtyData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.ocupacionPct.toFixed(1)}%</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.cuartosDisponibles)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.cuartosOcupados)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorOcupacionHoteleraMty && (
            <AnalisisIA
              graficaId={indicadorOcupacionHoteleraMty.id}
              entidadClave={entidadOcupacionHoteleraMty}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Ocupación hotelera',
                rango: range,
              }}
              datosFiltrados={payloadOcupacionHoteleraMty}
            />
          )}
          {indicadorOcupacionHoteleraMty && (
            <AnalisisRevisado
              graficaId={indicadorOcupacionHoteleraMty.id}
              entidadClave={entidadOcupacionHoteleraMty}
            />
          )}
        </>
      ) : selectedIndicador === 'conectividad_aerea' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Conectividad aérea
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-conectividad-aerea`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {conectividadData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={conectividadData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxConectividad]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Operaciones']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="valor" stroke={palette[0] ?? palette[1]} strokeWidth={2} dot={{ r: 3 }} name="Operaciones aeroportuarias" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de conectividad aérea para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: AFAC — Estadísticas de operaciones aeroportuarias (Mérida).
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Operaciones</th>
                </tr>
              </thead>
              <tbody>
                {conectividadData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.valor)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorConectividad && (
            <AnalisisIA
              graficaId={indicadorConectividad.id}
              entidadClave={entidadConectividad}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Conectividad aérea',
                rango: range,
              }}
              datosFiltrados={payloadConectividad}
            />
          )}
          {indicadorConectividad && (
            <AnalisisRevisado graficaId={indicadorConectividad.id} entidadClave={entidadConectividad} />
          )}
        </>
      ) : selectedIndicador === 'conexiones_aereas' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Conexiones aéreas (llegadas a Mérida)
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-conexiones-aereas`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {conexionesData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={360}>
                <BarChart data={conexionesData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="origen" interval={0} angle={-25} textAnchor="end" height={80} tick={{ fontSize: Math.max(10, xAxisSize - 1), fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxConexiones]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Vuelos']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="vuelos" fill={palette[0] ?? palette[1]} name="Vuelos de llegada" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de conexiones aéreas para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: datos.gob.mx / AFAC — Llegadas a Mérida por aeropuerto de origen.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Aeropuerto de origen</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Vuelos</th>
                </tr>
              </thead>
              <tbody>
                {conexionesData.map((row) => (
                  <tr key={row.origen} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.origen}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.vuelos)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorConexiones && (
            <AnalisisIA
              graficaId={indicadorConexiones.id}
              entidadClave={entidadConexiones}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Conexiones aéreas',
                rango: range,
              }}
              datosFiltrados={payloadConexiones}
            />
          )}
          {indicadorConexiones && (
            <AnalisisRevisado graficaId={indicadorConexiones.id} entidadClave={entidadConexiones} />
          )}
        </>
      ) : selectedIndicador === 'oferta_servicios_turisticos' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Oferta de servicios turísticos
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-oferta-servicios-turisticos`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {ofertaData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <BarChart data={ofertaData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxOferta]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number, name: string) => [fmt(Number(value)), name]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="servicios" fill={palette[0] ?? palette[1]} name="Servicios otorgados" />
                  <Bar dataKey="ventas" fill={palette[1] ?? palette[0]} name="Ventas (MDP)" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de oferta de servicios turísticos para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: datos.gob.mx — Servicios turísticos otorgados y ventas obtenidas (Mérida).
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Servicios otorgados</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Ventas (MDP)</th>
                </tr>
              </thead>
              <tbody>
                {ofertaData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.servicios)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.ventas)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorOfertaTuristica && (
            <AnalisisIA
              graficaId={indicadorOfertaTuristica.id}
              entidadClave={entidadOferta}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Oferta de servicios turísticos',
                rango: range,
              }}
              datosFiltrados={payloadOferta}
            />
          )}
          {indicadorOfertaTuristica && (
            <AnalisisRevisado graficaId={indicadorOfertaTuristica.id} entidadClave={entidadOferta} />
          )}
        </>
      ) : selectedIndicador === 'ocupacion_hotelera_promedio' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Ocupación hotelera promedio
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-ocupacion-hotelera-promedio`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {ocupacionHoteleraData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={ocupacionHoteleraData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, 100]} tickFormatter={(v) => `${Number(v)}%`} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [`${Number(value).toFixed(1)}%`, 'Ocupación promedio']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="porcentaje" stroke={palette[0] ?? palette[1]} strokeWidth={2} dot={{ r: 3 }} name="% de ocupación" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de ocupación hotelera para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Observatur Yucatán — Porcentaje promedio de habitaciones ocupadas en Mérida.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Ocupación promedio (%)</th>
                </tr>
              </thead>
              <tbody>
                {ocupacionHoteleraData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{row.porcentaje.toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorOcupacionHotelera && (
            <AnalisisIA
              graficaId={indicadorOcupacionHotelera.id}
              entidadClave={entidadOcupacionHotelera}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Ocupación hotelera promedio',
                rango: range,
              }}
              datosFiltrados={payloadOcupacionHotelera}
            />
          )}
          {indicadorOcupacionHotelera && (
            <AnalisisRevisado graficaId={indicadorOcupacionHotelera.id} entidadClave={entidadOcupacionHotelera} />
          )}
        </>
      ) : selectedIndicador === 'llegada_pernocta' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Llegada de visitantes con pernocta
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-llegada-pernocta`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {llegadaPernoctaData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <BarChart data={llegadaPernoctaData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxLlegadaPernocta]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number, name: string) => [fmt(Number(value)), name]} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="nacionales" fill={palette[0] ?? palette[1]} name="Nacionales" />
                  <Bar dataKey="internacionales" fill={palette[1] ?? palette[0]} name="Internacionales" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de llegada de visitantes con pernocta para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Observatur Yucatán — Visitantes con pernocta por procedencia en Mérida.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Nacionales</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Internacionales</th>
                </tr>
              </thead>
              <tbody>
                {llegadaPernoctaData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.nacionales)}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.internacionales)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorLlegadaPernocta && (
            <AnalisisIA
              graficaId={indicadorLlegadaPernocta.id}
              entidadClave={entidadLlegadaPernocta}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Llegada de visitantes con pernocta',
                rango: range,
              }}
              datosFiltrados={payloadLlegadaPernocta}
            />
          )}
          {indicadorLlegadaPernocta && (
            <AnalisisRevisado graficaId={indicadorLlegadaPernocta.id} entidadClave={entidadLlegadaPernocta} />
          )}
        </>
      ) : selectedIndicador === 'gasto_promedio_diario' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Gasto promedio diario del visitante con pernocta
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-gasto-promedio-diario`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {gastoPromedioData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={gastoPromedioData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxGastoPromedio]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [`$${fmt(Number(value))} MXN`, 'Gasto promedio diario']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="valor" stroke={palette[0] ?? palette[1]} strokeWidth={2} dot={{ r: 3 }} name="MXN por visitante/día" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de gasto promedio diario para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Observatur Yucatán — Gasto promedio diario de visitantes con pernocta en Mérida.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Gasto promedio diario (MXN)</th>
                </tr>
              </thead>
              <tbody>
                {gastoPromedioData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>${fmt(row.valor)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorGastoPromedio && (
            <AnalisisIA
              graficaId={indicadorGastoPromedio.id}
              entidadClave={entidadGastoPromedio}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Gasto promedio diario del visitante con pernocta',
                rango: range,
              }}
              datosFiltrados={payloadGastoPromedio}
            />
          )}
          {indicadorGastoPromedio && (
            <AnalisisRevisado graficaId={indicadorGastoPromedio.id} entidadClave={entidadGastoPromedio} />
          )}
        </>
      ) : selectedIndicador === 'derrama_economica' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Derrama económica estimada (visitantes con pernocta)
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-derrama-economica`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {derramaData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <BarChart data={derramaData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxDerrama]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [`$${fmt(Number(value))} MDP`, 'Derrama económica']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="valor" fill={palette[0] ?? palette[1]} name="Derrama (Millones MXN)" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de derrama económica para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Observatur Yucatán — Derrama económica estimada de visitantes con pernocta en Mérida.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Derrama económica (Millones MXN)</th>
                </tr>
              </thead>
              <tbody>
                {derramaData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>${fmt(row.valor)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorDerrama && (
            <AnalisisIA
              graficaId={indicadorDerrama.id}
              entidadClave={entidadDerrama}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Derrama económica estimada de visitantes con pernocta',
                rango: range,
              }}
              datosFiltrados={payloadDerrama}
            />
          )}
          {indicadorDerrama && (
            <AnalisisRevisado graficaId={indicadorDerrama.id} entidadClave={entidadDerrama} />
          )}
        </>
      ) : selectedIndicador === 'ingreso_hotelero' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Ingreso hotelero
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-ingreso-hotelero`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {ingresoHoteleroData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={ingresoHoteleroData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxIngresoHotelero]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [`$${fmt(Number(value))} MDP`, 'Ingreso hotelero']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="valor" stroke={palette[0] ?? palette[1]} strokeWidth={2} dot={{ r: 3 }} name="Ingreso hotelero (Millones MXN)" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de ingreso hotelero para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Observatur Yucatán — Ingreso hotelero anual estimado en Mérida.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Ingreso hotelero (Millones MXN)</th>
                </tr>
              </thead>
              <tbody>
                {ingresoHoteleroData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>${fmt(row.valor)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorIngresoHotelero && (
            <AnalisisIA
              graficaId={indicadorIngresoHotelero.id}
              entidadClave={entidadIngresoHotelero}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Ingreso hotelero',
                rango: range,
              }}
              datosFiltrados={payloadIngresoHotelero}
            />
          )}
          {indicadorIngresoHotelero && (
            <AnalisisRevisado graficaId={indicadorIngresoHotelero.id} entidadClave={entidadIngresoHotelero} />
          )}
        </>
      ) : selectedIndicador === 'establecimientos_turisticos' ? (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Establecimientos de servicios turísticos (Yucatán)
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-establecimientos-turisticos`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {establecimientosData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <BarChart data={establecimientosData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxEstablecimientos]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Establecimientos']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Bar dataKey="valor" fill={palette[0] ?? palette[1]} name="Establecimientos" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de establecimientos turísticos para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Observatur Yucatán — Establecimientos de servicios turísticos en el estado.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Establecimientos</th>
                </tr>
              </thead>
              <tbody>
                {establecimientosData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.valor)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorEstablecimientos && (
            <AnalisisIA
              graficaId={indicadorEstablecimientos.id}
              entidadClave={entidadEstablecimientos}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Establecimientos de servicios turísticos en el estado',
                rango: range,
              }}
              datosFiltrados={payloadEstablecimientos}
            />
          )}
          {indicadorEstablecimientos && (
            <AnalisisRevisado graficaId={indicadorEstablecimientos.id} entidadClave={entidadEstablecimientos} />
          )}
        </>
      ) : (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>
            <p style={{ fontSize: `${titleSize}px`, fontFamily, color: '#e2e8f0', margin: 0, fontWeight: 700, textAlign: 'center' }}>
              Población ocupada en restaurantes y hoteles
            </p>
            <button
              onClick={() => downloadChartAsPng(chartRef, `ciudad-${selectedCity.id}-ocupacion-rest-hot`)}
              title="Descargar gráfica en alta resolución"
              style={{ position: 'absolute', right: 0, background: 'transparent', border: '1px solid #2d3148', borderRadius: '4px', color: '#64748b', fontSize: '11px', fontFamily, padding: '4px 10px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#0576F3'; e.currentTarget.style.color = '#0576F3' }}
              onMouseLeave={(e) => { e.currentTarget.style.borderColor = '#2d3148'; e.currentTarget.style.color = '#64748b' }}
            >
              ↓ PNG
            </button>
          </div>
          {ocupacionData.length ? (
            <div ref={chartRef}>
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={ocupacionData} margin={{ top: 24, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3148" />
                  <XAxis dataKey="anio" tick={{ fontSize: xAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <YAxis domain={[0, yMaxOcupacion]} tickFormatter={(v) => fmt(Number(v))} tick={{ fontSize: yAxisSize, fontFamily, fill: '#94a3b8' }} axisLine={{ stroke: '#2d3148' }} tickLine={false} />
                  <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2d3148', borderRadius: '6px', fontFamily, fontSize: '12px' }} formatter={(value: number) => [fmt(Number(value)), 'Personas ocupadas']} labelStyle={{ color: '#e2e8f0' }} itemStyle={{ color: '#94a3b8' }} />
                  <Legend verticalAlign="bottom" align="center" wrapperStyle={{ fontSize: `${xAxisSize}px`, fontFamily, color: '#94a3b8', paddingTop: '8px' }} />
                  <Line type="monotone" dataKey="valor" stroke={palette[0] ?? palette[1]} strokeWidth={2} dot={{ r: 3 }} name="Personas ocupadas" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={{ border: '1px solid #2d3148', borderRadius: '8px', padding: '24px', textAlign: 'center', color: '#94a3b8', fontSize: '12px', fontFamily }}>
              Sin datos de ocupación para el rango seleccionado.
            </div>
          )}
          <p style={{ fontSize: '11px', color: '#4a5568', fontFamily, margin: '-8px 0 0 0', textAlign: 'left' }}>
            Fuente: Observatur Yucatán / ENOE-INEGI — Población ocupada en servicios de alojamiento y restaurantes.
          </p>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', fontFamily, color: '#94a3b8' }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #2d3148' }}>
                  <th style={{ textAlign: 'left', padding: '6px 8px', color: '#64748b' }}>Año</th>
                  <th style={{ textAlign: 'right', padding: '6px 8px', color: '#64748b' }}>Personas ocupadas</th>
                </tr>
              </thead>
              <tbody>
                {ocupacionData.map((row) => (
                  <tr key={row.anio} style={{ borderBottom: '1px solid #1e2235' }}>
                    <td style={{ padding: '5px 8px' }}>{row.anio}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right' }}>{fmt(row.valor)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {indicadorOcupacion && (
            <AnalisisIA
              graficaId={indicadorOcupacion.id}
              entidadClave={entidadOcupacion}
              contexto={{
                ciudad: selectedCity.label,
                indicador: 'Población ocupada en restaurantes y hoteles',
                rango: range,
              }}
              datosFiltrados={payloadOcupacion}
            />
          )}
          {indicadorOcupacion && (
            <AnalisisRevisado graficaId={indicadorOcupacion.id} entidadClave={entidadOcupacion} />
          )}
        </>
      )}
    </div>
  )
}
