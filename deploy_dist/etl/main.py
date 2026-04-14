import os
import sys

# Asegurar que el directorio etl/ esté en el path
sys.path.insert(0, os.path.dirname(__file__))

from core.db import connect_with_retry
from core.logger import get_logger
from scheduler import create_scheduler

logger = get_logger("etl.main")


def main() -> None:
    logger.info("Iniciando servicio ETL — AEDMI")

    # Verificar conexión a BD antes de arrancar
    connect_with_retry()

    # Importar módulos ETL registrados
    # KPIs Nacionales — Banxico + INEGI
    from scheduler import registrar_modulo

    registrar_modulo("sources.banxico.tipo_cambio", "TipoCambioExtractor", fuente_id=2)
    registrar_modulo("sources.banxico.inflacion", "InflacionExtractor", fuente_id=3)
    # PIB trimestral ahora viene de INEGI BIE (más actualizado que Banxico SR16734)
    registrar_modulo("sources.inegi.pib_trimestral", "PIBTrimestralINEGIExtractor", fuente_id=1)
    # Demografía nacional
    registrar_modulo("sources.inegi.poblacion_nacional", "PoblacionNacionalExtractor", fuente_id=4)
    registrar_modulo(
        "sources.inegi.grupos_edad_nacional", "GruposEdadNacionalExtractor", fuente_id=5
    )
    registrar_modulo(
        "sources.inegi.poblacion_sexo_nacional", "PoblacionSexoNacionalExtractor", fuente_id=6
    )
    registrar_modulo("sources.inegi.pea_nacional", "PEANacionalExtractor", fuente_id=7)
    registrar_modulo(
        "sources.inegi.ocupacion_sector_nacional", "OcupacionSectorNacionalExtractor", fuente_id=8
    )
    registrar_modulo("sources.worldbank.pib_anual", "PIBAnualWorldBankExtractor", fuente_id=9)
    registrar_modulo("sources.imf.pib_proyeccion", "PIBProyeccionIMFExtractor", fuente_id=10)
    registrar_modulo("sources.se.ied_sector", "IEDSectorExtractor", fuente_id=11)
    registrar_modulo("sources.se.ied_pais", "IEDPaisExtractor", fuente_id=12)
    registrar_modulo("sources.se.anuncios_inversion", "AnunciosInversionExtractor", fuente_id=17)
    registrar_modulo("sources.se.anuncios_base", "AnunciosBaseExtractor", fuente_id=18)
    registrar_modulo("sources.inegi.pib_sector", "PIBSectorExtractor", fuente_id=19)
    registrar_modulo("sources.inegi.balanza_comercial", "BalanzaComercialExtractor", fuente_id=20)
    registrar_modulo("sources.se.ied_estados", "IEDEstadosExtractor", fuente_id=23)
    registrar_modulo("sources.inegi.estados_info", "EstadosInfoExtractor", fuente_id=24)
    registrar_modulo("sources.inegi.demografia_estatal", "DemografiaEstatalExtractor", fuente_id=25)
    registrar_modulo(
        "sources.conapo.proyecciones_estatal", "ProyeccionesEstatalExtractor", fuente_id=26
    )
    registrar_modulo("sources.inegi.itaee_estatal", "ITAEEEstatalExtractor", fuente_id=27)
    registrar_modulo("sources.se.anuncios_estatal", "AnunciosEstatalExtractor", fuente_id=28)
    registrar_modulo("sources.sectur.hotelera_estatal", "HoteleraEstatalExtractor", fuente_id=29)
    registrar_modulo(
        "sources.sectur.llegada_turistas_estatal", "LlegadaTuristasEstatalExtractor", fuente_id=30
    )
    registrar_modulo(
        "sources.se.exportaciones_estatal", "ExportacionesEstatalExtractor", fuente_id=31
    )
    registrar_modulo(
        "sources.afac.aeropuertos_estatal", "AeropuertosEstatalExtractor", fuente_id=32
    )
    registrar_modulo("sources.worldbank.turismo_ingresos", "TurismoIngresosExtractor", fuente_id=13)
    registrar_modulo("sources.worldbank.turismo_ranking", "TurismoRankingExtractor", fuente_id=14)
    registrar_modulo("sources.inegi.balanza_visitantes", "BalanzaVisitantesExtractor", fuente_id=15)
    registrar_modulo("sources.inegi.actividad_hotelera", "ActividadHoteleraExtractor", fuente_id=22)
    registrar_modulo("sources.afac.mercado_aereo", "MercadoAereoExtractor", fuente_id=16)
    registrar_modulo(
        "sources.afac.operaciones_aeroportuarias",
        "OperacionesAeroportuariasExtractor",
        fuente_id=21,
    )
    registrar_modulo(
        "sources.conapo.municipios_poblacion", "MunicipiosPoblacionExtractor", fuente_id=33
    )
    registrar_modulo(
        "sources.conapo.municipios_piramide_edad", "MunicipiosPiramideEdadExtractor", fuente_id=34
    )
    registrar_modulo(
        "sources.conapo.municipios_proyeccion", "MunicipiosProyeccionExtractor", fuente_id=35
    )
    registrar_modulo(
        "sources.conapo.localidades_poblacion", "LocalidadesPoblacionExtractor", fuente_id=36
    )
    registrar_modulo(
        "sources.conapo.localidades_piramide_edad", "LocalidadesPiramideEdadExtractor", fuente_id=37
    )
    registrar_modulo(
        "sources.inegi.ocupacion_restaurantes_hoteles_yucatan",
        "OcupacionRestaurantesHotelesYucatanExtractor",
        fuente_id=38,
    )
    registrar_modulo(
        "sources.se.comercio_internacional_merida",
        "ComercioInternacionalMeridaExtractor",
        fuente_id=39,
    )
    registrar_modulo(
        "sources.afac.conectividad_aerea_merida",
        "ConectividadAereaMeridaExtractor",
        fuente_id=40,
    )
    registrar_modulo(
        "sources.afac.conexiones_aereas_merida",
        "ConexionesAereasMeridaExtractor",
        fuente_id=41,
    )
    registrar_modulo(
        "sources.sectur.oferta_servicios_turisticos_merida",
        "OfertaServiciosTuristicosMeridaExtractor",
        fuente_id=42,
    )
    registrar_modulo(
        "sources.sectur.ocupacion_hotelera_promedio_merida",
        "OcupacionHoteleraPromedioMeridaExtractor",
        fuente_id=43,
    )
    registrar_modulo(
        "sources.sectur.llegada_visitantes_pernocta_merida",
        "LlegadaVisitantesPernoctaMeridaExtractor",
        fuente_id=44,
    )
    registrar_modulo(
        "sources.sectur.gasto_promedio_diario_pernocta_merida",
        "GastoPromedioDiarioPernoctaMeridaExtractor",
        fuente_id=45,
    )
    registrar_modulo(
        "sources.sectur.derrama_economica_pernocta_merida",
        "DerramaEconomicaPernoctaMeridaExtractor",
        fuente_id=46,
    )
    registrar_modulo(
        "sources.sectur.ingreso_hotelero_merida",
        "IngresoHoteleroMeridaExtractor",
        fuente_id=47,
    )
    registrar_modulo(
        "sources.sectur.establecimientos_servicios_turisticos_yucatan",
        "EstablecimientosServiciosTuristicosYucatanExtractor",
        fuente_id=48,
    )
    registrar_modulo(
        "sources.se.comercio_internacional_monterrey",
        "ComercioInternacionalMonterreyExtractor",
        fuente_id=49,
    )
    registrar_modulo(
        "sources.se.exportaciones_importaciones_monterrey",
        "ExportacionesImportacionesMonterreyExtractor",
        fuente_id=50,
    )
    registrar_modulo(
        "sources.afac.llegadas_pasajeros_monterrey",
        "LlegadasPasajerosMonterreyExtractor",
        fuente_id=51,
    )
    registrar_modulo(
        "sources.sectur.visitantes_nacionales_extranjeros_monterrey",
        "VisitantesNacionalesExtranjerosMonterreyExtractor",
        fuente_id=52,
    )
    registrar_modulo(
        "sources.sectur.ocupacion_hotelera_monterrey",
        "OcupacionHoteleraMonterreyExtractor",
        fuente_id=53,
    )
    registrar_modulo(
        "sources.inegi.pea_queretaro",
        "PeaQueretaroExtractor",
        fuente_id=54,
    )
    registrar_modulo(
        "sources.inegi.tasa_participacion_laboral_queretaro",
        "TasaParticipacionLaboralQueretaroExtractor",
        fuente_id=55,
    )
    registrar_modulo(
        "sources.inegi.tasa_desocupacion_queretaro",
        "TasaDesocupacionQueretaroExtractor",
        fuente_id=56,
    )
    registrar_modulo(
        "sources.inegi.composicion_sectorial_empleo_queretaro",
        "ComposicionSectorialEmpleoQueretaroExtractor",
        fuente_id=57,
    )
    registrar_modulo(
        "sources.inegi.pib_estatal_total_queretaro",
        "PibEstatalTotalQueretaroExtractor",
        fuente_id=58,
    )
    registrar_modulo(
        "sources.inegi.pib_per_capita_queretaro",
        "PibPerCapitaQueretaroExtractor",
        fuente_id=59,
    )
    registrar_modulo(
        "sources.inegi.pib_sector_queretaro",
        "PibSectorQueretaroExtractor",
        fuente_id=60,
    )
    registrar_modulo(
        "sources.inegi.tasa_crecimiento_economico_queretaro",
        "TasaCrecimientoEconomicoQueretaroExtractor",
        fuente_id=61,
    )
    registrar_modulo(
        "sources.se.exportaciones_estatales_queretaro",
        "ExportacionesEstatalesQueretaroExtractor",
        fuente_id=62,
    )
    registrar_modulo(
        "sources.se.importaciones_estatales_queretaro",
        "ImportacionesEstatalesQueretaroExtractor",
        fuente_id=63,
    )
    registrar_modulo(
        "sources.se.comercio_internacional_neto_queretaro",
        "ComercioInternacionalNetoQueretaroExtractor",
        fuente_id=64,
    )
    registrar_modulo(
        "sources.se.balance_comercial_neto_queretaro",
        "BalanceComercialNetoQueretaroExtractor",
        fuente_id=65,
    )
    registrar_modulo(
        "sources.se.ied_anual_queretaro",
        "IEDAnualQueretaroExtractor",
        fuente_id=66,
    )
    registrar_modulo(
        "sources.se.ied_municipio_queretaro",
        "IEDMunicipioQueretaroExtractor",
        fuente_id=67,
    )
    registrar_modulo(
        "sources.inegi.llegada_total_turistas_queretaro",
        "LlegadaTotalTuristasQueretaroExtractor",
        fuente_id=68,
    )
    registrar_modulo(
        "sources.sectur.turismo_nacional_vs_internacional_queretaro",
        "TurismoNacionalVsInternacionalQueretaroExtractor",
        fuente_id=69,
    )
    registrar_modulo(
        "sources.sectur.derrama_economica_turistica_queretaro",
        "DerramaEconomicaTuristicaQueretaroExtractor",
        fuente_id=70,
    )
    registrar_modulo(
        "sources.sectur.ocupacion_hotelera_queretaro",
        "OcupacionHoteleraQueretaroExtractor",
        fuente_id=71,
    )
    registrar_modulo(
        "sources.sectur.crecimiento_habitaciones_ocupadas_queretaro",
        "CrecimientoHabitacionesOcupadasQueretaroExtractor",
        fuente_id=72,
    )
    registrar_modulo(
        "sources.afac.pasajeros_anuales_aiq_queretaro",
        "PasajerosAnualesAIQQueretaroExtractor",
        fuente_id=73,
    )
    registrar_modulo(
        "sources.afac.crecimiento_anual_pasajeros_aiq_queretaro",
        "CrecimientoAnualPasajerosAIQQueretaroExtractor",
        fuente_id=74,
    )
    registrar_modulo(
        "sources.afac.vuelos_nacionales_internacionales_aiq_queretaro",
        "VuelosNacionalesInternacionalesAIQQueretaroExtractor",
        fuente_id=75,
    )
    registrar_modulo(
        "sources.afac.historico_toneladas_transportadas_aiq_queretaro",
        "HistoricoToneladasTransportadasAIQQueretaroExtractor",
        fuente_id=76,
    )
    registrar_modulo(
        "sources.sectur.pueblos_magicos_catalogo",
        "PueblosMagicosCatalogoExtractor",
        fuente_id=77,
    )

    scheduler = create_scheduler()

    logger.info("Scheduler iniciado. Esperando ejecución programada...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Servicio ETL detenido.")


if __name__ == "__main__":
    main()
