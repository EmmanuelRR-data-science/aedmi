from scheduler import registrar_modulo, run_all_modules

MODULES = [
    ("sources.banxico.tipo_cambio", "TipoCambioExtractor", 2),
    ("sources.banxico.inflacion", "InflacionExtractor", 3),
    ("sources.inegi.pib_trimestral", "PIBTrimestralINEGIExtractor", 1),
    ("sources.inegi.poblacion_nacional", "PoblacionNacionalExtractor", 4),
    ("sources.inegi.grupos_edad_nacional", "GruposEdadNacionalExtractor", 5),
    ("sources.inegi.poblacion_sexo_nacional", "PoblacionSexoNacionalExtractor", 6),
    ("sources.inegi.pea_nacional", "PEANacionalExtractor", 7),
    ("sources.inegi.ocupacion_sector_nacional", "OcupacionSectorNacionalExtractor", 8),
    ("sources.worldbank.pib_anual", "PIBAnualWorldBankExtractor", 9),
    ("sources.imf.pib_proyeccion", "PIBProyeccionIMFExtractor", 10),
    ("sources.se.ied_sector", "IEDSectorExtractor", 11),
    ("sources.se.ied_pais", "IEDPaisExtractor", 12),
    ("sources.se.anuncios_inversion", "AnunciosInversionExtractor", 17),
    ("sources.se.anuncios_base", "AnunciosBaseExtractor", 18),
    ("sources.inegi.pib_sector", "PIBSectorExtractor", 19),
    ("sources.inegi.balanza_comercial", "BalanzaComercialExtractor", 20),
    ("sources.se.ied_estados", "IEDEstadosExtractor", 23),
    ("sources.inegi.estados_info", "EstadosInfoExtractor", 24),
    ("sources.inegi.demografia_estatal", "DemografiaEstatalExtractor", 25),
    ("sources.conapo.proyecciones_estatal", "ProyeccionesEstatalExtractor", 26),
    ("sources.inegi.itaee_estatal", "ITAEEEstatalExtractor", 27),
    ("sources.se.anuncios_estatal", "AnunciosEstatalExtractor", 28),
    ("sources.sectur.hotelera_estatal", "HoteleraEstatalExtractor", 29),
    ("sources.sectur.llegada_turistas_estatal", "LlegadaTuristasEstatalExtractor", 30),
    ("sources.se.exportaciones_estatal", "ExportacionesEstatalExtractor", 31),
    ("sources.afac.aeropuertos_estatal", "AeropuertosEstatalExtractor", 32),
    ("sources.worldbank.turismo_ingresos", "TurismoIngresosExtractor", 13),
    ("sources.worldbank.turismo_ranking", "TurismoRankingExtractor", 14),
    ("sources.inegi.balanza_visitantes", "BalanzaVisitantesExtractor", 15),
    ("sources.inegi.actividad_hotelera", "ActividadHoteleraExtractor", 22),
    ("sources.afac.mercado_aereo", "MercadoAereoExtractor", 16),
    ("sources.afac.operaciones_aeroportuarias", "OperacionesAeroportuariasExtractor", 21),
    ("sources.conapo.municipios_poblacion", "MunicipiosPoblacionExtractor", 33),
    ("sources.conapo.municipios_piramide_edad", "MunicipiosPiramideEdadExtractor", 34),
    ("sources.conapo.municipios_proyeccion", "MunicipiosProyeccionExtractor", 35),
    ("sources.conapo.localidades_poblacion", "LocalidadesPoblacionExtractor", 36),
    ("sources.conapo.localidades_piramide_edad", "LocalidadesPiramideEdadExtractor", 37),
    (
        "sources.inegi.ocupacion_restaurantes_hoteles_yucatan",
        "OcupacionRestaurantesHotelesYucatanExtractor",
        38,
    ),
    (
        "sources.se.comercio_internacional_merida",
        "ComercioInternacionalMeridaExtractor",
        39,
    ),
    (
        "sources.afac.conectividad_aerea_merida",
        "ConectividadAereaMeridaExtractor",
        40,
    ),
    (
        "sources.afac.conexiones_aereas_merida",
        "ConexionesAereasMeridaExtractor",
        41,
    ),
    (
        "sources.sectur.oferta_servicios_turisticos_merida",
        "OfertaServiciosTuristicosMeridaExtractor",
        42,
    ),
    (
        "sources.sectur.ocupacion_hotelera_promedio_merida",
        "OcupacionHoteleraPromedioMeridaExtractor",
        43,
    ),
    (
        "sources.sectur.llegada_visitantes_pernocta_merida",
        "LlegadaVisitantesPernoctaMeridaExtractor",
        44,
    ),
    (
        "sources.sectur.gasto_promedio_diario_pernocta_merida",
        "GastoPromedioDiarioPernoctaMeridaExtractor",
        45,
    ),
    (
        "sources.sectur.derrama_economica_pernocta_merida",
        "DerramaEconomicaPernoctaMeridaExtractor",
        46,
    ),
    (
        "sources.sectur.ingreso_hotelero_merida",
        "IngresoHoteleroMeridaExtractor",
        47,
    ),
    (
        "sources.sectur.establecimientos_servicios_turisticos_yucatan",
        "EstablecimientosServiciosTuristicosYucatanExtractor",
        48,
    ),
    (
        "sources.se.comercio_internacional_monterrey",
        "ComercioInternacionalMonterreyExtractor",
        49,
    ),
    (
        "sources.se.exportaciones_importaciones_monterrey",
        "ExportacionesImportacionesMonterreyExtractor",
        50,
    ),
    (
        "sources.afac.llegadas_pasajeros_monterrey",
        "LlegadasPasajerosMonterreyExtractor",
        51,
    ),
    (
        "sources.sectur.visitantes_nacionales_extranjeros_monterrey",
        "VisitantesNacionalesExtranjerosMonterreyExtractor",
        52,
    ),
    (
        "sources.sectur.ocupacion_hotelera_monterrey",
        "OcupacionHoteleraMonterreyExtractor",
        53,
    ),
    (
        "sources.inegi.pea_queretaro",
        "PeaQueretaroExtractor",
        54,
    ),
    (
        "sources.inegi.tasa_participacion_laboral_queretaro",
        "TasaParticipacionLaboralQueretaroExtractor",
        55,
    ),
    (
        "sources.inegi.tasa_desocupacion_queretaro",
        "TasaDesocupacionQueretaroExtractor",
        56,
    ),
    (
        "sources.inegi.composicion_sectorial_empleo_queretaro",
        "ComposicionSectorialEmpleoQueretaroExtractor",
        57,
    ),
    (
        "sources.inegi.pib_estatal_total_queretaro",
        "PibEstatalTotalQueretaroExtractor",
        58,
    ),
    (
        "sources.inegi.pib_per_capita_queretaro",
        "PibPerCapitaQueretaroExtractor",
        59,
    ),
    (
        "sources.inegi.pib_sector_queretaro",
        "PibSectorQueretaroExtractor",
        60,
    ),
    (
        "sources.inegi.tasa_crecimiento_economico_queretaro",
        "TasaCrecimientoEconomicoQueretaroExtractor",
        61,
    ),
    (
        "sources.se.exportaciones_estatales_queretaro",
        "ExportacionesEstatalesQueretaroExtractor",
        62,
    ),
    (
        "sources.se.importaciones_estatales_queretaro",
        "ImportacionesEstatalesQueretaroExtractor",
        63,
    ),
    (
        "sources.se.comercio_internacional_neto_queretaro",
        "ComercioInternacionalNetoQueretaroExtractor",
        64,
    ),
    (
        "sources.se.balance_comercial_neto_queretaro",
        "BalanceComercialNetoQueretaroExtractor",
        65,
    ),
    (
        "sources.se.ied_anual_queretaro",
        "IEDAnualQueretaroExtractor",
        66,
    ),
    (
        "sources.se.ied_municipio_queretaro",
        "IEDMunicipioQueretaroExtractor",
        67,
    ),
    (
        "sources.inegi.llegada_total_turistas_queretaro",
        "LlegadaTotalTuristasQueretaroExtractor",
        68,
    ),
    (
        "sources.sectur.turismo_nacional_vs_internacional_queretaro",
        "TurismoNacionalVsInternacionalQueretaroExtractor",
        69,
    ),
    (
        "sources.sectur.derrama_economica_turistica_queretaro",
        "DerramaEconomicaTuristicaQueretaroExtractor",
        70,
    ),
    (
        "sources.sectur.ocupacion_hotelera_queretaro",
        "OcupacionHoteleraQueretaroExtractor",
        71,
    ),
    (
        "sources.sectur.crecimiento_habitaciones_ocupadas_queretaro",
        "CrecimientoHabitacionesOcupadasQueretaroExtractor",
        72,
    ),
    (
        "sources.afac.pasajeros_anuales_aiq_queretaro",
        "PasajerosAnualesAIQQueretaroExtractor",
        73,
    ),
    (
        "sources.afac.crecimiento_anual_pasajeros_aiq_queretaro",
        "CrecimientoAnualPasajerosAIQQueretaroExtractor",
        74,
    ),
    (
        "sources.afac.vuelos_nacionales_internacionales_aiq_queretaro",
        "VuelosNacionalesInternacionalesAIQQueretaroExtractor",
        75,
    ),
    (
        "sources.afac.historico_toneladas_transportadas_aiq_queretaro",
        "HistoricoToneladasTransportadasAIQQueretaroExtractor",
        76,
    ),
    (
        "sources.sectur.pueblos_magicos_catalogo",
        "PueblosMagicosCatalogoExtractor",
        77,
    ),
]

for module, cls, fuente_id in MODULES:
    registrar_modulo(module, cls, fuente_id)

run_all_modules()
