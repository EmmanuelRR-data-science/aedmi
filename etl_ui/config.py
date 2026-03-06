"""
Configuración de indicadores para ETL UI (sección Nacional).
Cada indicador define: nombre, función de preview (E+T) y función de guardado (L) en PostgreSQL local.
"""
import os

# Ruta por defecto del Excel de aeropuertos (Downloads del usuario)
def _producto_aeropuertos_path():
    p = os.getenv("PRODUCTO_AEROPUERTOS_XLSX", "").strip()
    if p and os.path.isfile(p):
        return p
    downloads = os.path.join(
        os.environ.get("USERPROFILE", ""),
        "Downloads",
        "producto-aeropuertos-2006-2025-nov-29122025.xlsx",
    )
    return downloads if os.path.isfile(downloads) else None


# ----- Preview (solo E+T) -----
def _preview_kpis_nacional():
    from services.data_sources import get_kpis_nacional
    return get_kpis_nacional()


def _preview_crecimiento_poblacional():
    from services.data_sources import _fetch_inegi_crecimiento_poblacional
    return _fetch_inegi_crecimiento_poblacional()


def _preview_estructura_poblacional():
    from services.data_sources import _fetch_inegi_estructura_poblacional
    return _fetch_inegi_estructura_poblacional()


def _preview_distribucion_sexo():
    from services.data_sources import _fetch_inegi_distribucion_sexo
    return _fetch_inegi_distribucion_sexo()


def _preview_pea():
    from services.data_sources import _fetch_inegi_pea
    return _fetch_inegi_pea()


def _preview_pob_sector_actividad():
    from services.data_sources import _fetch_inegi_pob_sector_actividad
    return _fetch_inegi_pob_sector_actividad()


def _preview_inflacion_nacional():
    from services.data_sources import _fetch_inflacion_nacional_banxico
    return _fetch_inflacion_nacional_banxico()


def _preview_tipo_cambio():
    from services.data_sources import _fetch_tipo_cambio_banxico
    diario, mensual = _fetch_tipo_cambio_banxico()
    return {"diario": diario or [], "mensual": mensual or []}


def _preview_proyeccion_pib():
    from services.data_sources import _fetch_proyeccion_pib_fmi
    data, tc_fix, tc_date = _fetch_proyeccion_pib_fmi()
    return {"data": data or [], "tc_fix": tc_fix, "tc_date": tc_date}


def _preview_ied_flujo_entidad():
    from services.data_sources import _fetch_and_process_ied_flujo, _load_ied_flujo_from_csv
    data = _fetch_and_process_ied_flujo()
    return data or _load_ied_flujo_from_csv() or []


def _preview_ied_paises():
    from services.data_sources import _fetch_and_process_ied_paises, _load_ied_paises_from_csv
    data = _fetch_and_process_ied_paises()
    return data or _load_ied_paises_from_csv() or []


def _preview_ied_sectores():
    from services.data_sources import _fetch_and_process_ied_sectores, _load_ied_sectores_from_csv
    data = _fetch_and_process_ied_sectores()
    return data or _load_ied_sectores_from_csv() or []


def _preview_ranking_turismo():
    from services.data_sources import _fetch_and_process_ranking_turismo_wb
    return _fetch_and_process_ranking_turismo_wb() or []


def _preview_balanza_visitantes():
    from services.data_sources import _fetch_and_process_balanza_visitantes
    return _fetch_and_process_balanza_visitantes()


def _preview_balanza_comercial():
    from services.data_sources import fetch_balanza_comercial_producto_from_api
    return fetch_balanza_comercial_producto_from_api()


def _preview_producto_aeropuertos():
    from services.data_sources import load_producto_aeropuertos_from_excel
    path = _producto_aeropuertos_path()
    if not path:
        raise FileNotFoundError(
            "No se encontró el Excel. Defina PRODUCTO_AEROPUERTOS_XLSX o coloque el archivo en Downloads."
        )
    return load_producto_aeropuertos_from_excel(path)


def _preview_participacion_mercado():
    from services.data_sources import _fetch_and_process_participacion_mercado_aereo
    nac, intl = _fetch_and_process_participacion_mercado_aereo()
    return {"nacional": nac or [], "internacional": intl or []}


def _preview_anuncios_combinados():
    from services.data_sources import _fetch_and_process_anuncios_combinados
    return _fetch_and_process_anuncios_combinados()


def _preview_anuncios_base():
    from services.data_sources import _fetch_and_process_anuncios_base
    return _fetch_and_process_anuncios_base()


def _preview_actividad_hotelera_nacional():
    from services.data_sources import fetch_actividad_hotelera_nacional
    por_anio, por_categoria = fetch_actividad_hotelera_nacional()
    return {"por_anio": por_anio, "por_categoria": por_categoria}


# ----- Save -----
def _save_kpis_nacional(data):
    from services.db import save_kpis_nacional_to_db
    return save_kpis_nacional_to_db(data)


def _save_tipo_cambio(data):
    from services.db import save_tipo_cambio_to_db
    return save_tipo_cambio_to_db(data.get("diario", []), data.get("mensual", []))


def _save_proyeccion_pib(data):
    from services.db import save_proyeccion_pib_to_db
    return save_proyeccion_pib_to_db(
        data.get("data", []),
        data.get("tc_fix", 20.0),
        data.get("tc_date", "N/D"),
    )


def _save_participacion_mercado(data):
    from services.db import save_participacion_mercado_to_db, save_participacion_internacional_to_db
    nac = data.get("nacional", [])
    intl = data.get("internacional", [])
    ok1 = save_participacion_mercado_to_db(nac) if nac else True
    ok2 = save_participacion_internacional_to_db(intl) if intl else True
    return ok1 and ok2


def _save_actividad_hotelera_nacional(data):
    from services.db import save_actividad_hotelera_nacional_to_db
    return save_actividad_hotelera_nacional_to_db(
        data.get("por_anio", []),
        data.get("por_categoria", []),
    )


# clave -> { name, description, preview_fn, save_fn, table_name }
# preview_fn retorna list[dict] o dict (ej. {nacional, internacional}, {diario, mensual}, etc.)
# save_fn recibe ese mismo dato y guarda en PostgreSQL
INDICATORS = {
    # KPIs resumen (composite)
    "kpis_nacional": {
        "name": "KPIs Nacional (resumen)",
        "description": "PIB USD, tipo de cambio, inflación, PIB MXN. Composite desde varias fuentes.",
        "preview_fn": _preview_kpis_nacional,
        "save_fn": _save_kpis_nacional,
        "table_name": "kpis_nacional",
    },
    # Demografía
    "crecimiento_poblacional": {
        "name": "Crecimiento poblacional nacional",
        "description": "INEGI. Año y valor (población).",
        "preview_fn": _preview_crecimiento_poblacional,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_crecimiento_poblacional_to_db"]).save_crecimiento_poblacional_to_db(d),
        "table_name": "crecimiento_poblacional_nacional",
    },
    "estructura_poblacion_edad": {
        "name": "Distribución de la población por edad",
        "description": "INEGI. Año, pob_0_14, pob_15_64, pob_65_plus.",
        "preview_fn": _preview_estructura_poblacional,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_estructura_poblacional_to_db"]).save_estructura_poblacional_to_db(d),
        "table_name": "estructura_poblacional_inegi",
    },
    "distribucion_sexo": {
        "name": "Distribución de la población por sexo",
        "description": "INEGI. Año, male, female.",
        "preview_fn": _preview_distribucion_sexo,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_distribucion_sexo_to_db"]).save_distribucion_sexo_to_db(d),
        "table_name": "distribucion_sexo_inegi",
    },
    "poblacion_economica_activa": {
        "name": "Población Económicamente Activa (PEA)",
        "description": "INEGI. Año, trimestre, valor.",
        "preview_fn": _preview_pea,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_pea_to_db"]).save_pea_to_db(d),
        "table_name": "pea_inegi",
    },
    "poblacion_sector_actividad": {
        "name": "Población por sector de actividad económica",
        "description": "INEGI. Sector, valor, pct, es_residual.",
        "preview_fn": _preview_pob_sector_actividad,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_pob_sector_actividad_to_db"]).save_pob_sector_actividad_to_db(d),
        "table_name": "pob_sector_actividad",
    },
    # Economía
    "inflacion_nacional": {
        "name": "Inflación nacional",
        "description": "Banxico INPC. Año, mes, inflación.",
        "preview_fn": _preview_inflacion_nacional,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_inflacion_nacional_to_db"]).save_inflacion_nacional_to_db(d),
        "table_name": "inflacion_nacional",
    },
    "tipo_cambio_mxn_usd": {
        "name": "Tipo de cambio (MXN/USD)",
        "description": "Banxico SF43718. Serie diaria y promedio mensual.",
        "preview_fn": _preview_tipo_cambio,
        "save_fn": _save_tipo_cambio,
        "table_name": "tipo_cambio_banxico_diario + tipo_cambio_banxico_mensual",
    },
    "proyeccion_pib": {
        "name": "Proyección PIB",
        "description": "FMI WEO. Año, PIB MXN/USD total y per cápita.",
        "preview_fn": _preview_proyeccion_pib,
        "save_fn": _save_proyeccion_pib,
        "table_name": "pib_proyeccion_fmi",
    },
    # IED
    "ied_flujo_estado": {
        "name": "IED flujo por entidad",
        "description": "Secretaría de Economía. Entidad, mdd_4t, rank, periodo.",
        "preview_fn": _preview_ied_flujo_entidad,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_ied_flujo_entidad_to_db"]).save_ied_flujo_entidad_to_db(d),
        "table_name": "ied_flujo_entidad",
    },
    "ied_pais_origen": {
        "name": "IED por país de origen",
        "description": "Secretaría de Economía. País, montos por año.",
        "preview_fn": _preview_ied_paises,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_ied_paises_to_db"]).save_ied_paises_to_db(d),
        "table_name": "ied_paises",
    },
    "ied_sector_economico": {
        "name": "IED por sector económico",
        "description": "Secretaría de Economía. Sector, montos.",
        "preview_fn": _preview_ied_sectores,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_ied_sectores_to_db"]).save_ied_sectores_to_db(d),
        "table_name": "ied_sectores",
    },
    # Turismo y comercio
    "ranking_turismo_mundial": {
        "name": "Ranking Turismo Mundial",
        "description": "Banco Mundial WDI. País, año, valor.",
        "preview_fn": _preview_ranking_turismo,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_ranking_turismo_wb_to_db"]).save_ranking_turismo_wb_to_db(d),
        "table_name": "ranking_turismo_wb",
    },
    "balanza_visitantes": {
        "name": "Balanza de Visitantes",
        "description": "INEGI BISE. Año, entradas, salidas, balance.",
        "preview_fn": _preview_balanza_visitantes,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_balanza_visitantes_to_db"]).save_balanza_visitantes_to_db(d),
        "table_name": "balanza_visitantes_inegi",
    },
    "balanza_comercial_producto": {
        "name": "Balanza Comercial por Producto",
        "description": "API Economía (inegi_foreign_trade_product). Año, flujo, producto, valor.",
        "preview_fn": _preview_balanza_comercial,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_balanza_comercial_producto_to_db"]).save_balanza_comercial_producto_to_db(d),
        "table_name": "balanza_comercial_producto",
    },
    "producto_aeropuertos_nacional": {
        "name": "Operaciones Aeroportuarias (Producto Aeropuertos)",
        "description": "Excel producto-aeropuertos (TD Prod Aptos). Año, aeropuerto, operaciones.",
        "preview_fn": _preview_producto_aeropuertos,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_producto_aeropuertos_nacional_to_db"]).save_producto_aeropuertos_nacional_to_db(d),
        "table_name": "producto_aeropuertos_nacional",
    },
    "participacion_mercado_aereo": {
        "name": "Participación Mercado Aéreo",
        "description": "DataTur CUADRO_DGAC. Nacional (aerolíneas) e internacional (regiones).",
        "preview_fn": _preview_participacion_mercado,
        "save_fn": _save_participacion_mercado,
        "table_name": "participacion_mercado_aereo + participacion_internacional_region",
    },
    "actividad_hotelera": {
        "name": "Actividad hotelera nacional",
        "description": "DataTur Base70centros.csv. Por año y por categoría.",
        "preview_fn": _preview_actividad_hotelera_nacional,
        "save_fn": _save_actividad_hotelera_nacional,
        "table_name": "actividad_hotelera_nacional + actividad_hotelera_nacional_por_categoria",
    },
    # Anuncios de inversión
    "anuncios_inversion_combinados": {
        "name": "Anuncios de Inversión Combinados",
        "description": "DataMéxico. Año, estado, num_anuncios, monto_inversión.",
        "preview_fn": _preview_anuncios_combinados,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_anuncios_combinados_to_db"]).save_anuncios_combinados_to_db(d),
        "table_name": "anuncios_inversion_combinados",
    },
    "anuncios_inversion_base": {
        "name": "Anuncios de Inversión Base",
        "description": "DataMéxico. Año, país, estado, sector, monto.",
        "preview_fn": _preview_anuncios_base,
        "save_fn": lambda d: __import__("services.db", fromlist=["save_anuncios_base_to_db"]).save_anuncios_base_to_db(d),
        "table_name": "anuncios_inversion_base",
    },
}
