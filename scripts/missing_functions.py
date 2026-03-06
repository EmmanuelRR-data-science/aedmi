
# ──────────────────────────────────────────────────────────────
# Funciones recuperadas (21) – se perdieron con git restore
# ──────────────────────────────────────────────────────────────

import unicodedata as _unicodedata

# ── 1. Diccionario nombre → código INEGI (2 dígitos) ────────
ESTADO_NOMBRE_TO_CODIGO = {
    "Aguascalientes": "01",
    "Baja California": "02",
    "Baja California Sur": "03",
    "Campeche": "04",
    "Coahuila": "05", "Coahuila de Zaragoza": "05",
    "Colima": "06",
    "Chiapas": "07",
    "Chihuahua": "08",
    "CDMX": "09", "Ciudad de México": "09",
    "Durango": "10",
    "Guanajuato": "11",
    "Guerrero": "12",
    "Hidalgo": "13",
    "Jalisco": "14",
    "Estado de México": "15", "México": "15",
    "Michoacán": "16", "Michoacán de Ocampo": "16",
    "Morelos": "17",
    "Nayarit": "18",
    "Nuevo León": "19",
    "Oaxaca": "20",
    "Puebla": "21",
    "Querétaro": "22",
    "Quintana Roo": "23",
    "San Luis Potosí": "24",
    "Sinaloa": "25",
    "Sonora": "26",
    "Tabasco": "27",
    "Tamaulipas": "28",
    "Tlaxcala": "29",
    "Veracruz": "30", "Veracruz de Ignacio de la Llave": "30",
    "Yucatán": "31",
    "Zacatecas": "32",
}


# ── 2. Normalizar nombre de estado ──────────────────────────
def _normalizar_estado(name: str) -> str:
    if not name:
        return ""
    name = name.lower().strip()
    return "".join(
        c for c in _unicodedata.normalize("NFD", name)
        if _unicodedata.category(c) != "Mn"
    )


# ── 3. get_pib_sector_economico ─────────────────────────────
def get_pib_sector_economico():
    """Lee pob_sector_actividad de PostgreSQL."""
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT sector, valor, pct, es_residual FROM pob_sector_actividad ORDER BY valor DESC")
            rows = cur.fetchall()
            return [
                {"sector": r[0], "valor": r[1], "pct": float(r[2]) if r[2] is not None else 0, "es_residual": r[3]}
                for r in rows
            ]
    except Exception:
        return []


# ── 4. get_balanza_comercial_producto ────────────────────────
def get_balanza_comercial_producto():
    """Devuelve datos de balanza comercial.  Intenta DataMéxico primero, luego BD."""
    try:
        import requests
        url = "https://api.datamexico.org/tesseract/data.jsonrecords?cube=economy_foreign_trade_ent&drilldowns=Year&measures=Trade+Value&parents=false&sparse=false"
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            body = r.json()
            data = body.get("data", [])
            if data:
                return data
    except Exception:
        pass
    # Fallback CSV
    try:
        import csv, os
        path = os.path.join("data", "process", "balanza_visitantes_inegi.csv")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                return [dict(row) for row in reader]
    except Exception:
        pass
    return []


# ── 5. get_operaciones_aeroportuarias ────────────────────────
def get_operaciones_aeroportuarias():
    """Retorna {total: int, por_grupo: [{nombre, operaciones, pct}]}."""
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT aerolinea, participacion FROM participacion_mercado_aereo")
            rows = cur.fetchall()
            if rows:
                total = sum(float(r[1]) for r in rows if r[1])
                por_grupo = [
                    {"nombre": r[0], "operaciones": 0, "pct": round(float(r[1]) * 100, 2) if r[1] else 0}
                    for r in rows
                ]
                return {"total": len(rows), "por_grupo": por_grupo}
    except Exception:
        pass
    return {"total": 0, "por_grupo": []}


# ── 6. get_actividad_hotelera ────────────────────────────────
def get_actividad_hotelera():
    """Retorna {nacional: [...], por_categoria: [...]}."""
    try:
        from services.db import get_actividad_hotelera_estatal_from_db
        data = get_actividad_hotelera_estatal_from_db(None)
        if data:
            return data
    except Exception:
        pass
    return {"nacional": [], "por_categoria": []}


# ── 7. get_demografia_estatal ────────────────────────────────
def get_demografia_estatal(estado_nombre: str):
    codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado_nombre)
    if not codigo:
        return None
    try:
        from services.db import get_demografia_estatal_from_db
        return get_demografia_estatal_from_db(codigo)
    except Exception:
        pass
    # Fallback JSON local
    import os, json
    path = os.path.join("data", "process", f"demografia_estatal_{codigo}.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


# ── 8. get_proyecciones_conapo ───────────────────────────────
def get_proyecciones_conapo(estado_nombre: str):
    codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado_nombre)
    if not codigo:
        return None
    try:
        from services.db import get_proyecciones_conapo_from_db
        data = get_proyecciones_conapo_from_db(codigo)
        if data:
            return data
    except Exception:
        pass
    return None


# ── 9. get_itaee_estatal ─────────────────────────────────────
def get_itaee_estatal(estado_nombre: str):
    codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado_nombre)
    if not codigo:
        return None
    try:
        from services.db import get_itaee_estatal_from_db
        data = get_itaee_estatal_from_db(codigo)
        if data:
            return data
    except Exception:
        pass
    return None


# ── 10. get_municipios_por_estado ────────────────────────────
def get_municipios_por_estado(estado_nombre: str):
    try:
        from services.db import get_municipios_from_db
        return get_municipios_from_db(estado_nombre)
    except Exception:
        return []


# ── 11. get_localidades ─────────────────────────────────────
def get_localidades(estado: str, municipio: str):
    try:
        from services.db import get_localidades_from_db
        return get_localidades_from_db(estado, municipio)
    except Exception:
        return []


# ── 12. get_distribucion_poblacion_localidad ─────────────────
def get_distribucion_poblacion_localidad(estado: str, municipio: str, localidad: str):
    try:
        from services.db import get_distribucion_poblacion_localidad_from_db
        return get_distribucion_poblacion_localidad_from_db(estado, municipio, localidad)
    except Exception:
        return None


# ── 13. get_distribucion_poblacion_municipal ─────────────────
def get_distribucion_poblacion_municipal(estado: str, municipio: str):
    try:
        from services.db import get_distribucion_poblacion_municipal_from_db
        return get_distribucion_poblacion_municipal_from_db(estado, municipio)
    except Exception:
        return None


# ── 14. get_aeropuertos_por_estado ───────────────────────────
def get_aeropuertos_por_estado(codigo: str):
    try:
        from services.db import get_aeropuertos_estatal_from_db
        return get_aeropuertos_estatal_from_db(codigo)
    except Exception:
        return []


# ── 15. get_exportaciones_por_estado ─────────────────────────
def get_exportaciones_por_estado():
    """Intenta DataMéxico, luego BD, luego CSV."""
    try:
        import requests
        url = "https://api.datamexico.org/tesseract/data.jsonrecords?cube=economy_foreign_trade_ent&drilldowns=Year,State&measures=Trade+Value&parents=false&sparse=false"
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            body = r.json()
            data = body.get("data", [])
            if data:
                result = []
                for row in data:
                    result.append({
                        "year": row.get("Year"),
                        "state_slug": str(row.get("State", "")).lower().replace(" ", "-"),
                        "trade_value": row.get("Trade Value", 0),
                    })
                return result
    except Exception:
        pass
    # Fallback BD
    try:
        from services.db import get_exportaciones_estatal_from_db
        return get_exportaciones_estatal_from_db()
    except Exception:
        pass
    return []


# ── 16. get_proyeccion_poblacional_municipal ────────────────
def get_proyeccion_poblacional_municipal(estado: str, municipio: str):
    try:
        from services.db import get_proyeccion_poblacional_municipal_from_db
        return get_proyeccion_poblacional_municipal_from_db(estado, municipio)
    except Exception:
        return None


# ── 17. get_actividad_hotelera_estatal ──────────────────────
def get_actividad_hotelera_estatal(estado: str, anio=None):
    """Retorna (data_dict, error_str, years_list)."""
    try:
        from services.db import get_actividad_hotelera_estatal_from_db
        data = get_actividad_hotelera_estatal_from_db(estado, anio=anio)
        if data is None:
            return None, f"No hay datos de actividad hotelera para {estado}", []
        years = data.pop("years", []) if isinstance(data, dict) else []
        return data, None, years
    except Exception as e:
        return None, str(e), []


# ── 18. get_crecimiento_historico_localidad ──────────────────
def get_crecimiento_historico_localidad(estado: str, municipio: str, localidad: str):
    try:
        from services.db import get_crecimiento_historico_localidad_from_db
        return get_crecimiento_historico_localidad_from_db(estado, municipio, localidad)
    except Exception:
        return []


# ── 19. get_mapa_carretero_estatal ───────────────────────────
def get_mapa_carretero_estatal(estado: str):
    """Retorna (bytes_png | None, error_str | None)."""
    try:
        import requests
        norm = _normalizar_estado(estado).replace(" ", "-")
        url = f"https://www.sct.gob.mx/fileadmin/DireccionesGrales/DGIT/Mapas-Carreteros/{norm}.png"
        r = requests.get(url, timeout=15)
        if r.status_code == 200 and len(r.content) > 1000:
            return r.content, None
        return None, f"No se encontró mapa carretero para {estado}"
    except Exception as e:
        return None, str(e)


# ── 20. process_llegada_turistas_from_upload ─────────────────
def process_llegada_turistas_from_upload(filepath: str):
    """Procesa archivo Excel de llegada de turistas. Retorna (data_by_estado, error)."""
    try:
        import pandas as pd
        df = pd.read_excel(filepath)
        if df.empty:
            return None, "Archivo vacío"
        data_by_estado = {}
        for _, row in df.iterrows():
            estado = str(row.get("Estado", row.get("estado", ""))).strip()
            if not estado:
                continue
            if estado not in data_by_estado:
                data_by_estado[estado] = []
            data_by_estado[estado].append(row.to_dict())
        return data_by_estado, None
    except Exception as e:
        return None, str(e)


# ── 21. process_actividad_hotelera_from_upload ───────────────
def process_actividad_hotelera_from_upload(filepath: str):
    """Procesa archivo Excel de CETM actividad hotelera. Retorna (data_by_estado, error)."""
    try:
        import pandas as pd
        df = pd.read_excel(filepath)
        if df.empty:
            return None, "Archivo vacío"
        data_by_estado = {}
        for _, row in df.iterrows():
            estado = str(row.get("Estado", row.get("ESTADO", row.get("estado", "")))).strip()
            if not estado:
                continue
            if estado not in data_by_estado:
                data_by_estado[estado] = []
            data_by_estado[estado].append(row.to_dict())
        return data_by_estado, None
    except Exception as e:
        return None, str(e)
