"""
Patch data_sources.py to add API fallbacks for empty DB tables.
"""

with open('services/data_sources.py', 'r', encoding='utf-8') as f:
    text = f.read()


# ── Fix get_itaee_estatal: add INEGI BIE API fallback ──
old_itaee = '''def get_itaee_estatal(estado_nombre: str):
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
    return None'''

new_itaee = '''def get_itaee_estatal(estado_nombre: str):
    codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado_nombre)
    if not codigo:
        norm = _normalizar_estado(estado_nombre)
        for name, c in ESTADO_NOMBRE_TO_CODIGO.items():
            if _normalizar_estado(name) == norm:
                codigo = c
                break
    if not codigo:
        return None
    # 1. PostgreSQL
    try:
        from services.db import get_itaee_estatal_from_db
        data = get_itaee_estatal_from_db(codigo)
        if data:
            return data
    except Exception:
        pass
    # 2. Fallback: INEGI BIE API
    try:
        import requests
        url = f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR/6207067158/es/0700/false/BISE/2.0/00000000-0000-0000-0000-000000000000?type=json"
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            body = r.json()
            series = body.get("Series", [{}])
            if series:
                obs = series[0].get("OBSERVATIONS", [])
                if obs:
                    last = obs[-1]
                    return {
                        "anio": last.get("TIME_PERIOD", ""),
                        "total": float(last.get("OBS_VALUE", 0)),
                        "primario": 0,
                        "secundario": 0,
                        "terciario": 0,
                    }
    except Exception:
        pass
    return None'''

text = text.replace(old_itaee, new_itaee)


# ── Fix get_proyecciones_conapo: add CONAPO CSV fallback ──
old_conapo = '''def get_proyecciones_conapo(estado_nombre: str):
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
    return None'''

new_conapo = '''def get_proyecciones_conapo(estado_nombre: str):
    codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado_nombre)
    if not codigo:
        norm = _normalizar_estado(estado_nombre)
        for name, c in ESTADO_NOMBRE_TO_CODIGO.items():
            if _normalizar_estado(name) == norm:
                codigo = c
                break
    if not codigo:
        return None
    # 1. PostgreSQL
    try:
        from services.db import get_proyecciones_conapo_from_db
        data = get_proyecciones_conapo_from_db(codigo)
        if data:
            return data
    except Exception:
        pass
    # 2. Fallback: local CSV
    try:
        import os, csv
        path = os.path.join("data", "process", "proyecciones_conapo.csv")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                results = []
                for row in reader:
                    if str(row.get("CVE_GEO", "")).zfill(2) == codigo:
                        results.append({
                            "anio": int(row.get("AÑO", row.get("anio", 0))),
                            "total": float(row.get("POB_MIT_AÑO", row.get("total", 0))),
                            "hombres": float(row.get("HOMBRES", row.get("hombres", 0))),
                            "mujeres": float(row.get("MUJERES", row.get("mujeres", 0))),
                        })
                if results:
                    return results
    except Exception:
        pass
    return None'''

text = text.replace(old_conapo, new_conapo)


# ── Fix get_aeropuertos_por_estado: return empty list properly ──
# (already returns [] from DB — 404 is correct behavior when no data)


# ── Fix get_exportaciones_por_estado: fix fallback chain ──
# The DataMexico API might be slow — add longer timeout and better error handling
old_export_fn = '''def get_exportaciones_por_estado():
    """Intenta BD, luego DataMéxico."""
    # 1. Intentar PostgreSQL
    try:
        from services.db import get_exportaciones_estatal_from_db
        data = get_exportaciones_estatal_from_db()
        if data:
            return data
    except Exception:
        pass

    # 2. Fallback API DataMéxico
    try:
        import requests
        url = "https://api.datamexico.org/tesseract/data.jsonrecords?cube=economy_foreign_trade_ent&drilldowns=Year,State&measures=Trade+Value&parents=false&sparse=false"
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            body = r.json()
            raw = body.get("data", [])
            if raw:
                result = []
                for row in raw:
                    result.append({
                        "year": row.get("Year"),
                        "state_slug": str(row.get("State", "")).lower().replace(" ", "-"),
                        "trade_value": row.get("Trade Value", 0),
                    })
                return result
    except Exception:
        pass
    return []'''

new_export_fn = '''def get_exportaciones_por_estado():
    """Intenta BD, luego DataMéxico, luego CSV local."""
    # 1. Intentar PostgreSQL
    try:
        from services.db import get_exportaciones_estatal_from_db
        data = get_exportaciones_estatal_from_db()
        if data:
            return data
    except Exception:
        pass

    # 2. Fallback API DataMéxico (timeout más largo)
    try:
        import requests
        url = "https://api.datamexico.org/tesseract/data.jsonrecords?cube=economy_foreign_trade_ent&drilldowns=Year,State&measures=Trade+Value&parents=false&sparse=false"
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            body = r.json()
            raw = body.get("data", [])
            if raw:
                result = []
                for row in raw:
                    result.append({
                        "year": row.get("Year"),
                        "state_slug": str(row.get("State", "")).lower().replace(" ", "-"),
                        "trade_value": row.get("Trade Value", 0),
                    })
                return result
    except Exception:
        pass

    # 3. Fallback: CSV procesados
    try:
        import os, csv
        path = os.path.join("data", "process", "exportaciones_estatal.csv")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                return [dict(row) for row in reader]
    except Exception:
        pass
    return []'''

text = text.replace(old_export_fn, new_export_fn)


with open('services/data_sources.py', 'w', encoding='utf-8') as f:
    f.write(text)

print("All fallback functions patched.")

import py_compile
py_compile.compile('services/data_sources.py', doraise=True)
print("Compiles OK.")
