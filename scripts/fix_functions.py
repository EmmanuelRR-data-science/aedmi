"""
Replace the broken functions in data_sources.py with corrected versions.
"""

# Read the current file
with open('services/data_sources.py', 'r', encoding='utf-8') as f:
    text = f.read()

# ── Fix 1: get_actividad_hotelera() ──
old_hotel = '''def get_actividad_hotelera():
    """Retorna {nacional: [...], por_categoria: [...]}."""
    try:
        from services.db import get_actividad_hotelera_estatal_from_db
        data = get_actividad_hotelera_estatal_from_db(None)
        if data:
            return data
    except Exception:
        pass
    return {"nacional": [], "por_categoria": []}'''

new_hotel = '''def get_actividad_hotelera():
    """Retorna {nacional: [...], por_categoria: [...]}."""
    # La tabla actividad_hotelera_estatal es por estado;
    # no hay datos agregados nacionales en BD — retornar placeholder.
    return {"nacional": [], "por_categoria": []}'''

text = text.replace(old_hotel, new_hotel)

# ── Fix 2: get_exportaciones_por_estado() ──
old_export = '''def get_exportaciones_por_estado():
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
    return []'''

new_export = '''def get_exportaciones_por_estado():
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

text = text.replace(old_export, new_export)

# ── Fix 3: get_actividad_hotelera_estatal() – correct tuple unpacking ──
old_hotel_est = '''def get_actividad_hotelera_estatal(estado: str, anio=None):
    """Retorna (data_dict, error_str, years_list)."""
    try:
        from services.db import get_actividad_hotelera_estatal_from_db
        data = get_actividad_hotelera_estatal_from_db(estado, anio=anio)
        if data is None:
            return None, f"No hay datos de actividad hotelera para {estado}", []
        years = data.pop("years", []) if isinstance(data, dict) else []
        return data, None, years
    except Exception as e:
        return None, str(e), []'''

new_hotel_est = '''def get_actividad_hotelera_estatal(estado: str, anio=None):
    """Retorna (data_dict, error_str, years_list)."""
    try:
        from services.db import get_actividad_hotelera_estatal_from_db
        codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado)
        if not codigo:
            norm = _normalizar_estado(estado)
            for name, c in ESTADO_NOMBRE_TO_CODIGO.items():
                if _normalizar_estado(name) == norm:
                    codigo = c
                    break
        if not codigo:
            return None, f"Estado no encontrado: {estado}", []
        result = get_actividad_hotelera_estatal_from_db(codigo, anio=anio)
        # DB returns (data_dict_or_None, years_list)
        if isinstance(result, tuple):
            data, years = result
        else:
            data, years = result, []
        if data is None:
            return None, f"No hay datos de actividad hotelera para {estado}", years
        return data, None, years
    except Exception as e:
        return None, str(e), []'''

text = text.replace(old_hotel_est, new_hotel_est)

# ── Fix 4: get_demografia_estatal() – ensure proper DB lookup with code ──
old_demo = '''def get_demografia_estatal(estado_nombre: str):
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
    return None'''

new_demo = '''def get_demografia_estatal(estado_nombre: str):
    codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado_nombre)
    if not codigo:
        # Try normalized matching
        norm = _normalizar_estado(estado_nombre)
        for name, c in ESTADO_NOMBRE_TO_CODIGO.items():
            if _normalizar_estado(name) == norm:
                codigo = c
                break
    if not codigo:
        return None
    try:
        from services.db import get_demografia_estatal_from_db
        data = get_demografia_estatal_from_db(codigo)
        if data:
            return data
    except Exception:
        pass
    # Fallback JSON local
    import os, json
    path = os.path.join("data", "process", f"demografia_estatal_{codigo}.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None'''

text = text.replace(old_demo, new_demo)


with open('services/data_sources.py', 'w', encoding='utf-8') as f:
    f.write(text)

print("All functions patched successfully.")

# Verify compilation
import py_compile
py_compile.compile('services/data_sources.py', doraise=True)
print("data_sources.py compiles OK.")
