# api/core/mapa_fuentes.py
"""Matriz pública de fuentes del módulo mapa (trazabilidad; MVP + refuerzo)."""

from __future__ import annotations

from datetime import datetime, timezone

CATALOGO_FUENTES_MAPA: list[dict[str, str | bool]] = [
    {
        "id": "inegi_wscatgeo_ageb",
        "nombre": "INEGI — wscatgeo (AGEB urbano/rural)",
        "url": "https://gaia.inegi.org.mx/wscatgeo/v2",
        "cobertura": "Nacional",
        "periodicidad": "Corte 2020 (Marco Geoestadístico)",
        "formato": "GeoJSON / API",
        "acceso": "api",
        "token_requerido": False,
        "capa_indicador": "ageb_urbano / ageb_rural",
        "fase": "mvp",
    },
    {
        "id": "osm_overpass",
        "nombre": "OpenStreetMap — Overpass",
        "url": "https://overpass-api.de",
        "cobertura": "Global (datos de comunidad)",
        "periodicidad": "En vivo",
        "formato": "Overpass QL / JSON",
        "acceso": "api",
        "token_requerido": False,
        "capa_indicador": "red_vial, nodos, zonas",
        "fase": "mvp",
    },
    {
        "id": "inegi_denue",
        "nombre": "INEGI — DENUE (aprox. POI oficiales)",
        "url": "https://www.inegi.org.mx/temas/mapa/",
        "cobertura": "Nacional",
        "periodicidad": "Actualización periódica",
        "formato": "Capas / tabulado",
        "acceso": "descarga",
        "token_requerido": False,
        "capa_indicador": "poi_hoteles / comercio",
        "fase": "mvp",
    },
    {
        "id": "sectur_pueblos",
        "nombre": "SECTUR — Pueblos Mágicos",
        "url": "https://www.gob.mx/sectur",
        "cobertura": "Municipios designados",
        "periodicidad": "Listado oficial",
        "formato": "Tabulado / ETL",
        "acceso": "hibrido",
        "token_requerido": False,
        "capa_indicador": "pueblos_magicos_cercanos",
        "fase": "mvp",
    },
    {
        "id": "google_places",
        "nombre": "Google Places (modo híbrido, opcional)",
        "url": "https://developers.google.com/maps/documentation/places",
        "cobertura": "Condicionada a licencia/cuota",
        "periodicidad": "En vivo",
        "formato": "API REST",
        "acceso": "api",
        "token_requerido": True,
        "capa_indicador": "POI híbrido (proximidad)",
        "fase": "refuerzo",
    },
    {
        "id": "inventario_10_indicadores",
        "nombre": "AEDMI — indicadores de ubicación (cálculo local)",
        "url": "internal://mapa/indicadores",
        "cobertura": "7 ciudades (alcance acotado)",
        "periodicidad": "Por consulta",
        "formato": "JSON (API interna)",
        "acceso": "api",
        "token_requerido": False,
        "capa_indicador": "Bloques estado / proximidad / accesos",
        "fase": "mvp",
    },
]


def indicadores_mapa_10() -> list[dict[str, str]]:
    now = datetime.now(tz=timezone.utc).year
    base = [
        (
            "densidad_entorno",
            "Densidad edificatoria aproximada",
            "unidades/km2",
            "estado_propiedad",
        ),
        ("hacinamiento", "Hacinamiento aproximado (proxy)", "índice", "estado_propiedad"),
        ("diversidad_usos", "Diversidad de usos (proxy)", "índice 0-1", "estado_propiedad"),
        ("dist_poi", "Distancia a POI relevante", "m", "proximidad"),
        ("cobertura_comercial", "Cobertura comercial en radio", "%", "proximidad"),
        (
            "cobertura_salud_edu",
            "Cobertura servicios básicos (proxy)",
            "%",
            "proximidad",
        ),
        (
            "conectividad_vial",
            "Conectividad vial aproximada (proxy)",
            "índice",
            "mapa_accesos",
        ),
        ("tiempo_urbano", "Tiempo de entorno (proxy)", "min", "mapa_accesos"),
        ("ruta_salida", "Eficiencia de ruta a red vial (proxy)", "índice", "mapa_accesos"),
        (
            "vulnerabilidad_riesgo",
            "Riesgo ambiental/urbano (proxy)",
            "índice 0-1",
            "estado_propiedad",
        ),
    ]
    return [
        {
            "clave": t[0],
            "descripcion": t[1],
            "unidad": t[2],
            "bloque": t[3],
            "fuente": "AEDMI mapa (metadato)",
            "dependencia_radio": "sí"
            if t[0] not in ("densidad_entorno", "hacinamiento")
            else "parcial",
            "año": str(now),
        }
        for t in base
    ]
