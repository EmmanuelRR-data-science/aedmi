import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import get_settings
from core.db import get_db
from routers.auth import get_current_user
from schemas.mapa import (
    MapaAgebResponse,
    MapaAlcanceResponse,
    MapaAnalisisIARequest,
    MapaAnalisisIAResponse,
    MapaCapaDatosResponse,
    MapaCapaFeatureResponse,
    MapaCapaResponse,
    MapaCiudadObjetivoResponse,
    MapaDegradacionResponse,
    MapaIndicadorResponse,
    MapaPoiTopResponse,
    MapaPuebloMagicoResponse,
    MapaQueryRequest,
    MapaQueryResponse,
    MapaSugerenciaResponse,
    MapaUbicacionResponse,
)

router = APIRouter(prefix="/mapa", tags=["mapa"])
settings = get_settings()
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"

WSCATGEO_BASE = "https://gaia.inegi.org.mx/wscatgeo/v2/geo"
WSCATGEO_TABULAR_BASE = "https://gaia.inegi.org.mx/wscatgeo/v2"
WSCATGEO_TIMEOUT_SECONDS = 4.0
OVERPASS_BASE = "https://overpass-api.de/api/interpreter"
OVERPASS_BASE_FALLBACK = "https://overpass.kumi.systems/api/interpreter"
OVERPASS_TIMEOUT_SECONDS = 8.0
OVERPASS_LAYER_RADIUS_MAX_M = 1500
GEOJSON_CACHE_TTL_SECONDS = 600
TABULAR_CACHE_TTL_SECONDS = 300
OVERPASS_CACHE_TTL_SECONDS = 120
AGEB_FALLBACK_MAX_AGE_SECONDS = 86400
DEFAULT_CVE_ENT = "22"
DEFAULT_CVE_MUN = "014"

_geojson_cache: dict[str, tuple[datetime, dict[str, Any]]] = {}
_tabular_cache: dict[str, tuple[datetime, Any]] = {}
_overpass_cache: dict[str, tuple[datetime, dict[str, Any]]] = {}
_ageb_last_good_by_region: dict[str, tuple[datetime, MapaAgebResponse]] = {}

_POI_CATEGORIA_TO_LAYER: dict[str, str] = {
    "hotelero": "poi_hoteles",
    "comercial": "poi_comercio",
    "salud": "poi_comercio",
    "educacion": "poi_comercio",
    "transporte": "nodos_transporte",
    "turistico": "poi_comercio",
}

_POI_PESO_CATEGORIA: dict[str, float] = {
    "transporte": 1.0,
    "salud": 0.9,
    "educacion": 0.8,
    "hotelero": 0.7,
    "comercial": 0.6,
    "turistico": 0.55,
}

POI_BASE: list[dict[str, Any]] = [
    {
        "id": "poi_hotel_centro",
        "nombre": "Hotel Centro Ejecutivo",
        "categoria": "hotelero",
        "lat_offset": 0.012,
        "lng_offset": -0.010,
        "source_type_rt": "hybrid",
        "source_name_rt": "INEGI DENUE + Google Places",
        "source_type_official": "official",
        "source_name_official": "INEGI DENUE",
    },
    {
        "id": "poi_plaza_principal",
        "nombre": "Plaza Comercial Principal",
        "categoria": "comercial",
        "lat_offset": -0.009,
        "lng_offset": 0.011,
        "source_type_rt": "hybrid",
        "source_name_rt": "INEGI DENUE + Google Places",
        "source_type_official": "official",
        "source_name_official": "INEGI DENUE",
    },
    {
        "id": "poi_hospital_regional",
        "nombre": "Hospital Regional",
        "categoria": "salud",
        "lat_offset": 0.017,
        "lng_offset": 0.004,
        "source_type_rt": "official",
        "source_name_rt": "INEGI DENUE",
        "source_type_official": "official",
        "source_name_official": "INEGI DENUE",
    },
    {
        "id": "poi_universidad_tecnologica",
        "nombre": "Universidad Tecnológica",
        "categoria": "educacion",
        "lat_offset": -0.014,
        "lng_offset": -0.007,
        "source_type_rt": "official",
        "source_name_rt": "INEGI DENUE",
        "source_type_official": "official",
        "source_name_official": "INEGI DENUE",
    },
    {
        "id": "poi_terminal_autobuses",
        "nombre": "Terminal de Autobuses",
        "categoria": "transporte",
        "lat_offset": 0.004,
        "lng_offset": 0.018,
        "source_type_rt": "official",
        "source_name_rt": "INEGI RNC",
        "source_type_official": "official",
        "source_name_official": "INEGI RNC",
    },
    {
        "id": "poi_museo_historia",
        "nombre": "Museo de Historia Regional",
        "categoria": "turistico",
        "lat_offset": -0.006,
        "lng_offset": 0.002,
        "source_type_rt": "hybrid",
        "source_name_rt": "SECTUR + Google Places",
        "source_type_official": "official",
        "source_name_official": "SECTUR",
    },
    {
        "id": "poi_hotel_bussines",
        "nombre": "Hotel Business Park",
        "categoria": "hotelero",
        "lat_offset": 0.021,
        "lng_offset": -0.016,
        "source_type_rt": "hybrid",
        "source_name_rt": "INEGI DENUE + Google Places",
        "source_type_official": "official",
        "source_name_official": "INEGI DENUE",
    },
    {
        "id": "poi_centro_medico_norte",
        "nombre": "Centro Médico Norte",
        "categoria": "salud",
        "lat_offset": 0.026,
        "lng_offset": 0.010,
        "source_type_rt": "official",
        "source_name_rt": "INEGI DENUE",
        "source_type_official": "official",
        "source_name_official": "INEGI DENUE",
    },
    {
        "id": "poi_plaza_oriente",
        "nombre": "Plaza Oriente",
        "categoria": "comercial",
        "lat_offset": -0.020,
        "lng_offset": 0.015,
        "source_type_rt": "hybrid",
        "source_name_rt": "INEGI DENUE + Google Places",
        "source_type_official": "official",
        "source_name_official": "INEGI DENUE",
    },
    {
        "id": "poi_campus_central",
        "nombre": "Campus Central Universitario",
        "categoria": "educacion",
        "lat_offset": -0.023,
        "lng_offset": -0.004,
        "source_type_rt": "official",
        "source_name_rt": "INEGI DENUE",
        "source_type_official": "official",
        "source_name_official": "INEGI DENUE",
    },
    {
        "id": "poi_parque_turistico",
        "nombre": "Parque Turístico Municipal",
        "categoria": "turistico",
        "lat_offset": 0.031,
        "lng_offset": -0.001,
        "source_type_rt": "hybrid",
        "source_name_rt": "SECTUR + Google Places",
        "source_type_official": "official",
        "source_name_official": "SECTUR",
    },
    {
        "id": "poi_aeropuerto_regional",
        "nombre": "Aeropuerto Regional",
        "categoria": "transporte",
        "lat_offset": 0.036,
        "lng_offset": -0.020,
        "source_type_rt": "official",
        "source_name_rt": "AFAC + INEGI RNC",
        "source_type_official": "official",
        "source_name_official": "AFAC + INEGI RNC",
    },
]

PUEBLOS_MAGICOS_BASE: list[dict[str, Any]] = [
    {
        "id": "pm_bernal",
        "nombre": "Bernal",
        "entidad": "Querétaro",
        "lat": 20.7417,
        "lng": -99.9417,
    },
    {
        "id": "pm_tequisquiapan",
        "nombre": "Tequisquiapan",
        "entidad": "Querétaro",
        "lat": 20.5205,
        "lng": -99.8917,
    },
    {
        "id": "pm_san_miguel",
        "nombre": "San Miguel de Allende",
        "entidad": "Guanajuato",
        "lat": 20.9144,
        "lng": -100.7436,
    },
    {
        "id": "pm_dolores",
        "nombre": "Dolores Hidalgo",
        "entidad": "Guanajuato",
        "lat": 21.1578,
        "lng": -100.9302,
    },
    {
        "id": "pm_real_catorce",
        "nombre": "Real de Catorce",
        "entidad": "San Luis Potosí",
        "lat": 23.6904,
        "lng": -100.8857,
    },
    {
        "id": "pm_tlalpujahua",
        "nombre": "Tlalpujahua",
        "entidad": "Michoacán",
        "lat": 19.8055,
        "lng": -100.1723,
    },
    {
        "id": "pm_mineral_chico",
        "nombre": "Mineral del Chico",
        "entidad": "Hidalgo",
        "lat": 20.2144,
        "lng": -98.7313,
    },
    {
        "id": "pm_cholula",
        "nombre": "San Pedro Cholula",
        "entidad": "Puebla",
        "lat": 19.0610,
        "lng": -98.3074,
    },
    {
        "id": "pm_valle_bravo",
        "nombre": "Valle de Bravo",
        "entidad": "Estado de México",
        "lat": 19.1926,
        "lng": -100.1329,
    },
    {"id": "pm_taxco", "nombre": "Taxco", "entidad": "Guerrero", "lat": 18.5550, "lng": -99.6064},
]


CAPAS_BASE: list[MapaCapaResponse] = [
    MapaCapaResponse(
        id="red_vial",
        nombre="Red vial",
        categoria="mapa_accesos",
        source_type="official",
    ),
    MapaCapaResponse(
        id="ageb_urbano",
        nombre="AGEB urbana",
        categoria="estado_propiedad",
        source_type="official",
    ),
    MapaCapaResponse(
        id="ageb_rural",
        nombre="AGEB rural",
        categoria="estado_propiedad",
        source_type="official",
    ),
    MapaCapaResponse(
        id="poi_hoteles",
        nombre="Hoteles",
        categoria="proximidad",
        source_type="hybrid",
    ),
    MapaCapaResponse(
        id="poi_comercio",
        nombre="Comercio",
        categoria="proximidad",
        source_type="hybrid",
    ),
    MapaCapaResponse(
        id="nodos_transporte",
        nombre="Nodos de transporte",
        categoria="mapa_accesos",
        source_type="official",
    ),
    MapaCapaResponse(
        id="zona_industrial",
        nombre="Zonificación industrial",
        categoria="proximidad",
        source_type="official",
    ),
    MapaCapaResponse(
        id="zona_vivienda",
        nombre="Zonificación de vivienda",
        categoria="proximidad",
        source_type="official",
    ),
]

CIUDADES_OBJETIVO_BASE: list[MapaCiudadObjetivoResponse] = [
    MapaCiudadObjetivoResponse(
        id="cdmx",
        nombre="Ciudad de México",
        entidad="Ciudad de México",
        cve_ent="09",
        cve_mun="015",
        lat=19.4326,
        lng=-99.1332,
    ),
    MapaCiudadObjetivoResponse(
        id="monterrey",
        nombre="Monterrey",
        entidad="Nuevo León",
        cve_ent="19",
        cve_mun="039",
        lat=25.6866,
        lng=-100.3161,
    ),
    MapaCiudadObjetivoResponse(
        id="guadalajara",
        nombre="Guadalajara",
        entidad="Jalisco",
        cve_ent="14",
        cve_mun="039",
        lat=20.6597,
        lng=-103.3496,
    ),
    MapaCiudadObjetivoResponse(
        id="puebla",
        nombre="Puebla",
        entidad="Puebla",
        cve_ent="21",
        cve_mun="114",
        lat=19.0413,
        lng=-98.2062,
    ),
    MapaCiudadObjetivoResponse(
        id="queretaro",
        nombre="Querétaro",
        entidad="Querétaro",
        cve_ent="22",
        cve_mun="014",
        lat=20.5888,
        lng=-100.3899,
    ),
    MapaCiudadObjetivoResponse(
        id="merida",
        nombre="Mérida",
        entidad="Yucatán",
        cve_ent="31",
        cve_mun="050",
        lat=20.9674,
        lng=-89.5926,
    ),
    MapaCiudadObjetivoResponse(
        id="tijuana",
        nombre="Tijuana",
        entidad="Baja California",
        cve_ent="02",
        cve_mun="004",
        lat=32.5149,
        lng=-117.0382,
    ),
]


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _cache_is_fresh(cached_at: datetime, ttl_seconds: int) -> bool:
    return (_utc_now() - cached_at) <= timedelta(seconds=ttl_seconds)


def _resolve_ciudad_objetivo(cve_ent: str, cve_mun: str) -> MapaCiudadObjetivoResponse | None:
    exact = next(
        (c for c in CIUDADES_OBJETIVO_BASE if c.cve_ent == cve_ent and c.cve_mun == cve_mun),
        None,
    )
    if exact:
        return exact
    same_entity = [c for c in CIUDADES_OBJETIVO_BASE if c.cve_ent == cve_ent]
    if len(same_entity) == 1:
        return same_entity[0]
    return None


def _normalize_capa_id(capa_id: str) -> str:
    if capa_id == "ageb_urbana":
        return "ageb_urbano"
    return capa_id


async def _call_groq_mapa(payload_json: str) -> str:
    prompt = (
        "Eres un analista territorial experto en México. "
        "Recibirás datos de filtros activos del mapa (capas y métricas resumidas). "
        "Genera un informe breve y accionable para toma de decisiones de ubicación. "
        "Incluye: 1) hallazgos clave, 2) riesgos/limitaciones de cobertura, "
        "3) recomendaciones inmediatas. Responde en Español de México, claro y conciso.\n\n"
        f"Datos:\n{payload_json}"
    )
    headers = {
        "Authorization": f"Bearer {settings.groq_api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.25,
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(GROQ_API_URL, headers=headers, json=body)
    response.raise_for_status()
    data = response.json()
    return str(data["choices"][0]["message"]["content"])


def _point_in_ring(lon: float, lat: float, ring: list[list[float]]) -> bool:
    inside = False
    if len(ring) < 3:
        return False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i]
        xj, yj = ring[j]
        intersects = ((yi > lat) != (yj > lat)) and (
            lon < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi
        )
        if intersects:
            inside = not inside
        j = i
    return inside


def _point_in_polygon(lon: float, lat: float, polygon: list[list[list[float]]]) -> bool:
    if not polygon:
        return False
    outer = polygon[0]
    if not _point_in_ring(lon, lat, outer):
        return False
    for hole in polygon[1:]:
        if _point_in_ring(lon, lat, hole):
            return False
    return True


def _feature_contains_point(feature: dict[str, Any], lon: float, lat: float) -> bool:
    geometry = feature.get("geometry") or {}
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")
    if not geom_type or coords is None:
        return False
    if geom_type == "Polygon":
        return _point_in_polygon(lon, lat, coords)
    if geom_type == "MultiPolygon":
        return any(_point_in_polygon(lon, lat, poly) for poly in coords)
    return False


def _as_feature_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    features = payload.get("features")
    if isinstance(features, list):
        return [f for f in features if isinstance(f, dict)]
    return []


def _as_record_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
    return []


def _first_prop(props: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = props.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _safe_cve_loc(cvegeo: str) -> str:
    if len(cvegeo) >= 9:
        return cvegeo[5:9]
    return "0000"


def _safe_cve_ageb(cvegeo: str) -> str:
    if len(cvegeo) >= 4:
        return cvegeo[-4:]
    return "0000"


def _feature_centroid(feature: dict[str, Any]) -> tuple[float, float] | None:
    geometry = feature.get("geometry") or {}
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")
    points: list[tuple[float, float]] = []
    if geom_type == "Polygon" and isinstance(coords, list):
        rings = coords
        if rings and isinstance(rings[0], list):
            for pair in rings[0]:
                if isinstance(pair, list) and len(pair) >= 2:
                    points.append((float(pair[0]), float(pair[1])))
    elif geom_type == "MultiPolygon" and isinstance(coords, list):
        for polygon in coords:
            if isinstance(polygon, list) and polygon:
                outer = polygon[0]
                for pair in outer:
                    if isinstance(pair, list) and len(pair) >= 2:
                        points.append((float(pair[0]), float(pair[1])))
    if not points:
        return None
    lon = sum(p[0] for p in points) / len(points)
    lat = sum(p[1] for p in points) / len(points)
    return (lat, lon)


def _nearest_feature_by_centroid(
    features: list[dict[str, Any]], lat: float, lng: float
) -> dict[str, Any] | None:
    nearest: dict[str, Any] | None = None
    nearest_dist = float("inf")
    for feature in features:
        centroid = _feature_centroid(feature)
        if not centroid:
            continue
        lat_c, lng_c = centroid
        dist = _haversine_km(lat, lng, lat_c, lng_c)
        if dist < nearest_dist:
            nearest = feature
            nearest_dist = dist
    return nearest


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except Exception:
        return None


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    earth_radius_km = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return earth_radius_km * c


async def _fetch_overpass(query: str, cache_key: str) -> dict[str, Any]:
    cached = _overpass_cache.get(cache_key)
    if cached and _cache_is_fresh(cached[0], OVERPASS_CACHE_TTL_SECONDS):
        return cached[1]
    payload: dict[str, Any] | None = None
    last_exc: Exception | None = None
    for endpoint in (OVERPASS_BASE, OVERPASS_BASE_FALLBACK):
        try:
            async with httpx.AsyncClient(timeout=OVERPASS_TIMEOUT_SECONDS) as client:
                response = await client.post(endpoint, data={"data": query})
            response.raise_for_status()
            maybe_payload = response.json()
            if isinstance(maybe_payload, dict):
                payload = maybe_payload
                break
            raise ValueError("Respuesta inválida de Overpass")
        except Exception as exc:
            last_exc = exc
            continue
    if payload is None:
        if last_exc:
            raise last_exc
        raise ValueError("No se obtuvo payload de Overpass")
    _overpass_cache[cache_key] = (_utc_now(), payload)
    return payload


def _layer_is_enabled(categoria: str, capas_activas: list[str]) -> bool:
    layer_id = _POI_CATEGORIA_TO_LAYER.get(categoria)
    return not layer_id or layer_id in capas_activas


def _fallback_top_pois(
    *,
    lat: float,
    lng: float,
    radio_m: int,
    source_mode: str,
    capas_activas: list[str],
    limit: int,
    categorias: list[str] | None = None,
) -> list[MapaPoiTopResponse]:
    now = _utc_now()
    requested = {c.strip().lower() for c in (categorias or []) if c.strip()}
    pois: list[tuple[float, MapaPoiTopResponse]] = []
    for item in POI_BASE:
        categoria = str(item["categoria"])
        if requested and categoria not in requested:
            continue
        if not _layer_is_enabled(categoria, capas_activas):
            continue
        poi_lat = lat + float(item["lat_offset"])
        poi_lng = lng + float(item["lng_offset"])
        distancia_km = _haversine_km(lat, lng, poi_lat, poi_lng)
        distancia_m = distancia_km * 1000
        if distancia_m > radio_m:
            continue
        proximity_score = max(0.0, 1.0 - (distancia_m / max(radio_m, 1)))
        category_score = _POI_PESO_CATEGORIA.get(categoria, 0.4)
        if source_mode == "real_time_first":
            source_type = str(item["source_type_rt"])
            source_name = str(item["source_name_rt"])
            trust_score = 0.9 if source_type == "official" else 0.75
        else:
            source_type = str(item["source_type_official"])
            source_name = str(item["source_name_official"])
            trust_score = 0.9
        score = (0.60 * proximity_score) + (0.25 * category_score) + (0.15 * trust_score)
        pois.append(
            (
                score,
                MapaPoiTopResponse(
                    id=str(item["id"]),
                    nombre=str(item["nombre"]),
                    categoria=categoria,
                    lat=poi_lat,
                    lng=poi_lng,
                    distancia_m=round(distancia_m, 1),
                    source_type=source_type,  # type: ignore[arg-type]
                    source_name=source_name,
                    updated_at=now,
                ),
            )
        )
    pois.sort(key=lambda pair: pair[0], reverse=True)
    return [poi for _, poi in pois[:limit]]


def _osm_query_for_categories(lat: float, lng: float, radio_m: int, categorias: list[str]) -> str:
    predicates: list[str] = []
    for categoria in categorias:
        if categoria == "hotelero":
            predicates.append(f'nwr(around:{radio_m},{lat},{lng})["tourism"="hotel"];')
        elif categoria == "comercial":
            predicates.append(f'nwr(around:{radio_m},{lat},{lng})["shop"];')
        elif categoria == "salud":
            predicates.append(
                f'nwr(around:{radio_m},{lat},{lng})["amenity"~"hospital|clinic|doctors|pharmacy"];'
            )
        elif categoria == "educacion":
            predicates.append(
                f'nwr(around:{radio_m},{lat},{lng})["amenity"~"school|college|university"];'
            )
        elif categoria == "transporte":
            predicates.append(
                f'nwr(around:{radio_m},{lat},{lng})["amenity"~"bus_station|ferry_terminal"];'
            )
            predicates.append(f'nwr(around:{radio_m},{lat},{lng})["public_transport"];')
            predicates.append(f'nwr(around:{radio_m},{lat},{lng})["aeroway"="aerodrome"];')
        elif categoria == "turistico":
            predicates.append(
                f'nwr(around:{radio_m},{lat},{lng})["tourism"~"attraction|museum|theme_park|viewpoint"];'
            )
    body = "\n".join(predicates)
    return f"[out:json][timeout:25];({body});out center tags;"


def _infer_categoria_from_tags(tags: dict[str, Any]) -> str | None:
    tourism = str(tags.get("tourism", "")).lower()
    amenity = str(tags.get("amenity", "")).lower()
    shop = str(tags.get("shop", "")).lower()
    public_transport = str(tags.get("public_transport", "")).lower()
    aeroway = str(tags.get("aeroway", "")).lower()
    if tourism == "hotel":
        return "hotelero"
    if shop:
        return "comercial"
    if amenity in {"hospital", "clinic", "doctors", "pharmacy"}:
        return "salud"
    if amenity in {"school", "college", "university"}:
        return "educacion"
    if amenity in {"bus_station", "ferry_terminal"} or public_transport or aeroway == "aerodrome":
        return "transporte"
    if tourism in {"attraction", "museum", "theme_park", "viewpoint"}:
        return "turistico"
    return None


def _resolve_poi_name(tags: dict[str, Any]) -> str | None:
    for key in ("name", "brand", "operator"):
        value = str(tags.get(key, "")).strip()
        if value:
            return value
    return None


async def _build_top_pois(
    *,
    lat: float,
    lng: float,
    radio_m: int,
    source_mode: str,
    capas_activas: list[str],
    limit: int,
    categorias: list[str] | None = None,
) -> list[MapaPoiTopResponse]:
    overpass_radio_m = min(radio_m, OVERPASS_LAYER_RADIUS_MAX_M)
    requested = [c.strip().lower() for c in (categorias or []) if c.strip()]
    if not requested:
        requested = list(_POI_CATEGORIA_TO_LAYER.keys())
    requested = [c for c in requested if _layer_is_enabled(c, capas_activas)]
    if not requested:
        return []
    try:
        query = _osm_query_for_categories(lat, lng, overpass_radio_m, requested)
        payload = await _fetch_overpass(
            query=query,
            cache_key=f"osm-poi:{lat:.4f}:{lng:.4f}:{overpass_radio_m}:{','.join(sorted(requested))}",
        )
        elements = payload.get("elements")
        if not isinstance(elements, list):
            return _fallback_top_pois(
                lat=lat,
                lng=lng,
                radio_m=radio_m,
                source_mode=source_mode,
                capas_activas=capas_activas,
                limit=limit,
                categorias=requested,
            )
        now = _utc_now()
        seen: set[str] = set()
        ranked: list[tuple[float, MapaPoiTopResponse]] = []
        for element in elements:
            if not isinstance(element, dict):
                continue
            tags = element.get("tags") or {}
            if not isinstance(tags, dict):
                continue
            categoria = _infer_categoria_from_tags(tags)
            if not categoria or categoria not in requested:
                continue
            if not _layer_is_enabled(categoria, capas_activas):
                continue
            lat_value = element.get("lat")
            lng_value = element.get("lon")
            center = element.get("center")
            if lat_value is None and isinstance(center, dict):
                lat_value = center.get("lat")
                lng_value = center.get("lon")
            poi_lat = _parse_float(lat_value)
            poi_lng = _parse_float(lng_value)
            if poi_lat is None or poi_lng is None:
                continue
            poi_id = f"osm_{element.get('type', 'x')}_{element.get('id', '0')}"
            if poi_id in seen:
                continue
            seen.add(poi_id)
            distancia_m = _haversine_km(lat, lng, poi_lat, poi_lng) * 1000
            if distancia_m > radio_m:
                continue
            nombre = _resolve_poi_name(tags)
            if not nombre:
                # Evitar etiquetas genéricas como "POI Comercial" cuando no hay nombre real.
                continue
            proximity_score = max(0.0, 1.0 - (distancia_m / max(radio_m, 1)))
            category_score = _POI_PESO_CATEGORIA.get(categoria, 0.4)
            score = (0.60 * proximity_score) + (0.25 * category_score) + (0.15 * 0.75)
            ranked.append(
                (
                    score,
                    MapaPoiTopResponse(
                        id=poi_id,
                        nombre=nombre,
                        categoria=categoria,
                        lat=poi_lat,
                        lng=poi_lng,
                        distancia_m=round(distancia_m, 1),
                        source_type="hybrid" if source_mode == "real_time_first" else "official",
                        source_name=(
                            "OpenStreetMap + INEGI (normalizado)"
                            if source_mode == "real_time_first"
                            else "OpenStreetMap (base oficial local)"
                        ),
                        updated_at=now,
                    ),
                )
            )
        ranked.sort(key=lambda item: item[0], reverse=True)
        pois = [poi for _, poi in ranked[:limit]]
        if pois:
            return pois
    except Exception:
        pass
    return _fallback_top_pois(
        lat=lat,
        lng=lng,
        radio_m=radio_m,
        source_mode=source_mode,
        capas_activas=capas_activas,
        limit=limit,
        categorias=requested,
    )


def _build_capa_feature(
    *,
    feature_id: str,
    geometry_type: str,
    coordinates: list,
    properties: dict[str, str | int | float | bool | None] | None = None,
) -> MapaCapaFeatureResponse:
    return MapaCapaFeatureResponse(
        id=feature_id,
        geometry_type=geometry_type,  # type: ignore[arg-type]
        coordinates=coordinates,
        properties=properties or {},
    )


def _bbox_from_point(lat: float, lng: float, radio_m: int) -> tuple[float, float, float, float]:
    delta_lat = radio_m / 111_320
    safe_cos = max(math.cos(math.radians(lat)), 0.1)
    delta_lng = radio_m / (111_320 * safe_cos)
    return (lng - delta_lng, lat - delta_lat, lng + delta_lng, lat + delta_lat)


def _fallback_square_polygon(
    *, feature_id: str, lat: float, lng: float, radius_m: int, layer_name: str
) -> MapaCapaFeatureResponse:
    lat_delta = radius_m / 111_320
    safe_cos = max(math.cos(math.radians(lat)), 0.1)
    lng_delta = radius_m / (111_320 * safe_cos)
    ring = [
        [lng - lng_delta, lat - lat_delta],
        [lng + lng_delta, lat - lat_delta],
        [lng + lng_delta, lat + lat_delta],
        [lng - lng_delta, lat + lat_delta],
        [lng - lng_delta, lat - lat_delta],
    ]
    return _build_capa_feature(
        feature_id=feature_id,
        geometry_type="Polygon",
        coordinates=[ring],
        properties={"fallback": True, "layer": layer_name},
    )


def _point_in_bbox(lng: float, lat: float, bbox: tuple[float, float, float, float]) -> bool:
    left, bottom, right, top = bbox
    return left <= lng <= right and bottom <= lat <= top


async def _fetch_overpass_ways(
    lat: float, lng: float, radio_m: int
) -> list[MapaCapaFeatureResponse]:
    radius = min(radio_m, OVERPASS_LAYER_RADIUS_MAX_M)
    query = f"[out:json][timeout:20];(way(around:{radius},{lat},{lng})['highway'];);out geom 90;"
    try:
        payload = await _fetch_overpass(
            query=query,
            cache_key=f"osm-roads:{lat:.4f}:{lng:.4f}:{radius}",
        )
    except Exception:
        compact_radius = min(radius, 900)
        compact_query = (
            f"[out:json][timeout:20];(way(around:{compact_radius},{lat},{lng})"
            "['highway'~'motorway|trunk|primary|secondary'];);out geom 60;"
        )
        payload = await _fetch_overpass(
            query=compact_query,
            cache_key=f"osm-roads-compact:{lat:.4f}:{lng:.4f}:{compact_radius}",
        )
    features: list[MapaCapaFeatureResponse] = []
    elements = payload.get("elements")
    if not isinstance(elements, list):
        return features
    for element in elements:
        if not isinstance(element, dict):
            continue
        geom = element.get("geometry")
        if not isinstance(geom, list):
            continue
        coords: list[list[float]] = []
        for point in geom:
            if not isinstance(point, dict):
                continue
            lat_v = _parse_float(point.get("lat"))
            lng_v = _parse_float(point.get("lon"))
            if lat_v is None or lng_v is None:
                continue
            coords.append([lng_v, lat_v])
        if len(coords) < 2:
            continue
        tags = element.get("tags") if isinstance(element.get("tags"), dict) else {}
        properties = {"highway": str(tags.get("highway", ""))}
        features.append(
            _build_capa_feature(
                feature_id=f"way_{element.get('id', '0')}",
                geometry_type="LineString",
                coordinates=coords,
                properties=properties,
            )
        )
    return features


async def _fetch_overpass_transport_nodes(
    lat: float, lng: float, radio_m: int
) -> list[MapaCapaFeatureResponse]:
    radius = min(radio_m, OVERPASS_LAYER_RADIUS_MAX_M)
    query = f"""
[out:json][timeout:20];
(
  node(around:{radius},{lat},{lng})["amenity"="bus_station"];
  node(around:{radius},{lat},{lng})["public_transport"];
  node(around:{radius},{lat},{lng})["aeroway"="aerodrome"];
);
out body;
"""
    payload = await _fetch_overpass(
        query=query,
        cache_key=f"osm-nodes:{lat:.4f}:{lng:.4f}:{radius}",
    )
    features: list[MapaCapaFeatureResponse] = []
    elements = payload.get("elements")
    if not isinstance(elements, list):
        return features
    for element in elements:
        if not isinstance(element, dict):
            continue
        lat_v = _parse_float(element.get("lat"))
        lng_v = _parse_float(element.get("lon"))
        if lat_v is None or lng_v is None:
            continue
        tags = element.get("tags") if isinstance(element.get("tags"), dict) else {}
        kind = str(
            tags.get("amenity")
            or tags.get("public_transport")
            or tags.get("aeroway")
            or tags.get("railway")
            or ""
        )
        name = _resolve_poi_name(tags)
        properties: dict[str, str | int | float | bool | None] = {
            "kind": kind,
            "operator": str(tags.get("operator", "")),
            "network": str(tags.get("network", "")),
            "ref": str(tags.get("ref", "")),
        }
        if name:
            properties["name"] = name
        features.append(
            _build_capa_feature(
                feature_id=f"node_{element.get('id', '0')}",
                geometry_type="Point",
                coordinates=[lng_v, lat_v],
                properties=properties,
            )
        )
    if features:
        return features

    # Fallback controlado para mantener continuidad visual en ciudades objetivo.
    for item in POI_BASE:
        if str(item.get("categoria")) != "transporte":
            continue
        lat_offset = _parse_float(item.get("lat_offset"))
        lng_offset = _parse_float(item.get("lng_offset"))
        if lat_offset is None or lng_offset is None:
            continue
        poi_lat = lat + lat_offset
        poi_lng = lng + lng_offset
        if (_haversine_km(lat, lng, poi_lat, poi_lng) * 1000) > radio_m:
            continue
        features.append(
            _build_capa_feature(
                feature_id=str(item.get("id", "nodo_transporte_fallback")),
                geometry_type="Point",
                coordinates=[poi_lng, poi_lat],
                properties={
                    "name": str(item.get("nombre", "Nodo de transporte")),
                    "kind": "fallback",
                    "fallback": True,
                },
            )
        )
    return features


async def _fetch_overpass_landuse_polygons(
    *, lat: float, lng: float, radio_m: int, landuse_value: str
) -> list[MapaCapaFeatureResponse]:
    radius = min(radio_m, OVERPASS_LAYER_RADIUS_MAX_M)
    query = f"""
[out:json][timeout:20];
(
  way(around:{radius},{lat},{lng})["landuse"="{landuse_value}"];
  relation(around:{radius},{lat},{lng})["landuse"="{landuse_value}"];
);
out geom;
"""
    payload = await _fetch_overpass(
        query=query,
        cache_key=f"osm-landuse:{landuse_value}:{lat:.4f}:{lng:.4f}:{radius}",
    )
    features: list[MapaCapaFeatureResponse] = []
    elements = payload.get("elements")
    if not isinstance(elements, list):
        return features
    for element in elements:
        if not isinstance(element, dict):
            continue
        geom = element.get("geometry")
        if not isinstance(geom, list):
            continue
        ring: list[list[float]] = []
        for point in geom:
            if not isinstance(point, dict):
                continue
            lat_v = _parse_float(point.get("lat"))
            lng_v = _parse_float(point.get("lon"))
            if lat_v is None or lng_v is None:
                continue
            ring.append([lng_v, lat_v])
        if len(ring) < 3:
            continue
        if ring[0] != ring[-1]:
            ring.append(ring[0])
        features.append(
            _build_capa_feature(
                feature_id=f"landuse_{landuse_value}_{element.get('id', '0')}",
                geometry_type="Polygon",
                coordinates=[ring],
                properties={"landuse": landuse_value},
            )
        )
    return features


async def _fetch_overpass_zona_vivienda(
    *, lat: float, lng: float, radio_m: int
) -> list[MapaCapaFeatureResponse]:
    # Primero intenta uso de suelo residencial.
    primary = await _fetch_overpass_landuse_polygons(
        lat=lat,
        lng=lng,
        radio_m=radio_m,
        landuse_value="residential",
    )
    if primary:
        return primary

    # Fallback real: edificios residenciales en un radio más contenido.
    radius = min(radio_m, 1000)
    query = f"""
[out:json][timeout:20];
(
  way(around:{radius},{lat},{lng})["building"~"residential|apartments|house"];
);
out geom;
"""
    payload = await _fetch_overpass(
        query=query,
        cache_key=f"osm-vivienda-building:{lat:.4f}:{lng:.4f}:{radius}",
    )
    features: list[MapaCapaFeatureResponse] = []
    elements = payload.get("elements")
    if not isinstance(elements, list):
        return features
    for element in elements:
        if not isinstance(element, dict):
            continue
        geom = element.get("geometry")
        if not isinstance(geom, list):
            continue
        ring: list[list[float]] = []
        for point in geom:
            if not isinstance(point, dict):
                continue
            lat_v = _parse_float(point.get("lat"))
            lng_v = _parse_float(point.get("lon"))
            if lat_v is None or lng_v is None:
                continue
            ring.append([lng_v, lat_v])
        if len(ring) < 3:
            continue
        if ring[0] != ring[-1]:
            ring.append(ring[0])
        features.append(
            _build_capa_feature(
                feature_id=f"vivienda_building_{element.get('id', '0')}",
                geometry_type="Polygon",
                coordinates=[ring],
                properties={"building": "residential"},
            )
        )
    return features


def _extract_polygon_features_in_bbox(
    features: list[dict[str, Any]], bbox: tuple[float, float, float, float], max_items: int = 12
) -> list[MapaCapaFeatureResponse]:
    result: list[MapaCapaFeatureResponse] = []
    for idx, feature in enumerate(features):
        geometry = feature.get("geometry") if isinstance(feature.get("geometry"), dict) else {}
        geom_type = geometry.get("type")
        coords = geometry.get("coordinates")
        if geom_type == "Polygon" and isinstance(coords, list):
            keep = False
            for ring in coords:
                if not isinstance(ring, list):
                    continue
                for pair in ring:
                    if not isinstance(pair, list) or len(pair) < 2:
                        continue
                    lng_v = _parse_float(pair[0])
                    lat_v = _parse_float(pair[1])
                    if lng_v is None or lat_v is None:
                        continue
                    if _point_in_bbox(lng_v, lat_v, bbox):
                        keep = True
                        break
                if keep:
                    break
            if keep:
                props = (
                    feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
                )
                cvegeo = _first_prop(props, "cvegeo", "CVEGEO") or f"poly_{idx}"
                result.append(
                    _build_capa_feature(
                        feature_id=cvegeo,
                        geometry_type="Polygon",
                        coordinates=coords,
                        properties={"cvegeo": cvegeo},
                    )
                )
        if len(result) >= max_items:
            break
    return result


def _extract_polygon_features_direct(
    features: list[dict[str, Any]], max_items: int = 12
) -> list[MapaCapaFeatureResponse]:
    result: list[MapaCapaFeatureResponse] = []
    for idx, feature in enumerate(features):
        geometry = feature.get("geometry") if isinstance(feature.get("geometry"), dict) else {}
        geom_type = geometry.get("type")
        coords = geometry.get("coordinates")
        if geom_type != "Polygon" or not isinstance(coords, list):
            continue
        props = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
        cvegeo = _first_prop(props, "cvegeo", "CVEGEO") or f"poly_{idx}"
        result.append(
            _build_capa_feature(
                feature_id=cvegeo,
                geometry_type="Polygon",
                coordinates=coords,
                properties={"cvegeo": cvegeo},
            )
        )
        if len(result) >= max_items:
            break
    return result


def _extract_single_polygon_feature_for_location(
    features: list[dict[str, Any]], lat: float, lng: float
) -> list[MapaCapaFeatureResponse]:
    target = next((f for f in features if _feature_contains_point(f, lng, lat)), None)
    if not target:
        target = _nearest_feature_by_centroid(features, lat, lng)
    if not target:
        return []
    geometry = target.get("geometry") if isinstance(target.get("geometry"), dict) else {}
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")
    props = target.get("properties") if isinstance(target.get("properties"), dict) else {}
    cvegeo = _first_prop(props, "cvegeo", "CVEGEO") or "ageb_ref"
    if geom_type == "Polygon" and isinstance(coords, list):
        return [
            _build_capa_feature(
                feature_id=cvegeo,
                geometry_type="Polygon",
                coordinates=coords,
                properties={"cvegeo": cvegeo},
            )
        ]
    if geom_type == "MultiPolygon" and isinstance(coords, list) and coords:
        # Se usa el primer polígono para mantener payload acotado.
        first_poly = coords[0]
        if isinstance(first_poly, list):
            return [
                _build_capa_feature(
                    feature_id=cvegeo,
                    geometry_type="Polygon",
                    coordinates=first_poly,
                    properties={"cvegeo": cvegeo},
                )
            ]
    return []


def _extract_polygon_features_for_location(
    *,
    features: list[dict[str, Any]],
    bbox: tuple[float, float, float, float],
    lat: float,
    lng: float,
    max_items: int = 12,
) -> list[MapaCapaFeatureResponse]:
    # Primero prioriza polígonos que contienen el punto seleccionado.
    inside: list[dict[str, Any]] = []
    for feature in features:
        if _feature_contains_point(feature, lng, lat):
            inside.append(feature)
    if inside:
        return _extract_polygon_features_direct(inside, max_items=max_items)
    result = _extract_polygon_features_in_bbox(features, bbox, max_items=max_items)
    if result:
        return result
    return _extract_single_polygon_feature_for_location(features, lat, lng)


async def _build_capa_datos(
    *,
    capa_id: str,
    lat: float,
    lng: float,
    radio_m: int,
    cve_ent: str,
    cve_mun: str,
    source_mode: str,
) -> MapaCapaDatosResponse:
    capa_id_norm = _normalize_capa_id(capa_id)
    capa_meta = next((c for c in CAPAS_BASE if c.id == capa_id_norm), None)
    nombre = capa_meta.nombre if capa_meta else capa_id_norm
    source_type = capa_meta.source_type if capa_meta else "official"
    source_name = "Fuente en tiempo real"
    disponibilidad = "sin_datos"
    features: list[MapaCapaFeatureResponse] = []
    bbox = _bbox_from_point(lat, lng, max(radio_m, 500))
    ciudad_objetivo = _resolve_ciudad_objetivo(cve_ent=cve_ent, cve_mun=cve_mun)
    capas_acotadas = {"ageb_urbano", "ageb_rural", "zona_industrial", "zona_vivienda"}
    if capa_id_norm in capas_acotadas and ciudad_objetivo is None:
        return MapaCapaDatosResponse(
            capa_id=capa_id_norm,
            nombre=nombre,
            source_type="official",
            source_name="Fuera de alcance (7 ciudades objetivo)",
            disponibilidad="sin_datos",
            features=[],
        )
    try:
        if capa_id_norm == "red_vial":
            features = await _fetch_overpass_ways(lat, lng, radio_m)
            source_name = "OpenStreetMap Overpass (red vial)"
            source_type = "official"
        elif capa_id_norm == "nodos_transporte":
            features = await _fetch_overpass_transport_nodes(lat, lng, radio_m)
            source_name = "OpenStreetMap Overpass (nodos transporte)"
            source_type = "official"
            if not features:
                features = [
                    _build_capa_feature(
                        feature_id="nodo_transporte_ref",
                        geometry_type="Point",
                        coordinates=[lng, lat],
                        properties={"name": "Nodo de transporte de referencia", "fallback": True},
                    )
                ]
                source_name = "Cobertura mínima de nodos (fallback)"
                disponibilidad = "parcial"
        elif capa_id_norm == "ageb_urbano":
            try:
                geo = await _fetch_wscatgeo_geojson(f"agebu/{cve_ent}/{cve_mun}")
                source_name = "INEGI wscatgeo agebu (municipal)"
            except Exception:
                geo = await _fetch_wscatgeo_geojson(f"agebu/{cve_ent}")
                source_name = "INEGI wscatgeo agebu (entidad)"
            raw_features = _as_feature_list(geo)
            if not raw_features:
                geo = await _fetch_wscatgeo_geojson(f"agebu/{cve_ent}")
                raw_features = _as_feature_list(geo)
                source_name = "INEGI wscatgeo agebu (entidad)"
            features = _extract_polygon_features_for_location(
                features=raw_features,
                bbox=bbox,
                lat=lat,
                lng=lng,
            )
            if not features and "(municipal)" in source_name:
                geo = await _fetch_wscatgeo_geojson(f"agebu/{cve_ent}")
                raw_features = _as_feature_list(geo)
                features = _extract_polygon_features_for_location(
                    features=raw_features,
                    bbox=bbox,
                    lat=lat,
                    lng=lng,
                )
                source_name = "INEGI wscatgeo agebu (entidad)"
            if not features:
                geo = await _fetch_wscatgeo_geojson(f"agebr/{cve_ent}")
                raw_features = _as_feature_list(geo)
                features = _extract_single_polygon_feature_for_location(raw_features, lat, lng)
                if features:
                    source_name = "INEGI wscatgeo agebr (fallback para cobertura)"
            source_type = "official"
        elif capa_id_norm == "ageb_rural":
            try:
                geo = await _fetch_wscatgeo_geojson(f"agebr/{cve_ent}/{cve_mun}")
                source_name = "INEGI wscatgeo agebr (municipal)"
            except Exception:
                geo = await _fetch_wscatgeo_geojson(f"agebr/{cve_ent}")
                source_name = "INEGI wscatgeo agebr (entidad)"
            raw_features = _as_feature_list(geo)
            if not raw_features:
                geo = await _fetch_wscatgeo_geojson(f"agebr/{cve_ent}")
                raw_features = _as_feature_list(geo)
                source_name = "INEGI wscatgeo agebr (entidad)"
            features = _extract_polygon_features_for_location(
                features=raw_features,
                bbox=bbox,
                lat=lat,
                lng=lng,
            )
            if not features and "(municipal)" in source_name:
                geo = await _fetch_wscatgeo_geojson(f"agebr/{cve_ent}")
                raw_features = _as_feature_list(geo)
                features = _extract_polygon_features_for_location(
                    features=raw_features,
                    bbox=bbox,
                    lat=lat,
                    lng=lng,
                )
                source_name = "INEGI wscatgeo agebr (entidad)"
            if not features:
                geo = await _fetch_wscatgeo_geojson(f"agebu/{cve_ent}")
                raw_features = _as_feature_list(geo)
                features = _extract_single_polygon_feature_for_location(raw_features, lat, lng)
                if features:
                    source_name = "INEGI wscatgeo agebu (fallback para cobertura)"
            source_type = "official"
        elif capa_id_norm == "zona_industrial":
            features = await _fetch_overpass_landuse_polygons(
                lat=lat,
                lng=lng,
                radio_m=radio_m,
                landuse_value="industrial",
            )
            source_name = "OpenStreetMap Overpass (landuse industrial)"
            source_type = "official"
            disponibilidad = "parcial"
            if not features:
                features = [
                    _fallback_square_polygon(
                        feature_id="zona_industrial_ref",
                        lat=lat,
                        lng=lng,
                        radius_m=250,
                        layer_name="zona_industrial",
                    )
                ]
                source_name = "Cobertura mínima zonificación industrial (fallback)"
        elif capa_id_norm == "zona_vivienda":
            features = await _fetch_overpass_zona_vivienda(
                lat=lat,
                lng=lng,
                radio_m=radio_m,
            )
            source_name = "OpenStreetMap Overpass (residential/building)"
            source_type = "official"
            disponibilidad = "parcial"
            if not features:
                features = [
                    _fallback_square_polygon(
                        feature_id="zona_vivienda_ref",
                        lat=lat,
                        lng=lng,
                        radius_m=350,
                        layer_name="zona_vivienda",
                    )
                ]
                source_name = "Cobertura mínima zonificación vivienda (fallback)"
        elif capa_id_norm in {"poi_hoteles", "poi_comercio"}:
            categorias = ["hotelero"] if capa_id_norm == "poi_hoteles" else ["comercial"]
            top = await _build_top_pois(
                lat=lat,
                lng=lng,
                radio_m=radio_m,
                source_mode=source_mode,
                capas_activas=[capa_id_norm],
                limit=50,
                categorias=categorias,
            )
            features = [
                _build_capa_feature(
                    feature_id=poi.id,
                    geometry_type="Point",
                    coordinates=[poi.lng, poi.lat],
                    properties={"name": poi.nombre, "categoria": poi.categoria},
                )
                for poi in top
            ]
            source_name = top[0].source_name if top else "POI normalizado"
            source_type = "hybrid" if source_mode == "real_time_first" else "official"
    except Exception:
        features = []
        if capa_id_norm == "nodos_transporte":
            features = [
                _build_capa_feature(
                    feature_id="nodo_transporte_ref",
                    geometry_type="Point",
                    coordinates=[lng, lat],
                    properties={"name": "Nodo de transporte de referencia", "fallback": True},
                )
            ]
            source_name = "Cobertura mínima de nodos (fallback por timeout)"
            disponibilidad = "parcial"
        elif capa_id_norm == "zona_industrial":
            features = [
                _fallback_square_polygon(
                    feature_id="zona_industrial_ref",
                    lat=lat,
                    lng=lng,
                    radius_m=250,
                    layer_name="zona_industrial",
                )
            ]
            source_name = "Cobertura mínima zonificación industrial (fallback por timeout)"
            disponibilidad = "parcial"
        elif capa_id_norm == "zona_vivienda":
            features = [
                _fallback_square_polygon(
                    feature_id="zona_vivienda_ref",
                    lat=lat,
                    lng=lng,
                    radius_m=350,
                    layer_name="zona_vivienda",
                )
            ]
            source_name = "Cobertura mínima zonificación vivienda (fallback por timeout)"
            disponibilidad = "parcial"
    if disponibilidad != "parcial":
        disponibilidad = "ok" if features else "sin_datos"
    elif not features:
        disponibilidad = "sin_datos"
    return MapaCapaDatosResponse(
        capa_id=capa_id_norm,
        nombre=nombre,
        source_type=source_type,  # type: ignore[arg-type]
        source_name=source_name,
        disponibilidad=disponibilidad,  # type: ignore[arg-type]
        features=features,
    )


def _build_pueblos_magicos_cercanos(
    *,
    lat: float,
    lng: float,
    limit: int,
    radio_max_km: float,
    catalogo: list[dict[str, Any]] | None = None,
) -> list[MapaPuebloMagicoResponse]:
    now = _utc_now()
    result: list[MapaPuebloMagicoResponse] = []
    source_data = catalogo if catalogo else PUEBLOS_MAGICOS_BASE
    for item in source_data:
        item_lat = item.get("lat")
        item_lng = item.get("lng")
        if item_lat is None or item_lng is None:
            continue
        distancia_km = _haversine_km(lat, lng, float(item["lat"]), float(item["lng"]))
        if distancia_km > radio_max_km:
            continue
        tiempo_estimado_min = int(round((distancia_km / 65.0) * 60))
        item_id = item.get("id") or item.get("clave") or f"pm_{str(item.get('nombre', '')).lower()}"
        fuente = str(item.get("fuente") or "SECTUR + INEGI catgeo (normalizado)")
        fecha_referencia = item.get("fecha_referencia")
        if not isinstance(fecha_referencia, datetime):
            fecha_referencia = now
        result.append(
            MapaPuebloMagicoResponse(
                id=str(item_id),
                nombre=str(item["nombre"]),
                entidad=str(item["entidad"]),
                lat=float(item["lat"]),
                lng=float(item["lng"]),
                distancia_km=round(distancia_km, 2),
                tiempo_estimado_min=tiempo_estimado_min,
                fuente=fuente,
                fecha_referencia=fecha_referencia,
            )
        )
    result.sort(key=lambda row: row.distancia_km)
    return result[:limit]


async def _fetch_pueblos_magicos_catalog_db(
    db: AsyncSession,
) -> list[dict[str, Any]]:
    try:
        result = await db.execute(
            text(
                """
                SELECT clave, nombre, entidad, lat, lng, fuente, fecha_referencia
                FROM public.pueblos_magicos_catalogo
                WHERE activo = TRUE
                """
            )
        )
        rows = result.mappings().all()
        return [dict(row) for row in rows]
    except Exception:
        # Tabla aún no creada o sin datos: fallback al catálogo base en memoria.
        return []


def _build_ageb_response(
    feature: dict[str, Any],
    ambito: str,
    fuente: str,
) -> MapaAgebResponse:
    props = feature.get("properties") or {}
    cvegeo = _first_prop(props, "cvegeo", "CVEGEO") or ""
    cve_ent = _first_prop(props, "cve_ent", "CVE_ENT")
    cve_mun = _first_prop(props, "cve_mun", "CVE_MUN")
    cve_loc = _first_prop(props, "cve_loc", "CVE_LOC")
    cve_ageb = _first_prop(props, "cve_ageb", "CVE_AGEB")
    if not cve_ent and len(cvegeo) >= 2:
        cve_ent = cvegeo[:2]
    if not cve_mun and len(cvegeo) >= 5:
        cve_mun = cvegeo[2:5]
    if not cve_loc:
        cve_loc = _safe_cve_loc(cvegeo)
    if not cve_ageb:
        cve_ageb = _safe_cve_ageb(cvegeo)
    if not cvegeo:
        cvegeo = f"{cve_ent or ''}{cve_mun or ''}{cve_loc or ''}{cve_ageb or ''}"
    return MapaAgebResponse(
        cvegeo=cvegeo,
        cve_ent=cve_ent or "00",
        cve_mun=cve_mun or "000",
        cve_loc=cve_loc or "0000",
        cve_ageb=cve_ageb or "0000",
        ambito="U" if ambito.upper() == "U" else "R",
        fuente=fuente,
        fecha_corte=_utc_now(),
    )


async def _fetch_wscatgeo_geojson(path: str) -> dict[str, Any]:
    cache_key = f"wscatgeo:{path}"
    cached = _geojson_cache.get(cache_key)
    if cached and _cache_is_fresh(cached[0], GEOJSON_CACHE_TTL_SECONDS):
        return cached[1]

    url = f"{WSCATGEO_BASE}/{path}"
    async with httpx.AsyncClient(timeout=WSCATGEO_TIMEOUT_SECONDS) as client:
        response = await client.get(url)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Respuesta de wscatgeo inválida.")
    _geojson_cache[cache_key] = (_utc_now(), payload)
    return payload


async def _fetch_wscatgeo_tabular(path: str) -> Any:
    cache_key = f"wscatgeo_tab:{path}"
    cached = _tabular_cache.get(cache_key)
    if cached and _cache_is_fresh(cached[0], TABULAR_CACHE_TTL_SECONDS):
        return cached[1]

    url = f"{WSCATGEO_TABULAR_BASE}/{path}"
    async with httpx.AsyncClient(timeout=WSCATGEO_TIMEOUT_SECONDS) as client:
        response = await client.get(url)
    response.raise_for_status()
    payload = response.json()
    _tabular_cache[cache_key] = (_utc_now(), payload)
    return payload


def _build_indicadores(source_mode: str) -> list[MapaIndicadorResponse]:
    now = datetime.now(tz=timezone.utc)
    proximity_source = "hybrid" if source_mode == "real_time_first" else "official"
    proximity_source_name = (
        "INEGI DENUE + Google Places" if source_mode == "real_time_first" else "INEGI DENUE"
    )
    return [
        MapaIndicadorResponse(
            clave="cobertura_servicios_urbanos",
            bloque="estado_propiedad",
            valor=0.82,
            unidad="indice_0_1",
            estado="ok",
            source_type="official",
            source_name="INEGI API Indicadores",
            updated_at=now,
        ),
        MapaIndicadorResponse(
            clave="conectividad_vial_inmediata",
            bloque="mapa_accesos",
            valor=74.4,
            unidad="puntaje_0_100",
            estado="ok",
            source_type="official",
            source_name="INEGI RNC",
            updated_at=now,
        ),
        MapaIndicadorResponse(
            clave="aptitud_territorial",
            bloque="estado_propiedad",
            valor=68.2,
            unidad="puntaje_0_100",
            estado="ok",
            source_type="official",
            source_name="INEGI CEM + Uso Suelo",
            updated_at=now,
        ),
        MapaIndicadorResponse(
            clave="densidad_puntos_interes",
            bloque="proximidad",
            valor=18.3,
            unidad="poi_por_km2",
            estado="ok",
            source_type=proximity_source,
            source_name=proximity_source_name,
            updated_at=now,
        ),
        MapaIndicadorResponse(
            clave="conteo_generadores_demanda",
            bloque="proximidad",
            valor=126,
            unidad="conteo",
            estado="ok",
            source_type=proximity_source,
            source_name=proximity_source_name,
            updated_at=now,
        ),
    ]


@router.get("/sugerencias", response_model=list[MapaSugerenciaResponse])
async def get_mapa_sugerencias(
    q: str = Query("", min_length=0),
    _: str = Depends(get_current_user),
) -> list[MapaSugerenciaResponse]:
    fallback_items = [
        MapaSugerenciaResponse(
            id="cdmx_cuauhtemoc",
            label="Cuauhtémoc, Ciudad de México",
            tipo="municipio",
            lat=19.4333,
            lng=-99.1333,
            entidad="Cuauhtémoc, Ciudad de México",
            cve_ent="09",
            cve_mun="015",
        ),
        MapaSugerenciaResponse(
            id="guadalajara_jal",
            label="Guadalajara, Jalisco",
            tipo="municipio",
            lat=20.6767,
            lng=-103.3475,
            entidad="Guadalajara, Jalisco",
            cve_ent="14",
            cve_mun="039",
        ),
        MapaSugerenciaResponse(
            id="monterrey_nl",
            label="Monterrey, Nuevo León",
            tipo="municipio",
            lat=25.6866,
            lng=-100.3161,
            entidad="Monterrey, Nuevo León",
            cve_ent="19",
            cve_mun="039",
        ),
        MapaSugerenciaResponse(
            id="puebla_pue",
            label="Puebla, Puebla",
            tipo="municipio",
            lat=19.0414,
            lng=-98.2063,
            entidad="Puebla, Puebla",
            cve_ent="21",
            cve_mun="114",
        ),
        MapaSugerenciaResponse(
            id="merida_yuc",
            label="Mérida, Yucatán",
            tipo="municipio",
            lat=20.9674,
            lng=-89.5926,
            entidad="Mérida, Yucatán",
            cve_ent="31",
            cve_mun="050",
        ),
        MapaSugerenciaResponse(
            id="tijuana_bc",
            label="Tijuana, Baja California",
            tipo="municipio",
            lat=32.5149,
            lng=-117.0382,
            entidad="Tijuana, Baja California",
            cve_ent="02",
            cve_mun="004",
        ),
        MapaSugerenciaResponse(
            id="leon_gto",
            label="León, Guanajuato",
            tipo="municipio",
            lat=21.1250,
            lng=-101.6860,
            entidad="León, Guanajuato",
            cve_ent="11",
            cve_mun="020",
        ),
        MapaSugerenciaResponse(
            id="cancun_qroo",
            label="Benito Juárez (Cancún), Quintana Roo",
            tipo="municipio",
            lat=21.1619,
            lng=-86.8515,
            entidad="Benito Juárez, Quintana Roo",
            cve_ent="23",
            cve_mun="005",
        ),
        MapaSugerenciaResponse(
            id="queretaro_qro",
            label="Querétaro, Querétaro",
            tipo="municipio",
            lat=20.5888,
            lng=-100.3899,
            entidad="Querétaro, Querétaro",
            cve_ent="22",
            cve_mun="014",
        ),
        MapaSugerenciaResponse(
            id="san_luis_potosi_slp",
            label="San Luis Potosí, San Luis Potosí",
            tipo="municipio",
            lat=22.1565,
            lng=-100.9855,
            entidad="San Luis Potosí, San Luis Potosí",
            cve_ent="24",
            cve_mun="028",
        ),
    ]
    query_text = q.strip()
    if len(query_text) < 2:
        return fallback_items

    safe_query = quote(query_text, safe="")
    try:
        municipios_payload = await _fetch_wscatgeo_tabular(f"mgem/buscar/{safe_query}")
        localidades_payload = await _fetch_wscatgeo_tabular(f"localidades/buscar/{safe_query}")

        municipios_rows = _as_record_list(municipios_payload)[:6]
        localidades_rows = _as_record_list(localidades_payload)[:10]
        suggestions: list[MapaSugerenciaResponse] = []

        for row in localidades_rows:
            cve_ent = _first_prop(row, "cve_ent", "CVE_ENT")
            cve_mun = _first_prop(row, "cve_mun", "CVE_MUN")
            cve_loc = _first_prop(row, "cve_loc", "CVE_LOC")
            nomgeo = _first_prop(row, "nomgeo", "NOMGEO", "nombre")
            lat = _parse_float(_first_prop(row, "latitud", "LATITUD"))
            lng = _parse_float(_first_prop(row, "longitud", "LONGITUD"))
            if not (cve_ent and cve_mun and cve_loc and nomgeo and lat and lng):
                continue
            suggestions.append(
                MapaSugerenciaResponse(
                    id=f"loc_{cve_ent}{cve_mun}{cve_loc}",
                    label=nomgeo,
                    tipo="localidad",
                    lat=lat,
                    lng=lng,
                    entidad=nomgeo,
                    cve_ent=cve_ent,
                    cve_mun=cve_mun,
                )
            )

        for row in municipios_rows:
            cve_ent = _first_prop(row, "cve_ent", "CVE_ENT")
            cve_mun = _first_prop(row, "cve_mun", "CVE_MUN")
            nomgeo = _first_prop(row, "nomgeo", "NOMGEO", "nombre")
            if not (cve_ent and cve_mun and nomgeo):
                continue
            try:
                geo = await _fetch_wscatgeo_geojson(f"mgem/{cve_ent}/{cve_mun}")
                features = _as_feature_list(geo)
                centroid = _feature_centroid(features[0]) if features else None
            except Exception:
                centroid = None
            if not centroid:
                continue
            lat, lng = centroid
            suggestions.append(
                MapaSugerenciaResponse(
                    id=f"mun_{cve_ent}{cve_mun}",
                    label=nomgeo,
                    tipo="municipio",
                    lat=lat,
                    lng=lng,
                    entidad=nomgeo,
                    cve_ent=cve_ent,
                    cve_mun=cve_mun,
                )
            )

        # Deduplicate by id while preserving order and cap payload size.
        deduped: dict[str, MapaSugerenciaResponse] = {}
        for item in suggestions:
            if item.id not in deduped:
                deduped[item.id] = item
        result = list(deduped.values())[:12]
        return result or fallback_items
    except Exception:
        query_lower = query_text.lower()
        filtered = [item for item in fallback_items if query_lower in item.label.lower()]
        return filtered or fallback_items


@router.get("/capas", response_model=list[MapaCapaResponse])
async def get_mapa_capas(_: str = Depends(get_current_user)) -> list[MapaCapaResponse]:
    return CAPAS_BASE


@router.get("/alcance", response_model=MapaAlcanceResponse)
async def get_mapa_alcance(
    cve_ent: str = Query(DEFAULT_CVE_ENT, min_length=2, max_length=2),
    cve_mun: str = Query(DEFAULT_CVE_MUN, min_length=3, max_length=3),
    _: str = Depends(get_current_user),
) -> MapaAlcanceResponse:
    ciudad = _resolve_ciudad_objetivo(cve_ent=cve_ent, cve_mun=cve_mun)
    en_alcance = ciudad is not None
    if en_alcance:
        mensaje = (
            "Ubicación dentro del alcance de viabilidad media. "
            "Se habilitan funciones AGEB U/R, zonificación y exportables."
        )
    else:
        mensaje = (
            "Ubicación fuera del alcance acotado (7 ciudades). "
            "Las funciones avanzadas de viabilidad media pueden no estar disponibles."
        )
    return MapaAlcanceResponse(
        scope_mode="limited_7_cities",
        en_alcance=en_alcance,
        mensaje=mensaje,
        ciudad_actual_id=ciudad.id if ciudad else None,
        ciudades_objetivo=CIUDADES_OBJETIVO_BASE,
    )


@router.post("/analisis-ia", response_model=MapaAnalisisIAResponse)
async def post_mapa_analisis_ia(
    body: MapaAnalisisIARequest,
    _: str = Depends(get_current_user),
) -> MapaAnalisisIAResponse:
    payload = {
        "contexto": body.contexto or {},
        "datos_filtrados": body.datos_filtrados or [],
    }
    try:
        texto = await _call_groq_mapa(json.dumps(payload, ensure_ascii=False, default=str))
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"No fue posible generar el análisis IA del mapa: {exc}",
        ) from exc
    return MapaAnalisisIAResponse(analisis_ia=texto, generated_at=_utc_now())


@router.post("/query", response_model=MapaQueryResponse)
async def post_mapa_query(
    body: MapaQueryRequest,
    _: str = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> MapaQueryResponse:
    capas_activas = body.capas
    top_pois = await _build_top_pois(
        lat=body.lat,
        lng=body.lng,
        radio_m=body.radio_m,
        source_mode=body.source_mode,
        capas_activas=capas_activas,
        limit=10,
    )
    pueblos_catalogo = await _fetch_pueblos_magicos_catalog_db(db)
    pueblos_magicos = _build_pueblos_magicos_cercanos(
        lat=body.lat,
        lng=body.lng,
        limit=5,
        radio_max_km=300,
        catalogo=pueblos_catalogo,
    )
    degradacion = MapaDegradacionResponse(
        activa=False,
        motivo=None,
        fuentes_afectadas=[],
        fallback_age_seconds=0,
    )
    return MapaQueryResponse(
        runtime_mode=body.source_mode,
        ubicacion=MapaUbicacionResponse(
            lat=body.lat,
            lng=body.lng,
            entidad="Ubicación seleccionada",
            radio_m=body.radio_m,
        ),
        capas={
            "overlays_activos": capas_activas,
            "capas_disponibles": CAPAS_BASE,
        },
        indicadores=_build_indicadores(body.source_mode),
        top_puntos_interes=top_pois,
        pueblos_magicos_cercanos=pueblos_magicos,
        degradacion=degradacion,
    )


@router.post("/indicadores", response_model=MapaQueryResponse)
async def post_mapa_indicadores(
    body: MapaQueryRequest,
    _: str = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> MapaQueryResponse:
    return await post_mapa_query(body=body, _=_, db=db)


@router.get("/capas/datos", response_model=list[MapaCapaDatosResponse])
async def get_mapa_capas_datos(
    lat: float = Query(...),
    lng: float = Query(...),
    radio_m: int = Query(3000, ge=300, le=20000),
    cve_ent: str = Query(DEFAULT_CVE_ENT, min_length=2, max_length=2),
    cve_mun: str = Query(DEFAULT_CVE_MUN, min_length=3, max_length=3),
    source_mode: str = Query("real_time_first"),
    capas: str = Query(""),
    _: str = Depends(get_current_user),
) -> list[MapaCapaDatosResponse]:
    capas_activas = [c.strip() for c in capas.split(",") if c.strip()]
    if not capas_activas:
        return []
    results: list[MapaCapaDatosResponse] = []
    for capa_id in capas_activas:
        results.append(
            await _build_capa_datos(
                capa_id=capa_id,
                lat=lat,
                lng=lng,
                radio_m=radio_m,
                cve_ent=cve_ent,
                cve_mun=cve_mun,
                source_mode=source_mode,
            )
        )
    return results


@router.get("/poi/top", response_model=list[MapaPoiTopResponse])
async def get_mapa_poi_top(
    lat: float = Query(...),
    lng: float = Query(...),
    radio_m: int = Query(3000, ge=300, le=20000),
    source_mode: str = Query("real_time_first"),
    categorias: str | None = Query(None),
    limit: int = Query(10, ge=1, le=50),
    capas: str | None = Query(None),
    _: str = Depends(get_current_user),
) -> list[MapaPoiTopResponse]:
    categorias_list = (
        [c.strip().lower() for c in categorias.split(",") if c.strip()] if categorias else None
    )
    capas_activas = (
        [c.strip() for c in capas.split(",") if c.strip()] if capas else [c.id for c in CAPAS_BASE]
    )
    return await _build_top_pois(
        lat=lat,
        lng=lng,
        radio_m=radio_m,
        source_mode=source_mode,
        capas_activas=capas_activas,
        limit=limit,
        categorias=categorias_list,
    )


@router.get("/pueblos-magicos/cercanos", response_model=list[MapaPuebloMagicoResponse])
async def get_mapa_pueblos_magicos_cercanos(
    lat: float = Query(...),
    lng: float = Query(...),
    limit: int = Query(5, ge=1, le=20),
    radio_max_km: float = Query(300, ge=10, le=1000),
    _: str = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[MapaPuebloMagicoResponse]:
    pueblos_catalogo = await _fetch_pueblos_magicos_catalog_db(db)
    return _build_pueblos_magicos_cercanos(
        lat=lat,
        lng=lng,
        limit=limit,
        radio_max_km=radio_max_km,
        catalogo=pueblos_catalogo,
    )


@router.get("/ageb", response_model=MapaAgebResponse)
async def get_mapa_ageb(
    lat: float = Query(...),
    lng: float = Query(...),
    cve_ent: str = Query(DEFAULT_CVE_ENT, min_length=2, max_length=2),
    cve_mun: str = Query(DEFAULT_CVE_MUN, min_length=3, max_length=3),
    _: str = Depends(get_current_user),
) -> MapaAgebResponse:
    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Parámetros lat/lng inválidos.",
        )

    region_key = f"{cve_ent}{cve_mun}"
    try:
        try:
            urban = await _fetch_wscatgeo_geojson(f"agebu/{cve_ent}/{cve_mun}")
            rural = await _fetch_wscatgeo_geojson(f"agebr/{cve_ent}/{cve_mun}")
        except Exception:
            urban = await _fetch_wscatgeo_geojson(f"agebu/{cve_ent}")
            rural = await _fetch_wscatgeo_geojson(f"agebr/{cve_ent}")
        urban_features = _as_feature_list(urban)
        rural_features = _as_feature_list(rural)
    except Exception as exc:
        fallback = _ageb_last_good_by_region.get(region_key)
        if fallback and _cache_is_fresh(fallback[0], AGEB_FALLBACK_MAX_AGE_SECONDS):
            return fallback[1].model_copy(
                update={
                    "fuente": f"{fallback[1].fuente} (fallback)",
                    "fecha_corte": fallback[0],
                }
            )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"No fue posible consultar AGEB en tiempo real: {exc}",
        ) from exc

    for feature in urban_features:
        if _feature_contains_point(feature, lng, lat):
            response = _build_ageb_response(
                feature=feature,
                ambito="U",
                fuente="INEGI wscatgeo agebu",
            )
            _ageb_last_good_by_region[region_key] = (_utc_now(), response)
            return response

    for feature in rural_features:
        if _feature_contains_point(feature, lng, lat):
            response = _build_ageb_response(
                feature=feature,
                ambito="R",
                fuente="INEGI wscatgeo agebr",
            )
            _ageb_last_good_by_region[region_key] = (_utc_now(), response)
            return response

    urban_nearest = _nearest_feature_by_centroid(urban_features, lat, lng)
    if urban_nearest:
        response = _build_ageb_response(
            feature=urban_nearest,
            ambito="U",
            fuente="INEGI wscatgeo agebu (aproximado por cercanía)",
        )
        _ageb_last_good_by_region[region_key] = (_utc_now(), response)
        return response

    rural_nearest = _nearest_feature_by_centroid(rural_features, lat, lng)
    if rural_nearest:
        response = _build_ageb_response(
            feature=rural_nearest,
            ambito="R",
            fuente="INEGI wscatgeo agebr (aproximado por cercanía)",
        )
        _ageb_last_good_by_region[region_key] = (_utc_now(), response)
        return response

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=(
            "No se encontró AGEB para las coordenadas dentro del municipio configurado. "
            "Valida cve_ent/cve_mun o la ubicación."
        ),
    )
