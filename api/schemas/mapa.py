from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

RuntimeMode = Literal["real_time_first", "official_only"]
SourceType = Literal["official", "google_places", "hybrid"]
IndicadorEstado = Literal["ok", "degradado", "sin_datos"]
BloqueMapa = Literal["estado_propiedad", "proximidad", "mapa_accesos"]


class MapaSugerenciaResponse(BaseModel):
    id: str
    label: str
    tipo: Literal["municipio", "localidad"]
    lat: float
    lng: float
    entidad: str
    cve_ent: str
    cve_mun: str


class MapaCiudadObjetivoResponse(BaseModel):
    id: str
    nombre: str
    entidad: str
    cve_ent: str
    cve_mun: str
    lat: float
    lng: float


class MapaAlcanceResponse(BaseModel):
    scope_mode: Literal["limited_7_cities"]
    en_alcance: bool
    mensaje: str
    ciudad_actual_id: str | None = None
    ciudades_objetivo: list[MapaCiudadObjetivoResponse] = Field(default_factory=list)


class MapaCapaResponse(BaseModel):
    id: str
    nombre: str
    categoria: str
    source_type: SourceType
    disponible: bool = True


class MapaIndicadorResponse(BaseModel):
    clave: str
    bloque: BloqueMapa
    valor: float | None
    unidad: str
    estado: IndicadorEstado
    source_type: SourceType
    source_name: str
    updated_at: datetime


class MapaPoiTopResponse(BaseModel):
    id: str
    nombre: str
    categoria: Literal[
        "hotelero",
        "comercial",
        "salud",
        "educacion",
        "transporte",
        "turistico",
    ]
    lat: float
    lng: float
    distancia_m: float
    source_type: SourceType
    source_name: str
    updated_at: datetime


class MapaPuebloMagicoResponse(BaseModel):
    id: str
    nombre: str
    entidad: str
    lat: float
    lng: float
    distancia_km: float
    tiempo_estimado_min: int | None = None
    fuente: str
    fecha_referencia: datetime


class MapaCapaFeatureResponse(BaseModel):
    id: str
    geometry_type: Literal[
        "Point",
        "LineString",
        "Polygon",
        "MultiLineString",
        "MultiPolygon",
    ]
    coordinates: list
    properties: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class MapaCapaDatosResponse(BaseModel):
    capa_id: str
    nombre: str
    source_type: SourceType
    source_name: str
    disponibilidad: Literal["ok", "parcial", "sin_datos"] = "sin_datos"
    features: list[MapaCapaFeatureResponse] = Field(default_factory=list)


class MapaDegradacionResponse(BaseModel):
    activa: bool
    motivo: str | None = None
    fuentes_afectadas: list[str] = Field(default_factory=list)
    fallback_age_seconds: int = 0


class MapaUbicacionResponse(BaseModel):
    lat: float
    lng: float
    entidad: str
    radio_m: int


class MapaCapasActivasResponse(BaseModel):
    overlays_activos: list[str]
    capas_disponibles: list[MapaCapaResponse]


class MapaQueryRequest(BaseModel):
    lat: float
    lng: float
    radio_m: int = 3000
    source_mode: RuntimeMode = "real_time_first"
    capas: list[str] = Field(default_factory=list)


class MapaQueryResponse(BaseModel):
    runtime_mode: RuntimeMode
    ubicacion: MapaUbicacionResponse
    capas: MapaCapasActivasResponse
    indicadores: list[MapaIndicadorResponse]
    top_puntos_interes: list[MapaPoiTopResponse] = Field(default_factory=list)
    pueblos_magicos_cercanos: list[MapaPuebloMagicoResponse] = Field(default_factory=list)
    degradacion: MapaDegradacionResponse


class MapaAgebResponse(BaseModel):
    cvegeo: str
    cve_ent: str
    cve_mun: str
    cve_loc: str
    cve_ageb: str
    ambito: Literal["U", "R"]
    fuente: str
    fecha_corte: datetime


class MapaAnalisisIARequest(BaseModel):
    contexto: dict[str, Any] | None = None
    datos_filtrados: list[dict[str, Any]] | None = None


class MapaAnalisisIAResponse(BaseModel):
    analisis_ia: str
    generated_at: datetime
