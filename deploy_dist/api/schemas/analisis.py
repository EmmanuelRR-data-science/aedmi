# api/schemas/analisis.py
from datetime import datetime
from typing import Any

from pydantic import BaseModel


class AnalisisResponse(BaseModel):
    id: int
    indicador_id: int
    nivel_geografico: str
    entidad_clave: str | None
    analisis_ia: str | None
    analisis_revisado: str | None
    ia_generado_at: datetime | None
    revisado_at: datetime | None

    model_config = {"from_attributes": True}


class AnalisisRevisadoRequest(BaseModel):
    texto: str


class AnalisisIARequest(BaseModel):
    contexto: dict[str, Any] | None = None
    datos_filtrados: list[dict[str, Any]] | None = None
