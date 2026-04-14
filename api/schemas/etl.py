# api/schemas/etl.py
from datetime import datetime
from typing import Any

from pydantic import BaseModel


class ModuloETLResponse(BaseModel):
    id: int
    nombre: str
    fuente: str
    periodicidad: str
    ultima_ejecucion: datetime | None
    exitoso: bool | None
    estado: str


class ETLLogResponse(BaseModel):
    id: int
    fuente_id: int | None
    fuente_nombre: str | None
    tipo_ejecucion: str
    inicio: datetime
    fin: datetime | None
    exitoso: bool | None
    registros_cargados: int
    errores: int
    mensaje: str | None
    usuario: str | None


class UploadPreviewResponse(BaseModel):
    columnas_detectadas: list[str]
    filas_preview: list[dict[str, Any]]
    total_filas: int
    formato: str
    hay_diferencias: bool
    columnas_faltantes: list[str]
    columnas_nuevas: list[str]


class EjecucionManualRequest(BaseModel):
    fuente_id: int
    usuario: str | None = None
