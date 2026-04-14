# api/schemas/indicador.py
from pydantic import BaseModel


class IndicadorResponse(BaseModel):
    id: int
    clave: str
    nombre: str
    categoria: str
    nivel_geografico: str
    unidad: str | None
    descripcion: str | None
    tipo_grafica: str

    model_config = {"from_attributes": True}


class DatoIndicadorResponse(BaseModel):
    indicador_id: int
    nivel_geografico: str
    entidad_clave: str | None
    valor: float | None
    unidad: str | None
    periodo: int | None = None
    anio: int | None = None
    mes: int | None = None
    fecha: str | None = None
