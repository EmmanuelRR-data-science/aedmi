from datetime import datetime

from pydantic import BaseModel, ConfigDict, field_validator


class FuenteDatos(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    nombre: str
    url_referencia: str | None = None
    periodicidad: str
    ultima_carga: datetime | None = None
    modulo_etl: str
    estado: str = "pendiente"
    activo: bool = True
    notas: str | None = None

    @field_validator("periodicidad")
    @classmethod
    def validate_periodicidad(cls, v: str) -> str:
        allowed = {"diario", "semanal", "mensual", "anual", "quinquenal", "otra"}
        if v not in allowed:
            raise ValueError(f"periodicidad debe ser uno de: {allowed}")
        return v

    @field_validator("estado")
    @classmethod
    def validate_estado(cls, v: str) -> str:
        allowed = {"pendiente", "etl_listo", "api_lista", "grafica_lista", "completo"}
        if v not in allowed:
            raise ValueError(f"estado debe ser uno de: {allowed}")
        return v


class Indicador(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    clave: str
    nombre: str
    categoria: str
    nivel_geografico: str
    unidad: str | None = None
    fuente_id: int | None = None
    descripcion: str | None = None
    tipo_grafica: str = "bar"
    activo: bool = True

    @field_validator("categoria")
    @classmethod
    def validate_categoria(cls, v: str) -> str:
        allowed = {"demografia", "economia", "turismo", "conectividad_aerea"}
        if v not in allowed:
            raise ValueError(f"categoria debe ser uno de: {allowed}")
        return v

    @field_validator("nivel_geografico")
    @classmethod
    def validate_nivel(cls, v: str) -> str:
        allowed = {"nacional", "estatal", "municipal", "localidad", "ciudad"}
        if v not in allowed:
            raise ValueError(f"nivel_geografico debe ser uno de: {allowed}")
        return v


class RegistroDato(BaseModel):
    indicador_id: int
    nivel_geografico: str
    entidad_clave: str | None = None
    valor: float | None = None
    unidad: str | None = None


class ETLLog(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    fuente_id: int | None = None
    tipo_ejecucion: str
    inicio: datetime
    fin: datetime | None = None
    exitoso: bool | None = None
    registros_cargados: int = 0
    errores: int = 0
    mensaje: str | None = None
    usuario: str | None = None
