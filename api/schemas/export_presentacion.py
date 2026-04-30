# api/schemas/export_presentacion.py
from typing import Literal

from pydantic import BaseModel, Field, field_validator

MAX_PRESENTACION_LOTE = 30
MAX_PALETA_HEX = 32


class DatoSeriePresentacionItem(BaseModel):
    """Punto de la serie tal como la muestra la app (alineado a DatoIndicador en el front)."""

    periodo: str | int | float = Field(..., description="Período (año, fecha o etiqueta)")
    valor: float
    entidad_clave: str | None = Field(None, description="Opcional; p. ej. serie dentro del mismo indicador")
    unidad: str | None = None

class ExportPresentacionRequest(BaseModel):
    """PPTX: título, indicador, imagen opcional; análisis resuelto en servidor."""

    grafica_id: int = Field(..., description="Id del indicador (mismo que graficaId en el front)")
    titulo: str = Field(..., min_length=1, max_length=500)
    nivel_geografico: str = Field(
        default="nacional",
        max_length=30,
        description="Debe coincidir con public.analisis.nivel_geografico",
    )
    entidad_clave: str | None = None
    imagen_grafica_png_base64: str | None = Field(
        None,
        description="PNG en base64 o data URL; opcional",
    )
    texto_analisis: str | None = Field(
        None,
        max_length=200_000,
        description="Opcional: si se envía, sustituye la lectura de análisis en BD.",
    )
    leyenda_fuente: str | None = Field(
        None,
        max_length=600,
        description="Opcional: leyenda de fuente para el pie del PPTX; si no se envía, se resuelve desde la BD del indicador.",
    )


class ExportPresentacionItem(BaseModel):
    """Una gráfica dentro de un lote."""

    grafica_id: int
    titulo: str = Field(..., min_length=1, max_length=500)
    nivel_geografico: str = Field(..., max_length=30)
    entidad_clave: str | None = None
    imagen_grafica_png_base64: str | None = None
    subtitulo_contexto: str | None = Field(
        None, max_length=200, description="Texto bajo el título (p. ej. entidad)"
    )
    datos_serie: list[DatoSeriePresentacionItem] | None = Field(
        None,
        description="Cifras exactas mostradas en el dashboard; se incrustan en el prompt para Gamma.",
    )
    texto_analisis: str | None = Field(
        None,
        max_length=200_000,
        description="Opcional: cuerpo de análisis tal como en la app; si se envía, sustituye la lectura en BD.",
    )
    leyenda_fuente: str | None = Field(
        None,
        max_length=600,
        description="Texto de fuente de datos (p. ej. pie de gráfica); si no se envía, el API intenta resolverlo desde la BD.",
    )
    excel_chart_kind: Literal["column", "line", "pie", "none"] = Field(
        default="column",
        description="Tipo de gráfico nativo en Excel (MVP).",
    )


class ExportPresentacionLoteRequest(BaseModel):
    """Varias gráficas en un solo PPTX."""

    titulo_presentacion: str = Field(
        default="AEDMI — Selección de gráficas",
        min_length=1,
        max_length=200,
    )
    items: list[ExportPresentacionItem] = Field(
        default_factory=list,
        min_length=1,
    )
    modo_salida: Literal["pptx", "xlsx", "zip_pptx_xlsx"] = Field(
        default="pptx",
        description="pptx: solo PowerPoint; xlsx: solo Excel; zip_pptx_xlsx: ZIP con ambos.",
    )
    paleta_hex: list[str] | None = Field(
        None,
        description=(
            "Colores #RGB/#RRGGBB del panel de estilo del dashboard, en orden "
            "(primario, secundario, …); se envían al prompt de Gamma."
        ),
    )

    @field_validator("paleta_hex")
    @classmethod
    def limitar_paleta(cls, v: list[str] | None) -> list[str] | None:
        if not v:
            return None
        out = [str(x).strip() for x in v[:MAX_PALETA_HEX] if str(x).strip()]
        return out or None


class GammaExportResponse(BaseModel):
    """Resultado de una generación Gamma completada (polling en servidor)."""

    generation_id: str
    gamma_url: str | None = None
    export_url: str | None = None
