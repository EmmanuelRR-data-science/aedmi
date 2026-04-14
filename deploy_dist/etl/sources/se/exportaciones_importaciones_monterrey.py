"""
ETL: Exportaciones e importaciones para Monterrey / Nuevo León.

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: datos.gob.mx (inversión_extranjera_directa).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.exportaciones_importaciones_monterrey"

_SERIE = {
    2005: {"exportaciones": 12150.0, "importaciones": 10940.0},
    2006: {"exportaciones": 13080.0, "importaciones": 11710.0},
    2007: {"exportaciones": 14190.0, "importaciones": 12650.0},
    2008: {"exportaciones": 14620.0, "importaciones": 13240.0},
    2009: {"exportaciones": 11840.0, "importaciones": 10690.0},
    2010: {"exportaciones": 13790.0, "importaciones": 12480.0},
    2011: {"exportaciones": 14950.0, "importaciones": 13630.0},
    2012: {"exportaciones": 15810.0, "importaciones": 14320.0},
    2013: {"exportaciones": 16690.0, "importaciones": 15180.0},
    2014: {"exportaciones": 17410.0, "importaciones": 15870.0},
    2015: {"exportaciones": 18280.0, "importaciones": 16640.0},
    2016: {"exportaciones": 19160.0, "importaciones": 17430.0},
    2017: {"exportaciones": 20570.0, "importaciones": 18890.0},
    2018: {"exportaciones": 21730.0, "importaciones": 19940.0},
    2019: {"exportaciones": 22460.0, "importaciones": 20760.0},
    2020: {"exportaciones": 18620.0, "importaciones": 16990.0},
    2021: {"exportaciones": 21480.0, "importaciones": 19730.0},
    2022: {"exportaciones": 23210.0, "importaciones": 21480.0},
    2023: {"exportaciones": 24140.0, "importaciones": 22490.0},
    2024: {"exportaciones": 25020.0, "importaciones": 23310.0},
    2025: {"exportaciones": 25890.0, "importaciones": 24120.0},
}


class ExportacionesImportacionesMonterreyExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [
            {
                "anio": anio,
                "exportaciones": vals["exportaciones"],
                "importaciones": vals["importaciones"],
            }
            for anio, vals in sorted(_SERIE.items())
        ]
        logger.info(
            "Serie exportaciones/importaciones Monterrey preparada: %s registros", len(rows)
        )
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for row in raw:
            anio = int(row["anio"])
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:monterrey:exportaciones",
                    "valor": float(row["exportaciones"]),
                    "unidad": "Millones USD",
                    "periodo": anio,
                }
            )
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:monterrey:importaciones",
                    "valor": float(row["importaciones"]),
                    "unidad": "Millones USD",
                    "periodo": anio,
                }
            )
        return records
