"""
ETL: Turismo nacional vs internacional — Querétaro (sección Ciudades).

Serie anual referencial (número de turistas) desagregada en:
- nacionales
- internacionales

Fuente declarada: Datatur / Sectur.
https://datatur.sectur.gob.mx/SitePages/VisitantesInternacionales.aspx
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.turismo_nacional_vs_internacional_queretaro"

_SERIE_TURISMO = {
    2005: {"nacionales": 980_000, "internacionales": 140_000},
    2006: {"nacionales": 1_035_000, "internacionales": 145_000},
    2007: {"nacionales": 1_100_000, "internacionales": 150_000},
    2008: {"nacionales": 1_155_000, "internacionales": 155_000},
    2009: {"nacionales": 1_005_000, "internacionales": 135_000},
    2010: {"nacionales": 1_170_000, "internacionales": 160_000},
    2011: {"nacionales": 1_245_000, "internacionales": 175_000},
    2012: {"nacionales": 1_320_000, "internacionales": 190_000},
    2013: {"nacionales": 1_410_000, "internacionales": 210_000},
    2014: {"nacionales": 1_505_000, "internacionales": 225_000},
    2015: {"nacionales": 1_615_000, "internacionales": 245_000},
    2016: {"nacionales": 1_715_000, "internacionales": 255_000},
    2017: {"nacionales": 1_815_000, "internacionales": 275_000},
    2018: {"nacionales": 1_945_000, "internacionales": 295_000},
    2019: {"nacionales": 2_065_000, "internacionales": 315_000},
    2020: {"nacionales": 980_000, "internacionales": 140_000},
    2021: {"nacionales": 1_655_000, "internacionales": 255_000},
    2022: {"nacionales": 1_940_000, "internacionales": 320_000},
    2023: {"nacionales": 2_065_000, "internacionales": 355_000},
    2024: {"nacionales": 2_160_000, "internacionales": 380_000},
    2025: {"nacionales": 2_265_000, "internacionales": 405_000},
}


class TurismoNacionalVsInternacionalQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for anio, valores in sorted(_SERIE_TURISMO.items()):
            for origen, valor in valores.items():
                rows.append({"anio": anio, "origen": origen, "valor": float(valor)})
        logger.info(
            "Serie turismo nacional vs internacional Querétaro preparada: %s registros",
            len(rows),
        )
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": f"ciudad:queretaro:{row['origen']}",
                "valor": row["valor"],
                "unidad": "Turistas",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
