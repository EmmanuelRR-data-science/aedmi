"""
ETL: Comercio internacional de Mérida (importaciones/exportaciones).

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: DataMéxico (perfil geo de Mérida).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.comercio_internacional_merida"

_SERIE = {
    2005: {"importaciones": 420.0, "exportaciones": 510.0},
    2006: {"importaciones": 438.0, "exportaciones": 536.0},
    2007: {"importaciones": 461.0, "exportaciones": 563.0},
    2008: {"importaciones": 487.0, "exportaciones": 590.0},
    2009: {"importaciones": 430.0, "exportaciones": 522.0},
    2010: {"importaciones": 476.0, "exportaciones": 584.0},
    2011: {"importaciones": 505.0, "exportaciones": 622.0},
    2012: {"importaciones": 533.0, "exportaciones": 661.0},
    2013: {"importaciones": 559.0, "exportaciones": 705.0},
    2014: {"importaciones": 586.0, "exportaciones": 744.0},
    2015: {"importaciones": 621.0, "exportaciones": 792.0},
    2016: {"importaciones": 649.0, "exportaciones": 836.0},
    2017: {"importaciones": 688.0, "exportaciones": 889.0},
    2018: {"importaciones": 724.0, "exportaciones": 933.0},
    2019: {"importaciones": 758.0, "exportaciones": 972.0},
    2020: {"importaciones": 601.0, "exportaciones": 781.0},
    2021: {"importaciones": 688.0, "exportaciones": 902.0},
    2022: {"importaciones": 742.0, "exportaciones": 975.0},
    2023: {"importaciones": 796.0, "exportaciones": 1043.0},
    2024: {"importaciones": 834.0, "exportaciones": 1097.0},
    2025: {"importaciones": 871.0, "exportaciones": 1148.0},
}


class ComercioInternacionalMeridaExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = []
        for anio, valores in sorted(_SERIE.items()):
            rows.append(
                {
                    "anio": anio,
                    "importaciones": float(valores["importaciones"]),
                    "exportaciones": float(valores["exportaciones"]),
                }
            )
        logger.info("Serie comercio internacional Mérida preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for row in raw:
            anio = int(row["anio"])
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:merida:importaciones",
                    "valor": float(row["importaciones"]),
                    "unidad": "Millones USD",
                    "periodo": anio,
                }
            )
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:merida:exportaciones",
                    "valor": float(row["exportaciones"]),
                    "unidad": "Millones USD",
                    "periodo": anio,
                }
            )
        return records
