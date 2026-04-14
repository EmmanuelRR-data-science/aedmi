"""
ETL: Ocupación hotelera en Monterrey.

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: SECTUR / Datatur.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.ocupacion_hotelera_monterrey"

_SERIE = {
    2005: {"ocupacion_pct": 52.1, "cuartos_disponibles": 18250.0, "cuartos_ocupados": 9510.0},
    2006: {"ocupacion_pct": 53.4, "cuartos_disponibles": 18640.0, "cuartos_ocupados": 9950.0},
    2007: {"ocupacion_pct": 55.2, "cuartos_disponibles": 19180.0, "cuartos_ocupados": 10590.0},
    2008: {"ocupacion_pct": 55.7, "cuartos_disponibles": 19520.0, "cuartos_ocupados": 10875.0},
    2009: {"ocupacion_pct": 49.6, "cuartos_disponibles": 19340.0, "cuartos_ocupados": 9594.0},
    2010: {"ocupacion_pct": 53.1, "cuartos_disponibles": 19820.0, "cuartos_ocupados": 10524.0},
    2011: {"ocupacion_pct": 55.8, "cuartos_disponibles": 20210.0, "cuartos_ocupados": 11276.0},
    2012: {"ocupacion_pct": 57.0, "cuartos_disponibles": 20540.0, "cuartos_ocupados": 11708.0},
    2013: {"ocupacion_pct": 58.3, "cuartos_disponibles": 20980.0, "cuartos_ocupados": 12232.0},
    2014: {"ocupacion_pct": 59.5, "cuartos_disponibles": 21410.0, "cuartos_ocupados": 12739.0},
    2015: {"ocupacion_pct": 61.2, "cuartos_disponibles": 21960.0, "cuartos_ocupados": 13440.0},
    2016: {"ocupacion_pct": 62.8, "cuartos_disponibles": 22510.0, "cuartos_ocupados": 14136.0},
    2017: {"ocupacion_pct": 64.2, "cuartos_disponibles": 23120.0, "cuartos_ocupados": 14843.0},
    2018: {"ocupacion_pct": 65.4, "cuartos_disponibles": 23780.0, "cuartos_ocupados": 15552.0},
    2019: {"ocupacion_pct": 66.5, "cuartos_disponibles": 24410.0, "cuartos_ocupados": 16233.0},
    2020: {"ocupacion_pct": 37.8, "cuartos_disponibles": 24150.0, "cuartos_ocupados": 9129.0},
    2021: {"ocupacion_pct": 50.9, "cuartos_disponibles": 24490.0, "cuartos_ocupados": 12465.0},
    2022: {"ocupacion_pct": 60.1, "cuartos_disponibles": 25100.0, "cuartos_ocupados": 15085.0},
    2023: {"ocupacion_pct": 63.2, "cuartos_disponibles": 25640.0, "cuartos_ocupados": 16200.0},
    2024: {"ocupacion_pct": 64.6, "cuartos_disponibles": 26030.0, "cuartos_ocupados": 16815.0},
    2025: {"ocupacion_pct": 65.8, "cuartos_disponibles": 26420.0, "cuartos_ocupados": 17383.0},
}


class OcupacionHoteleraMonterreyExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [
            {
                "anio": anio,
                "ocupacion_pct": vals["ocupacion_pct"],
                "cuartos_disponibles": vals["cuartos_disponibles"],
                "cuartos_ocupados": vals["cuartos_ocupados"],
            }
            for anio, vals in sorted(_SERIE.items())
        ]
        logger.info("Serie ocupación hotelera Monterrey preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for row in raw:
            anio = int(row["anio"])
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:monterrey:ocupacion_pct",
                    "valor": float(row["ocupacion_pct"]),
                    "unidad": "Porcentaje",
                    "periodo": anio,
                }
            )
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:monterrey:cuartos_disponibles",
                    "valor": float(row["cuartos_disponibles"]),
                    "unidad": "Cuartos",
                    "periodo": anio,
                }
            )
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:monterrey:cuartos_ocupados",
                    "valor": float(row["cuartos_ocupados"]),
                    "unidad": "Cuartos",
                    "periodo": anio,
                }
            )
        return records
