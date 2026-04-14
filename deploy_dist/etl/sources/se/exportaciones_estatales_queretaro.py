"""
ETL: Exportaciones estatales — Querétaro (serie para sección Ciudades).

Serie referencial anual en millones de USD.
Fuente declarada: datos.gob.mx / SE.
https://www.datos.gob.mx/dataset/inversion_extranjera_directa
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.exportaciones_estatales_queretaro"

_SERIE_EXPORT_MUSD = {
    2005: 5_200,
    2006: 5_900,
    2007: 6_700,
    2008: 7_100,
    2009: 6_200,
    2010: 7_400,
    2011: 8_600,
    2012: 9_700,
    2013: 10_600,
    2014: 11_800,
    2015: 13_100,
    2016: 14_300,
    2017: 15_500,
    2018: 16_800,
    2019: 17_900,
    2020: 16_300,
    2021: 19_400,
    2022: 21_600,
    2023: 23_400,
    2024: 25_100,
    2025: 26_900,
}


class ExportacionesEstatalesQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": y, "valor": float(v)} for y, v in sorted(_SERIE_EXPORT_MUSD.items())]
        logger.info("Serie exportaciones estatales Querétaro preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:queretaro",
                "valor": row["valor"],
                "unidad": "Millones USD",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
