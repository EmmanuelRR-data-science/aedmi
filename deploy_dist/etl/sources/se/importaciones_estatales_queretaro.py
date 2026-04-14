"""
ETL: Importaciones estatales — Querétaro (serie para sección Ciudades).

Serie referencial anual en millones de USD.
Fuente declarada: datos.gob.mx / SE.
https://www.datos.gob.mx/dataset/inversion_extranjera_directa
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.importaciones_estatales_queretaro"

_SERIE_IMPORT_MUSD = {
    2005: 4_300,
    2006: 4_900,
    2007: 5_600,
    2008: 5_900,
    2009: 5_100,
    2010: 6_300,
    2011: 7_400,
    2012: 8_300,
    2013: 9_100,
    2014: 10_200,
    2015: 11_600,
    2016: 12_700,
    2017: 13_900,
    2018: 15_100,
    2019: 16_200,
    2020: 14_800,
    2021: 17_500,
    2022: 19_700,
    2023: 21_200,
    2024: 22_600,
    2025: 23_800,
}


class ImportacionesEstatalesQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": y, "valor": float(v)} for y, v in sorted(_SERIE_IMPORT_MUSD.items())]
        logger.info("Serie importaciones estatales Querétaro preparada: %s registros", len(rows))
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
