"""
ETL: Derrama económica estimada de visitantes con pernocta en Mérida.

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: Observatur Yucatán.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.derrama_economica_pernocta_merida"

_SERIE_MILLONES_MXN = {
    2005: 926.0,
    2006: 1009.0,
    2007: 1093.0,
    2008: 1172.0,
    2009: 986.0,
    2010: 1187.0,
    2011: 1291.0,
    2012: 1427.0,
    2013: 1575.0,
    2014: 1742.0,
    2015: 1947.0,
    2016: 2150.0,
    2017: 2410.0,
    2018: 2688.0,
    2019: 2993.0,
    2020: 1044.0,
    2021: 1708.0,
    2022: 2745.0,
    2023: 3152.0,
    2024: 3548.0,
    2025: 3967.0,
}


class DerramaEconomicaPernoctaMeridaExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [
            {"anio": anio, "derrama": valor} for anio, valor in sorted(_SERIE_MILLONES_MXN.items())
        ]
        logger.info("Serie derrama económica Mérida preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:merida",
                "valor": float(row["derrama"]),
                "unidad": "Millones MXN",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
