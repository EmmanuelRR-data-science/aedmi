"""
ETL: Gasto promedio diario del visitante con pernocta en Mérida.

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: Observatur Yucatán.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.gasto_promedio_diario_pernocta_merida"

_SERIE_MXN = {
    2005: 1120.0,
    2006: 1165.0,
    2007: 1210.0,
    2008: 1260.0,
    2009: 1185.0,
    2010: 1245.0,
    2011: 1290.0,
    2012: 1355.0,
    2013: 1420.0,
    2014: 1490.0,
    2015: 1575.0,
    2016: 1650.0,
    2017: 1735.0,
    2018: 1820.0,
    2019: 1915.0,
    2020: 1590.0,
    2021: 1710.0,
    2022: 1880.0,
    2023: 2015.0,
    2024: 2140.0,
    2025: 2275.0,
}


class GastoPromedioDiarioPernoctaMeridaExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": anio, "gasto": valor} for anio, valor in sorted(_SERIE_MXN.items())]
        logger.info("Serie gasto promedio diario Mérida preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:merida",
                "valor": float(row["gasto"]),
                "unidad": "MXN",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
