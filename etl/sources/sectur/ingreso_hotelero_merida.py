"""
ETL: Ingreso hotelero en Mérida.

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: Observatur Yucatán.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.ingreso_hotelero_merida"

_SERIE_MILLONES_MXN = {
    2005: 684.0,
    2006: 742.0,
    2007: 803.0,
    2008: 861.0,
    2009: 721.0,
    2010: 874.0,
    2011: 949.0,
    2012: 1056.0,
    2013: 1165.0,
    2014: 1288.0,
    2015: 1437.0,
    2016: 1596.0,
    2017: 1789.0,
    2018: 2002.0,
    2019: 2234.0,
    2020: 792.0,
    2021: 1243.0,
    2022: 1968.0,
    2023: 2267.0,
    2024: 2541.0,
    2025: 2849.0,
}


class IngresoHoteleroMeridaExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [
            {"anio": anio, "ingreso": valor} for anio, valor in sorted(_SERIE_MILLONES_MXN.items())
        ]
        logger.info("Serie ingreso hotelero Mérida preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:merida",
                "valor": float(row["ingreso"]),
                "unidad": "Millones MXN",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
