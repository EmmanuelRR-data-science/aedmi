"""
ETL: Ocupación hotelera promedio en Mérida.

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: Observatur Yucatán.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.ocupacion_hotelera_promedio_merida"

_SERIE_PORCENTAJE = {
    2005: 48.2,
    2006: 49.7,
    2007: 51.6,
    2008: 52.1,
    2009: 46.8,
    2010: 50.4,
    2011: 52.8,
    2012: 54.7,
    2013: 56.2,
    2014: 58.1,
    2015: 60.4,
    2016: 62.3,
    2017: 64.1,
    2018: 66.0,
    2019: 67.4,
    2020: 34.5,
    2021: 46.2,
    2022: 58.6,
    2023: 63.1,
    2024: 65.3,
    2025: 66.8,
}


class OcupacionHoteleraPromedioMeridaExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [
            {"anio": anio, "porcentaje": valor} for anio, valor in sorted(_SERIE_PORCENTAJE.items())
        ]
        logger.info("Serie ocupación hotelera Mérida preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:merida",
                "valor": float(row["porcentaje"]),
                "unidad": "Porcentaje",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
