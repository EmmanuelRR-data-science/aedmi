"""
ETL: Establecimientos de servicios turísticos en Yucatán.

Serie referencial para desarrollo de visualización en sección Ciudades (Mérida).
Fuente declarada: Observatur Yucatán.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.establecimientos_servicios_turisticos_yucatan"

_SERIE = {
    2005: 1240,
    2006: 1298,
    2007: 1365,
    2008: 1422,
    2009: 1376,
    2010: 1468,
    2011: 1549,
    2012: 1643,
    2013: 1741,
    2014: 1837,
    2015: 1954,
    2016: 2062,
    2017: 2188,
    2018: 2320,
    2019: 2469,
    2020: 2146,
    2021: 2288,
    2022: 2455,
    2023: 2592,
    2024: 2724,
    2025: 2868,
}


class EstablecimientosServiciosTuristicosYucatanExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [
            {"anio": anio, "establecimientos": float(valor)}
            for anio, valor in sorted(_SERIE.items())
        ]
        logger.info("Serie establecimientos turísticos Yucatán preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:merida",
                "valor": float(row["establecimientos"]),
                "unidad": "Establecimientos",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
