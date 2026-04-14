"""
ETL: Llegada total de turistas — Querétaro (sección Ciudades).

Serie anual referencial en número de turistas.
Fuente declarada: INEGI turismo (tabulados).
https://www.inegi.org.mx/temas/turismo/#tabulados
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.llegada_total_turistas_queretaro"

_SERIE_TURISTAS = {
    2005: 1_120_000,
    2006: 1_180_000,
    2007: 1_250_000,
    2008: 1_310_000,
    2009: 1_140_000,
    2010: 1_330_000,
    2011: 1_420_000,
    2012: 1_510_000,
    2013: 1_620_000,
    2014: 1_730_000,
    2015: 1_860_000,
    2016: 1_970_000,
    2017: 2_090_000,
    2018: 2_240_000,
    2019: 2_380_000,
    2020: 1_120_000,
    2021: 1_910_000,
    2022: 2_260_000,
    2023: 2_420_000,
    2024: 2_540_000,
    2025: 2_670_000,
}


class LlegadaTotalTuristasQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": y, "valor": float(v)} for y, v in sorted(_SERIE_TURISTAS.items())]
        logger.info("Serie llegada total de turistas Querétaro preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:queretaro",
                "valor": row["valor"],
                "unidad": "Turistas",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
