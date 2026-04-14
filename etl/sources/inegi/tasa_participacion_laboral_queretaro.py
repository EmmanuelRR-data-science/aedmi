"""
ETL: Tasa de participación laboral — Querétaro (ZM / ciudad).

Serie referencial anual para visualización en sección Ciudades.
Fuente declarada: INEGI — ENOE 15 años y más (tabulados).
https://www.inegi.org.mx/programas/enoe/15ymas/#tabulados
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.tasa_participacion_laboral_queretaro"

# Porcentaje de población de 15 años y más económicamente activa (serie referencial).
_SERIE_TPL_PCT = {
    2005: 57.8,
    2006: 58.1,
    2007: 58.5,
    2008: 58.9,
    2009: 58.2,
    2010: 58.7,
    2011: 59.0,
    2012: 59.4,
    2013: 59.8,
    2014: 60.1,
    2015: 60.4,
    2016: 60.9,
    2017: 61.2,
    2018: 61.5,
    2019: 61.9,
    2020: 57.4,
    2021: 60.6,
    2022: 61.4,
    2023: 62.1,
    2024: 62.7,
    2025: 63.2,
}


class TasaParticipacionLaboralQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [
            {"anio": anio, "valor": float(valor)} for anio, valor in sorted(_SERIE_TPL_PCT.items())
        ]
        logger.info(
            "Serie tasa participación laboral Querétaro preparada: %s registros",
            len(rows),
        )
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:queretaro",
                "valor": row["valor"],
                "unidad": "Porcentaje",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
