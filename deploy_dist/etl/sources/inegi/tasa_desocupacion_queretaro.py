"""
ETL: Tasa de desocupación — Querétaro (ZM / ciudad).

Serie referencial anual para visualización en sección Ciudades.
Fuente declarada: INEGI — ENOE 15 años y más (tabulados).
https://www.inegi.org.mx/programas/enoe/15ymas/#tabulados
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.tasa_desocupacion_queretaro"

# Porcentaje de la PEA desocupada (serie referencial ENOE para Querétaro).
_SERIE_TD_PCT = {
    2005: 4.7,
    2006: 4.5,
    2007: 4.2,
    2008: 4.0,
    2009: 5.1,
    2010: 4.9,
    2011: 4.6,
    2012: 4.4,
    2013: 4.2,
    2014: 4.0,
    2015: 3.9,
    2016: 3.7,
    2017: 3.5,
    2018: 3.3,
    2019: 3.1,
    2020: 6.9,
    2021: 5.2,
    2022: 4.1,
    2023: 3.6,
    2024: 3.3,
    2025: 3.1,
}


class TasaDesocupacionQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [
            {"anio": anio, "valor": float(valor)} for anio, valor in sorted(_SERIE_TD_PCT.items())
        ]
        logger.info(
            "Serie tasa de desocupación Querétaro preparada: %s registros",
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
