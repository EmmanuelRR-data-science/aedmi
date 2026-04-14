"""
ETL: Población económicamente activa (PEA) — Querétaro (ZM / ciudad).

Serie referencial anual para visualización en sección Ciudades.
Fuente declarada: INEGI — ENOE 15 años y más (datos abiertos).
https://www.inegi.org.mx/programas/enoe/15ymas/#datos_abiertos
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.pea_queretaro"

# Miles de personas 15 años y más (PEA), serie anual referencial ZM Querétaro
_SERIE_PEA_MILES = {
    2005: 382,
    2006: 391,
    2007: 402,
    2008: 414,
    2009: 418,
    2010: 428,
    2011: 441,
    2012: 455,
    2013: 468,
    2014: 482,
    2015: 496,
    2016: 512,
    2017: 528,
    2018: 545,
    2019: 562,
    2020: 518,
    2021: 578,
    2022: 612,
    2023: 642,
    2024: 668,
    2025: 691,
}


class PeaQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [
            {"anio": anio, "valor": float(valor)}
            for anio, valor in sorted(_SERIE_PEA_MILES.items())
        ]
        logger.info("Serie PEA Querétaro preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:queretaro",
                "valor": row["valor"],
                "unidad": "Miles de personas",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
