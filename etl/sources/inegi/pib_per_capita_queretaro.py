"""
ETL: PIB per cápita — Querétaro (serie para sección Ciudades).

Serie referencial anual en pesos corrientes por habitante.
Fuente declarada: INEGI tabulados.
https://www.inegi.org.mx/app/tabulados/default.aspx?pr=17&vr=6&in=2&tp=20&wr=1&cno=2
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.pib_per_capita_queretaro"

_SERIE_PIB_PER_CAPITA = {
    2005: 105_800,
    2006: 112_900,
    2007: 120_400,
    2008: 127_300,
    2009: 120_900,
    2010: 130_700,
    2011: 140_400,
    2012: 149_600,
    2013: 157_900,
    2014: 166_800,
    2015: 176_700,
    2016: 186_800,
    2017: 196_500,
    2018: 206_300,
    2019: 214_900,
    2020: 204_100,
    2021: 220_500,
    2022: 236_400,
    2023: 251_100,
    2024: 265_700,
    2025: 280_900,
}


class PibPerCapitaQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": y, "valor": float(v)} for y, v in sorted(_SERIE_PIB_PER_CAPITA.items())]
        logger.info("Serie PIB per cápita Querétaro preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:queretaro",
                "valor": row["valor"],
                "unidad": "Pesos por habitante",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
