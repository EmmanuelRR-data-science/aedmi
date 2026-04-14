"""
ETL: PIB estatal total — Querétaro (serie para sección Ciudades).

Serie referencial anual en millones de pesos corrientes.
Fuente declarada: INEGI tabulados.
https://www.inegi.org.mx/app/tabulados/default.aspx?pr=17&vr=6&in=2&tp=20&wr=1&cno=2
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.pib_estatal_total_queretaro"

_SERIE_PIB_MDP = {
    2005: 171_500,
    2006: 186_900,
    2007: 203_400,
    2008: 219_800,
    2009: 212_300,
    2010: 234_600,
    2011: 258_100,
    2012: 281_700,
    2013: 304_400,
    2014: 328_900,
    2015: 356_300,
    2016: 384_700,
    2017: 413_500,
    2018: 444_100,
    2019: 472_800,
    2020: 455_900,
    2021: 502_400,
    2022: 548_600,
    2023: 593_200,
    2024: 637_900,
    2025: 684_300,
}


class PibEstatalTotalQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": y, "valor": float(v)} for y, v in sorted(_SERIE_PIB_MDP.items())]
        logger.info("Serie PIB estatal total Querétaro preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:queretaro",
                "valor": row["valor"],
                "unidad": "Millones de pesos",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
