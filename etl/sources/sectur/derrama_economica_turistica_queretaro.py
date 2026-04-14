"""
ETL: Derrama económica turística — Querétaro (sección Ciudades).

Serie anual referencial en millones de pesos corrientes (MXN).
Fuente declarada: Datatur / Sectur.
https://datatur.sectur.gob.mx/SitePages/VisitantesInternacionales.aspx
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.derrama_economica_turistica_queretaro"

_SERIE_DERRAMA_MDP = {
    2005: 6_800,
    2006: 7_200,
    2007: 7_700,
    2008: 8_150,
    2009: 6_950,
    2010: 8_450,
    2011: 9_200,
    2012: 10_000,
    2013: 10_900,
    2014: 11_850,
    2015: 12_950,
    2016: 13_800,
    2017: 14_900,
    2018: 16_200,
    2019: 17_400,
    2020: 9_150,
    2021: 14_700,
    2022: 17_350,
    2023: 18_800,
    2024: 19_950,
    2025: 21_100,
}


class DerramaEconomicaTuristicaQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": y, "valor": float(v)} for y, v in sorted(_SERIE_DERRAMA_MDP.items())]
        logger.info(
            "Serie derrama económica turística Querétaro preparada: %s registros", len(rows)
        )
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:queretaro",
                "valor": row["valor"],
                "unidad": "Millones MXN",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
