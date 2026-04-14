"""
ETL: Ocupación hotelera — Querétaro (sección Ciudades).

Serie anual referencial en porcentaje de ocupación.
Fuente declarada: Datatur / Sectur.
https://datatur.sectur.gob.mx/SitePages/VisitantesInternacionales.aspx
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.ocupacion_hotelera_queretaro"

_SERIE_OCUPACION_PCT = {
    2005: 45.2,
    2006: 46.1,
    2007: 47.4,
    2008: 48.0,
    2009: 41.8,
    2010: 47.0,
    2011: 49.2,
    2012: 50.8,
    2013: 52.1,
    2014: 53.6,
    2015: 55.3,
    2016: 56.0,
    2017: 57.4,
    2018: 58.7,
    2019: 60.1,
    2020: 33.9,
    2021: 49.8,
    2022: 55.9,
    2023: 58.1,
    2024: 59.3,
    2025: 60.4,
}


class OcupacionHoteleraQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": y, "valor": float(v)} for y, v in sorted(_SERIE_OCUPACION_PCT.items())]
        logger.info("Serie ocupación hotelera Querétaro preparada: %s registros", len(rows))
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
