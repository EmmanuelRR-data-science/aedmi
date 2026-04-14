"""
ETL: Crecimiento de habitaciones ocupadas — Querétaro (sección Ciudades).

Serie anual referencial en variación porcentual.
Fuente declarada: Datatur / Sectur.
https://datatur.sectur.gob.mx/SitePages/VisitantesInternacionales.aspx
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.crecimiento_habitaciones_ocupadas_queretaro"

_SERIE_CRECIMIENTO_PCT = {
    2005: 2.1,
    2006: 2.3,
    2007: 2.9,
    2008: 1.8,
    2009: -11.2,
    2010: 12.5,
    2011: 4.9,
    2012: 3.7,
    2013: 3.4,
    2014: 3.8,
    2015: 4.4,
    2016: 2.1,
    2017: 3.2,
    2018: 3.8,
    2019: 3.1,
    2020: -36.0,
    2021: 33.5,
    2022: 9.2,
    2023: 4.5,
    2024: 2.7,
    2025: 1.9,
}


class CrecimientoHabitacionesOcupadasQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": y, "valor": float(v)} for y, v in sorted(_SERIE_CRECIMIENTO_PCT.items())]
        logger.info(
            "Serie crecimiento de habitaciones ocupadas Querétaro preparada: %s registros",
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
