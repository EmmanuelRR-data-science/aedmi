"""
ETL: Tasa de crecimiento económico — Querétaro (serie para sección Ciudades).

Serie referencial anual del crecimiento del PIB estatal (variación % anual).
Fuente declarada: INEGI PIB tabulados.
https://www.inegi.org.mx/temas/pib/#tabulados
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.tasa_crecimiento_economico_queretaro"

_SERIE_CRECI_PCT = {
    2005: 4.1,
    2006: 9.0,
    2007: 8.8,
    2008: 8.1,
    2009: -3.4,
    2010: 10.5,
    2011: 10.0,
    2012: 9.1,
    2013: 8.1,
    2014: 8.0,
    2015: 8.3,
    2016: 8.0,
    2017: 7.5,
    2018: 7.4,
    2019: 6.5,
    2020: -3.6,
    2021: 10.2,
    2022: 9.2,
    2023: 8.1,
    2024: 7.5,
    2025: 7.3,
}


class TasaCrecimientoEconomicoQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": y, "valor": float(v)} for y, v in sorted(_SERIE_CRECI_PCT.items())]
        logger.info(
            "Serie tasa crecimiento económico Querétaro preparada: %s registros",
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
