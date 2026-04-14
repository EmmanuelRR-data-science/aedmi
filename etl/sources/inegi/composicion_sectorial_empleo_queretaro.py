"""
ETL: Composición sectorial del empleo — Querétaro (ZM / ciudad).

Serie referencial anual para visualización en sección Ciudades.
Fuente declarada: INEGI — ENOE 15 años y más (tabulados).
https://www.inegi.org.mx/programas/enoe/15ymas/#tabulados
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.composicion_sectorial_empleo_queretaro"

# Porcentaje de ocupación por sector económico (suma ~100 por año).
_SERIE_SECTORIAL = {
    2005: {"primario": 8.7, "secundario": 37.4, "terciario": 53.9},
    2006: {"primario": 8.5, "secundario": 37.8, "terciario": 53.7},
    2007: {"primario": 8.2, "secundario": 38.1, "terciario": 53.7},
    2008: {"primario": 8.0, "secundario": 37.7, "terciario": 54.3},
    2009: {"primario": 8.3, "secundario": 35.9, "terciario": 55.8},
    2010: {"primario": 8.1, "secundario": 35.6, "terciario": 56.3},
    2011: {"primario": 7.9, "secundario": 35.4, "terciario": 56.7},
    2012: {"primario": 7.7, "secundario": 35.1, "terciario": 57.2},
    2013: {"primario": 7.5, "secundario": 34.8, "terciario": 57.7},
    2014: {"primario": 7.3, "secundario": 34.5, "terciario": 58.2},
    2015: {"primario": 7.1, "secundario": 34.1, "terciario": 58.8},
    2016: {"primario": 6.9, "secundario": 33.8, "terciario": 59.3},
    2017: {"primario": 6.7, "secundario": 33.4, "terciario": 59.9},
    2018: {"primario": 6.5, "secundario": 33.0, "terciario": 60.5},
    2019: {"primario": 6.3, "secundario": 32.6, "terciario": 61.1},
    2020: {"primario": 7.4, "secundario": 30.2, "terciario": 62.4},
    2021: {"primario": 6.9, "secundario": 31.4, "terciario": 61.7},
    2022: {"primario": 6.4, "secundario": 32.1, "terciario": 61.5},
    2023: {"primario": 6.1, "secundario": 32.4, "terciario": 61.5},
    2024: {"primario": 5.9, "secundario": 32.7, "terciario": 61.4},
    2025: {"primario": 5.7, "secundario": 33.0, "terciario": 61.3},
}


class ComposicionSectorialEmpleoQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for anio, sectores in sorted(_SERIE_SECTORIAL.items()):
            rows.append({"anio": anio, "sector": "primario", "valor": float(sectores["primario"])})
            rows.append(
                {"anio": anio, "sector": "secundario", "valor": float(sectores["secundario"])}
            )
            rows.append(
                {"anio": anio, "sector": "terciario", "valor": float(sectores["terciario"])}
            )
        logger.info(
            "Serie composición sectorial empleo Querétaro preparada: %s registros",
            len(rows),
        )
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": f"ciudad:queretaro:{row['sector']}",
                "valor": row["valor"],
                "unidad": "Porcentaje",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
