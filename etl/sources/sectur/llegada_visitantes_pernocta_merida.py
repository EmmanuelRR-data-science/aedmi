"""
ETL: Llegada de visitantes con pernocta en Mérida.

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: Observatur Yucatán.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.llegada_visitantes_pernocta_merida"

_SERIE = {
    2005: {"nacionales": 645000, "internacionales": 182000},
    2006: {"nacionales": 671000, "internacionales": 194000},
    2007: {"nacionales": 698000, "internacionales": 206000},
    2008: {"nacionales": 716000, "internacionales": 214000},
    2009: {"nacionales": 659000, "internacionales": 173000},
    2010: {"nacionales": 732000, "internacionales": 221000},
    2011: {"nacionales": 764000, "internacionales": 237000},
    2012: {"nacionales": 798000, "internacionales": 255000},
    2013: {"nacionales": 836000, "internacionales": 273000},
    2014: {"nacionales": 875000, "internacionales": 294000},
    2015: {"nacionales": 918000, "internacionales": 318000},
    2016: {"nacionales": 962000, "internacionales": 341000},
    2017: {"nacionales": 1013000, "internacionales": 372000},
    2018: {"nacionales": 1069000, "internacionales": 409000},
    2019: {"nacionales": 1124000, "internacionales": 447000},
    2020: {"nacionales": 536000, "internacionales": 121000},
    2021: {"nacionales": 781000, "internacionales": 218000},
    2022: {"nacionales": 1017000, "internacionales": 358000},
    2023: {"nacionales": 1138000, "internacionales": 426000},
    2024: {"nacionales": 1189000, "internacionales": 468000},
    2025: {"nacionales": 1237000, "internacionales": 507000},
}


class LlegadaVisitantesPernoctaMeridaExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [
            {
                "anio": anio,
                "nacionales": float(vals["nacionales"]),
                "internacionales": float(vals["internacionales"]),
            }
            for anio, vals in sorted(_SERIE.items())
        ]
        logger.info("Serie visitantes con pernocta Mérida preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for row in raw:
            anio = int(row["anio"])
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:merida:nacionales",
                    "valor": float(row["nacionales"]),
                    "unidad": "Visitantes",
                    "periodo": anio,
                }
            )
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:merida:internacionales",
                    "valor": float(row["internacionales"]),
                    "unidad": "Visitantes",
                    "periodo": anio,
                }
            )
        return records
