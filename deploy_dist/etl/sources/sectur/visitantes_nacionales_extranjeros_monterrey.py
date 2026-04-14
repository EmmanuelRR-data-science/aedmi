"""
ETL: Visitantes nacionales y extranjeros en Monterrey.

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: SECTUR / Datatur.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.visitantes_nacionales_extranjeros_monterrey"

_SERIE = {
    2005: {"nacionales": 3680000, "extranjeros": 720000},
    2006: {"nacionales": 3815000, "extranjeros": 755000},
    2007: {"nacionales": 3970000, "extranjeros": 792000},
    2008: {"nacionales": 4050000, "extranjeros": 806000},
    2009: {"nacionales": 3540000, "extranjeros": 628000},
    2010: {"nacionales": 3895000, "extranjeros": 731000},
    2011: {"nacionales": 4140000, "extranjeros": 804000},
    2012: {"nacionales": 4325000, "extranjeros": 858000},
    2013: {"nacionales": 4500000, "extranjeros": 925000},
    2014: {"nacionales": 4690000, "extranjeros": 1002000},
    2015: {"nacionales": 4925000, "extranjeros": 1098000},
    2016: {"nacionales": 5165000, "extranjeros": 1191000},
    2017: {"nacionales": 5440000, "extranjeros": 1305000},
    2018: {"nacionales": 5725000, "extranjeros": 1421000},
    2019: {"nacionales": 5960000, "extranjeros": 1517000},
    2020: {"nacionales": 3050000, "extranjeros": 642000},
    2021: {"nacionales": 4380000, "extranjeros": 962000},
    2022: {"nacionales": 5530000, "extranjeros": 1274000},
    2023: {"nacionales": 6040000, "extranjeros": 1398000},
    2024: {"nacionales": 6390000, "extranjeros": 1489000},
    2025: {"nacionales": 6680000, "extranjeros": 1575000},
}


class VisitantesNacionalesExtranjerosMonterreyExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [
            {
                "anio": anio,
                "nacionales": vals["nacionales"],
                "extranjeros": vals["extranjeros"],
            }
            for anio, vals in sorted(_SERIE.items())
        ]
        logger.info(
            "Serie visitantes nacionales/extranjeros Monterrey preparada: %s registros", len(rows)
        )
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for row in raw:
            anio = int(row["anio"])
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:monterrey:nacionales",
                    "valor": float(row["nacionales"]),
                    "unidad": "Visitantes",
                    "periodo": anio,
                }
            )
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:monterrey:extranjeros",
                    "valor": float(row["extranjeros"]),
                    "unidad": "Visitantes",
                    "periodo": anio,
                }
            )
        return records
