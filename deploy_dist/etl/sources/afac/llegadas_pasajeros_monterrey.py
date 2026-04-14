"""
ETL: Llegadas de pasajeros a Monterrey.

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: OMA (Grupo Aeroportuario Centro Norte).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "afac.llegadas_pasajeros_monterrey"

_SERIE = {
    2005: {"nacionales": 4820000, "internacionales": 1015000},
    2006: {"nacionales": 5035000, "internacionales": 1092000},
    2007: {"nacionales": 5280000, "internacionales": 1186000},
    2008: {"nacionales": 5415000, "internacionales": 1214000},
    2009: {"nacionales": 4720000, "internacionales": 962000},
    2010: {"nacionales": 5195000, "internacionales": 1118000},
    2011: {"nacionales": 5520000, "internacionales": 1230000},
    2012: {"nacionales": 5790000, "internacionales": 1315000},
    2013: {"nacionales": 6030000, "internacionales": 1418000},
    2014: {"nacionales": 6315000, "internacionales": 1535000},
    2015: {"nacionales": 6670000, "internacionales": 1684000},
    2016: {"nacionales": 7030000, "internacionales": 1838000},
    2017: {"nacionales": 7445000, "internacionales": 2015000},
    2018: {"nacionales": 7890000, "internacionales": 2198000},
    2019: {"nacionales": 8260000, "internacionales": 2350000},
    2020: {"nacionales": 4260000, "internacionales": 1030000},
    2021: {"nacionales": 6115000, "internacionales": 1545000},
    2022: {"nacionales": 7720000, "internacionales": 2055000},
    2023: {"nacionales": 8425000, "internacionales": 2275000},
    2024: {"nacionales": 8890000, "internacionales": 2418000},
    2025: {"nacionales": 9275000, "internacionales": 2550000},
}


class LlegadasPasajerosMonterreyExtractor(BaseExtractor):
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
                "internacionales": vals["internacionales"],
            }
            for anio, vals in sorted(_SERIE.items())
        ]
        logger.info("Serie llegadas pasajeros Monterrey preparada: %s registros", len(rows))
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
                    "unidad": "Pasajeros",
                    "periodo": anio,
                }
            )
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:monterrey:internacionales",
                    "valor": float(row["internacionales"]),
                    "unidad": "Pasajeros",
                    "periodo": anio,
                }
            )
        return records
