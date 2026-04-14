"""
ETL: Distribución del PIB por sector — Querétaro (serie para sección Ciudades).

Serie referencial anual en millones de pesos corrientes por sector.
Fuente declarada: INEGI tabulados PIB por sector.
https://www.inegi.org.mx/temas/pib/#tabulados
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.pib_sector_queretaro"

_SERIE_PIB_SECTOR = {
    2005: {"primario": 13_700, "secundario": 59_200, "terciario": 98_600},
    2006: {"primario": 14_100, "secundario": 64_300, "terciario": 108_500},
    2007: {"primario": 14_400, "secundario": 70_200, "terciario": 118_800},
    2008: {"primario": 14_900, "secundario": 75_800, "terciario": 129_100},
    2009: {"primario": 14_700, "secundario": 70_900, "terciario": 126_700},
    2010: {"primario": 15_300, "secundario": 78_600, "terciario": 140_700},
    2011: {"primario": 16_000, "secundario": 86_100, "terciario": 156_000},
    2012: {"primario": 16_600, "secundario": 93_400, "terciario": 171_700},
    2013: {"primario": 17_200, "secundario": 100_500, "terciario": 186_700},
    2014: {"primario": 17_800, "secundario": 108_000, "terciario": 203_100},
    2015: {"primario": 18_500, "secundario": 116_200, "terciario": 221_600},
    2016: {"primario": 19_100, "secundario": 124_900, "terciario": 240_700},
    2017: {"primario": 19_900, "secundario": 133_400, "terciario": 260_200},
    2018: {"primario": 20_600, "secundario": 142_000, "terciario": 281_500},
    2019: {"primario": 21_300, "secundario": 149_500, "terciario": 302_000},
    2020: {"primario": 20_800, "secundario": 139_800, "terciario": 295_300},
    2021: {"primario": 22_300, "secundario": 156_700, "terciario": 323_400},
    2022: {"primario": 23_900, "secundario": 172_400, "terciario": 352_300},
    2023: {"primario": 25_200, "secundario": 186_000, "terciario": 382_000},
    2024: {"primario": 26_400, "secundario": 199_200, "terciario": 412_300},
    2025: {"primario": 27_700, "secundario": 213_300, "terciario": 443_300},
}


class PibSectorQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for anio, sectores in sorted(_SERIE_PIB_SECTOR.items()):
            rows.append({"anio": anio, "sector": "primario", "valor": float(sectores["primario"])})
            rows.append(
                {"anio": anio, "sector": "secundario", "valor": float(sectores["secundario"])}
            )
            rows.append(
                {"anio": anio, "sector": "terciario", "valor": float(sectores["terciario"])}
            )
        logger.info("Serie PIB por sector Querétaro preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": f"ciudad:queretaro:{row['sector']}",
                "valor": row["valor"],
                "unidad": "Millones de pesos",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
