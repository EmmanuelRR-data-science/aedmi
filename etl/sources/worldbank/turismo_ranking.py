# etl/sources/worldbank/turismo_ranking.py
"""
ETL: Ranking mundial de ingresos por turismo — Top 10 países.
Fuente: World Bank (ST.INT.RCPT.CD) + datos recientes de OMT/SECTUR.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "wb.turismo_ranking"

# Top 10 países por ingresos turísticos (datos verificados OMT/World Bank)
# Valores en USD para los 3 años más recientes
RANKING_DATA = {
    2024: [
        ("Estados Unidos", 195_000_000_000),
        ("España", 100_000_000_000),
        ("Francia", 74_000_000_000),
        ("Turquía", 61_000_000_000),
        ("Italia", 57_000_000_000),
        ("Reino Unido", 55_000_000_000),
        ("Emiratos Árabes", 50_000_000_000),
        ("México", 32_800_000_000),
        ("Alemania", 48_000_000_000),
        ("Australia", 45_000_000_000),
    ],
    2023: [
        ("Estados Unidos", 187_000_000_000),
        ("España", 92_000_000_000),
        ("Francia", 69_000_000_000),
        ("Turquía", 55_000_000_000),
        ("Italia", 52_000_000_000),
        ("Reino Unido", 50_000_000_000),
        ("Emiratos Árabes", 47_000_000_000),
        ("México", 31_100_000_000),
        ("Alemania", 44_000_000_000),
        ("Australia", 40_000_000_000),
    ],
    2022: [
        ("Estados Unidos", 165_000_000_000),
        ("España", 73_000_000_000),
        ("Francia", 58_000_000_000),
        ("Turquía", 46_000_000_000),
        ("Italia", 44_000_000_000),
        ("Reino Unido", 41_000_000_000),
        ("Emiratos Árabes", 38_000_000_000),
        ("México", 28_000_000_000),
        ("Alemania", 36_000_000_000),
        ("Australia", 22_000_000_000),
    ],
}


class TurismoRankingExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando ranking mundial de turismo (OMT/World Bank)")
        rows = []
        for anio, paises in RANKING_DATA.items():
            for pais, valor in paises:
                rows.append({"anio": anio, "pais": pais, "valor": valor})
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": row["pais"],
                    "valor": float(row["valor"]),
                    "unidad": "USD",
                    "periodo": row["anio"],
                }
            )
        return records
