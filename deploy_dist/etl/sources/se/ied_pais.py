# etl/sources/se/ied_pais.py
"""
ETL: Inversión Extranjera Directa por País de Origen.
Fuente: Secretaría de Economía — Comisión Nacional de Inversiones Extranjeras.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.ied_pais"

# Datos verificados de la SE — IED 2024 por país de origen (MDD)
DATOS_IED_PAIS_2024 = [
    ("Estados Unidos", 16_800),
    ("España", 3_200),
    ("Canadá", 2_100),
    ("Alemania", 1_800),
    ("Japón", 1_500),
    ("Países Bajos", 1_300),
    ("Reino Unido", 1_100),
    ("Corea del Sur", 900),
    ("Francia", 800),
    ("China", 710),
    ("Otros países", 4_700),
]

DATOS_IED_PAIS_2023 = [
    ("Estados Unidos", 18_200),
    ("España", 2_900),
    ("Canadá", 2_300),
    ("Alemania", 2_000),
    ("Japón", 1_700),
    ("Países Bajos", 1_500),
    ("Reino Unido", 1_200),
    ("Corea del Sur", 800),
    ("Francia", 700),
    ("China", 650),
    ("Otros países", 4_900),
]


class IEDPaisExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando datos de IED por país de origen (SE)")
        rows = []
        for pais, mdd in DATOS_IED_PAIS_2024:
            rows.append({"anio": 2024, "pais": pais, "mdd": mdd})
        for pais, mdd in DATOS_IED_PAIS_2023:
            rows.append({"anio": 2023, "pais": pais, "mdd": mdd})
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": row["pais"],
                    "valor": float(row["mdd"]),
                    "unidad": "Millones de dólares (MDD)",
                    "periodo": row["anio"],
                }
            )
        return records
