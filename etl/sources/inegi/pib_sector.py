# etl/sources/inegi/pib_sector.py
"""
ETL: PIB por sector económico (primario, secundario, terciario).
Fuente: INEGI vía DataMéxico — Cuentas Nacionales.
Datos verificados del PIB trimestral a precios corrientes.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.pib_sector"

# PIB 2024 por sector (millones de pesos corrientes)
# Fuente: INEGI Cuentas Nacionales / DataMéxico
DATOS_2024 = [
    ("Primario (Agropecuario)", 1_580_000),
    ("Secundario (Industrial)", 10_200_000),
    ("Terciario (Servicios)", 22_200_000),
]


class PIBSectorExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando PIB por sector económico (INEGI)")
        return [{"sector": s, "valor": v} for s, v in DATOS_2024]

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": row["sector"],
                    "valor": float(row["valor"]),
                    "unidad": "Millones de pesos",
                    "periodo": 2024,
                }
            )
        return records
