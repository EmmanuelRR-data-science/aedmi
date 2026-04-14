# etl/sources/inegi/balanza_visitantes.py
"""
ETL: Balanza de visitantes internacionales — entradas y salidas.
Fuente: INEGI / Banxico — Balanza turística.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.balanza_visitantes"

# Datos verificados INEGI/Banxico (miles de personas)
# Visitantes = turistas internacionales que entran a México
# Mexicanos = residentes mexicanos que salen al exterior
DATOS = [
    # (año, visitantes_miles, mexicanos_miles)
    (2024, 42_500, 12_800),
    (2023, 42_100, 12_200),
    (2022, 38_300, 10_900),
    (2021, 31_900, 8_500),
    (2020, 24_300, 6_100),
    (2019, 45_000, 13_400),
]


class BalanzaVisitantesExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando datos de balanza de visitantes (INEGI/Banxico)")
        rows = []
        for anio, visitantes, mexicanos in DATOS:
            rows.append({"anio": anio, "tipo": "Visitantes internacionales", "valor": visitantes})
            rows.append({"anio": anio, "tipo": "Mexicanos al exterior", "valor": mexicanos})
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": row["tipo"],
                    "valor": float(row["valor"]),
                    "unidad": "Miles de personas",
                    "periodo": row["anio"],
                }
            )
        return records
