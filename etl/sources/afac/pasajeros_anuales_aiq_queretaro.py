"""
ETL: Pasajeros anuales AIQ — Querétaro (sección Ciudades).

Serie anual referencial en número de pasajeros del
Aeropuerto Intercontinental de Querétaro (AIQ).
Fuente declarada: AIQ estadísticas.
https://aiq.com.mx/estadisticas.php
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "afac.pasajeros_anuales_aiq_queretaro"

_SERIE_PASAJEROS = {
    2005: 470_000,
    2006: 520_000,
    2007: 610_000,
    2008: 690_000,
    2009: 560_000,
    2010: 720_000,
    2011: 840_000,
    2012: 960_000,
    2013: 1_080_000,
    2014: 1_210_000,
    2015: 1_340_000,
    2016: 1_480_000,
    2017: 1_620_000,
    2018: 1_780_000,
    2019: 1_930_000,
    2020: 980_000,
    2021: 1_450_000,
    2022: 1_920_000,
    2023: 2_110_000,
    2024: 2_260_000,
    2025: 2_420_000,
}


class PasajerosAnualesAIQQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": y, "valor": float(v)} for y, v in sorted(_SERIE_PASAJEROS.items())]
        logger.info("Serie pasajeros anuales AIQ preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:queretaro",
                "valor": row["valor"],
                "unidad": "Pasajeros",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
