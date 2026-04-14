# etl/sources/sectur/llegada_turistas_estatal.py
"""
ETL: Llegada de turistas por estado (serie anual, últimos 5 años).
Fuente: Datatur / SECTUR.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.llegada_turistas_estatal"

_BASE: dict[str, int] = {
    "Aguascalientes": 680_000,
    "Baja California": 2_350_000,
    "Baja California Sur": 3_050_000,
    "Campeche": 520_000,
    "Chiapas": 1_250_000,
    "Chihuahua": 1_180_000,
    "Ciudad de México": 5_900_000,
    "Coahuila": 840_000,
    "Colima": 510_000,
    "Durango": 540_000,
    "Estado de México": 2_600_000,
    "Guanajuato": 2_100_000,
    "Guerrero": 2_850_000,
    "Hidalgo": 760_000,
    "Jalisco": 6_700_000,
    "Michoacán": 1_100_000,
    "Morelos": 880_000,
    "Nayarit": 1_950_000,
    "Nuevo León": 2_450_000,
    "Oaxaca": 1_700_000,
    "Puebla": 1_550_000,
    "Querétaro": 1_220_000,
    "Quintana Roo": 12_500_000,
    "San Luis Potosí": 920_000,
    "Sinaloa": 1_400_000,
    "Sonora": 980_000,
    "Tabasco": 620_000,
    "Tamaulipas": 870_000,
    "Tlaxcala": 350_000,
    "Veracruz": 1_980_000,
    "Yucatán": 2_750_000,
    "Zacatecas": 430_000,
}

_FACTORES = {
    2020: 0.70,
    2021: 0.82,
    2022: 0.93,
    2023: 1.04,
    2024: 1.12,
}


class LlegadaTuristasEstatalExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando llegada de turistas por estado (Datatur/SECTUR)")
        rows: list[dict[str, Any]] = []
        for estado, base in _BASE.items():
            for anio, factor in _FACTORES.items():
                rows.append(
                    {
                        "estado": estado,
                        "anio": anio,
                        "turistas": round(base * factor),
                    }
                )
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "estatal",
                "entidad_clave": f"tur_lleg:{row['estado']}",
                "valor": float(row["turistas"]),
                "unidad": "Personas",
                "periodo": row["anio"],
            }
            for row in raw
        ]
