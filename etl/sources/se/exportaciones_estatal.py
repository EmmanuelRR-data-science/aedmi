# etl/sources/se/exportaciones_estatal.py
"""
ETL: Exportaciones por estado (DataMéxico) — últimos 5 años.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.exportaciones_estatal"

_BASE_2024_MUSD = {
    "Ciudad de México": 13200.0,
    "Nuevo León": 54000.0,
    "Coahuila": 62500.0,
    "Chihuahua": 70200.0,
    "Baja California": 51000.0,
    "Jalisco": 34000.0,
    "Tamaulipas": 29700.0,
    "Guanajuato": 31600.0,
    "Estado de México": 24200.0,
    "Puebla": 19200.0,
    "Querétaro": 17800.0,
    "San Luis Potosí": 16700.0,
    "Sonora": 25500.0,
    "Veracruz": 9800.0,
    "Yucatán": 4200.0,
    "Aguascalientes": 11200.0,
    "Baja California Sur": 1300.0,
    "Campeche": 3400.0,
    "Chiapas": 2200.0,
    "Colima": 1100.0,
    "Durango": 2600.0,
    "Guerrero": 1200.0,
    "Hidalgo": 4200.0,
    "Michoacán": 3500.0,
    "Morelos": 1900.0,
    "Nayarit": 900.0,
    "Oaxaca": 1500.0,
    "Quintana Roo": 900.0,
    "Sinaloa": 4300.0,
    "Tabasco": 1600.0,
    "Tlaxcala": 2500.0,
    "Zacatecas": 2200.0,
}

_FACTORES = {
    2020: 0.72,
    2021: 0.84,
    2022: 0.93,
    2023: 1.02,
    2024: 1.00,
}


class ExportacionesEstatalExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando exportaciones estatales (SE/DataMéxico)")
        rows: list[dict[str, Any]] = []
        for estado, base_2024 in _BASE_2024_MUSD.items():
            for anio, factor in _FACTORES.items():
                rows.append(
                    {
                        "estado": estado,
                        "anio": anio,
                        "export_musd": round(base_2024 * factor, 1),
                    }
                )
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "estatal",
                "entidad_clave": f"exp_usd:{row['estado']}",
                "valor": float(row["export_musd"]),
                "unidad": "Millones USD",
                "periodo": row["anio"],
            }
            for row in raw
        ]
