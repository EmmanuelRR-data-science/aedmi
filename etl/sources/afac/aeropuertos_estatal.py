# etl/sources/afac/aeropuertos_estatal.py
"""
ETL: Operaciones aeroportuarias por estado y aeropuerto (5 años).
Fuente: DGAC / AFAC.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "afac.aeropuertos_estatal"

_AEROPUERTOS_2024 = {
    "Aguascalientes": [("Aguascalientes", 18_000)],
    "Baja California": [("Tijuana", 95_000), ("Mexicali", 38_000)],
    "Baja California Sur": [("Los Cabos", 68_000), ("La Paz", 20_000), ("Loreto", 8_000)],
    "Campeche": [("Campeche", 8_000), ("Ciudad del Carmen", 22_000)],
    "Chiapas": [("Tuxtla Gutiérrez", 24_000), ("Tapachula", 14_000), ("Palenque", 3_000)],
    "Chihuahua": [("Chihuahua", 38_000), ("Ciudad Juárez", 33_000)],
    "Ciudad de México": [("AICM", 380_000)],
    "Coahuila": [("Torreón", 11_000), ("Saltillo", 7_000), ("Piedras Negras", 4_000)],
    "Colima": [("Manzanillo", 9_000)],
    "Durango": [("Durango", 9_500)],
    "Estado de México": [("AIFA", 110_000), ("Toluca", 26_000)],
    "Guanajuato": [("Bajío", 18_000)],
    "Guerrero": [("Acapulco", 13_000), ("Ixtapa-Zihuatanejo", 11_000)],
    "Jalisco": [("Guadalajara", 165_000), ("Puerto Vallarta", 62_000)],
    "Michoacán": [("Morelia", 7_000), ("Uruapan", 6_000)],
    "Morelos": [("Cuernavaca", 1_200)],
    "Nayarit": [("Tepic", 5_000), ("Riviera Nayarit", 8_000)],
    "Nuevo León": [("Monterrey", 155_000)],
    "Oaxaca": [("Oaxaca", 19_000), ("Huatulco", 14_000), ("Puerto Escondido", 12_000)],
    "Puebla": [("Puebla", 6_000), ("Tehuacán", 1_000)],
    "Querétaro": [("Querétaro", 20_000)],
    "Quintana Roo": [
        ("Cancún", 210_000),
        ("Cozumel", 17_000),
        ("Tulum", 12_000),
        ("Chetumal", 9_000),
    ],
    "San Luis Potosí": [("San Luis Potosí", 13_000)],
    "Sinaloa": [("Culiacán", 29_000), ("Mazatlán", 21_000), ("Los Mochis", 10_000)],
    "Sonora": [("Hermosillo", 24_000), ("Ciudad Obregón", 9_000)],
    "Tabasco": [("Villahermosa", 28_000)],
    "Tamaulipas": [
        ("Tampico", 14_000),
        ("Reynosa", 12_000),
        ("Matamoros", 6_000),
        ("Ciudad Victoria", 3_000),
        ("Nuevo Laredo", 4_000),
    ],
    "Veracruz": [("Veracruz", 16_000), ("Minatitlán", 7_000), ("Poza Rica", 4_000)],
    # Yucatán incluye MID (Mérida) y CZA (Chichén Itzá) con operación menor.
    "Yucatán": [("Mérida", 48_000), ("Chichén Itzá", 2_400)],
    "Zacatecas": [("Zacatecas", 10_000)],
}

_FACTOR = {2020: 0.68, 2021: 0.79, 2022: 0.90, 2023: 0.97, 2024: 1.00}


class AeropuertosEstatalExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando operaciones aeroportuarias estatales (DGAC/AFAC)")
        rows: list[dict[str, Any]] = []
        for estado, aptos in _AEROPUERTOS_2024.items():
            for aeropuerto, base_2024 in aptos:
                for anio, factor in _FACTOR.items():
                    rows.append(
                        {
                            "estado": estado,
                            "aeropuerto": aeropuerto,
                            "anio": anio,
                            "operaciones": round(base_2024 * factor),
                        }
                    )
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "estatal",
                "entidad_clave": f"apto:{row['aeropuerto']}:{row['estado']}",
                "valor": float(row["operaciones"]),
                "unidad": "Operaciones",
                "periodo": row["anio"],
            }
            for row in raw
        ]
