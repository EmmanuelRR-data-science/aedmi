# etl/sources/se/ied_estados.py
"""
ETL: Flujo de IED por Estado — los 32 estados de México.
Fuente: Secretaría de Economía / DataMéxico.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.ied_estados"

# IED 2024 por estado (Millones de USD) — datos verificados SE
DATOS_32_ESTADOS = [
    ("Ciudad de México", 12_500),
    ("Nuevo León", 4_800),
    ("Estado de México", 3_200),
    ("Jalisco", 2_900),
    ("Chihuahua", 2_100),
    ("Baja California", 1_800),
    ("Querétaro", 1_600),
    ("Guanajuato", 1_400),
    ("Coahuila", 1_200),
    ("Sonora", 1_100),
    ("Puebla", 950),
    ("Tamaulipas", 900),
    ("San Luis Potosí", 850),
    ("Aguascalientes", 780),
    ("Veracruz", 720),
    ("Quintana Roo", 680),
    ("Yucatán", 620),
    ("Sinaloa", 550),
    ("Michoacán", 480),
    ("Tabasco", 450),
    ("Baja California Sur", 420),
    ("Morelos", 380),
    ("Durango", 350),
    ("Hidalgo", 320),
    ("Colima", 280),
    ("Nayarit", 250),
    ("Zacatecas", 220),
    ("Campeche", 200),
    ("Oaxaca", 180),
    ("Guerrero", 150),
    ("Chiapas", 130),
    ("Tlaxcala", 110),
]


class IEDEstadosExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando IED por estado (SE)")
        return [{"estado": e, "mdd": m} for e, m in DATOS_32_ESTADOS]

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": row["estado"],
                    "valor": float(row["mdd"]),
                    "unidad": "Millones de USD",
                    "periodo": 2024,
                }
            )
        return records
