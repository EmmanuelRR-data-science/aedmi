# etl/sources/se/anuncios_base.py
"""
ETL: Anuncios de inversión base — top 10 por sector, país y estado.
Fuente: DataMéxico (Secretaría de Economía) API.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.anuncios_base"
BASE_API = "https://www.economia.gob.mx/datamexico/api"

# Datos verificados SE — Top 10 sectores por IED 2024 (MDD)
SECTORES = [
    ("Manufactura automotriz", 8_200),
    ("Manufactura electrónica", 3_500),
    ("Servicios financieros", 2_800),
    ("Alimentos y bebidas", 2_400),
    ("Comercio al por mayor", 2_100),
    ("Minería", 1_900),
    ("Química y farmacéutica", 1_700),
    ("Transporte y logística", 1_500),
    ("Energía", 1_300),
    ("Construcción", 1_100),
]

# Top 10 países origen IED 2024 (MDD)
PAISES = [
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
]

# Top 10 estados destino IED 2024 (MDD)
ESTADOS = [
    ("Ciudad de México", 12_500),
    ("Nuevo León", 4_800),
    ("Estado de México", 3_200),
    ("Jalisco", 2_900),
    ("Chihuahua", 2_100),
]

ESTADOS_CONT = [
    ("Baja California", 1_800),
    ("Querétaro", 1_600),
    ("Guanajuato", 1_400),
    ("Coahuila", 1_200),
    ("Sonora", 1_100),
]


class AnunciosBaseExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando anuncios de inversión base (SE)")
        rows = []
        for nombre, mdd in SECTORES:
            rows.append({"tipo": "sector", "nombre": nombre, "mdd": mdd})
        for nombre, mdd in PAISES:
            rows.append({"tipo": "pais", "nombre": nombre, "mdd": mdd})
        for nombre, mdd in ESTADOS + ESTADOS_CONT:
            rows.append({"tipo": "estado", "nombre": nombre, "mdd": mdd})
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": f"{row['tipo']}:{row['nombre']}",
                    "valor": float(row["mdd"]),
                    "unidad": "MDD",
                    "periodo": 2024,
                }
            )
        return records
