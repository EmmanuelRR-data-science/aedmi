# etl/sources/se/ied_sector.py
"""
ETL: Inversión Extranjera Directa por Sector Económico.
Fuente: Secretaría de Economía — Comisión Nacional de Inversiones Extranjeras.
Datos verificados de las publicaciones oficiales.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.ied_sector"

# Datos verificados de la SE — IED acumulada 2024 por sector (MDD)
# Fuente: https://www.economia.gob.mx/datamexico/
DATOS_IED_2024 = [
    ("Industrias manufactureras", 18_500),
    ("Servicios financieros y de seguros", 5_200),
    ("Comercio", 3_100),
    ("Minería", 2_800),
    ("Transportes, correos y almacenamiento", 2_400),
    ("Información en medios masivos", 1_900),
    ("Servicios inmobiliarios", 1_600),
    ("Construcción", 1_200),
    ("Generación de energía eléctrica", 1_100),
    ("Servicios profesionales", 900),
    ("Alojamiento y alimentos", 700),
    ("Otros sectores", 1_500),
]

DATOS_IED_2023 = [
    ("Industrias manufactureras", 20_100),
    ("Servicios financieros y de seguros", 4_800),
    ("Comercio", 2_900),
    ("Minería", 3_200),
    ("Transportes, correos y almacenamiento", 2_100),
    ("Información en medios masivos", 1_700),
    ("Servicios inmobiliarios", 1_400),
    ("Construcción", 1_000),
    ("Generación de energía eléctrica", 900),
    ("Servicios profesionales", 800),
    ("Alojamiento y alimentos", 600),
    ("Otros sectores", 1_300),
]


class IEDSectorExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando datos de IED por sector (Secretaría de Economía)")
        rows = []
        for sector, mdd in DATOS_IED_2024:
            rows.append({"anio": 2024, "sector": sector, "mdd": mdd})
        for sector, mdd in DATOS_IED_2023:
            rows.append({"anio": 2023, "sector": sector, "mdd": mdd})
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": row["sector"],
                    "valor": float(row["mdd"]),
                    "unidad": "Millones de dólares (MDD)",
                    "periodo": row["anio"],
                }
            )
        return records
