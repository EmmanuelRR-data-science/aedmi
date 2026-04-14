# etl/sources/se/anuncios_inversion.py
"""
ETL: Anuncios de inversión — DataMéxico (Secretaría de Economía).
Nacional por año vía API + desglose por estado con datos verificados.
"""

from typing import Any

import httpx

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.anuncios_inversion"
DATAMEX_URL = (
    "https://www.economia.gob.mx/datamexico/api/data"
    "?cube=fdi_10_year_country&drilldowns=Year&measures=Investment,Count"
)

# Top 10 estados por IED 2024 (datos verificados SE)
DATOS_ESTADOS_2024 = [
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
    ("Otros estados", 4_272),
]


class AnunciosInversionExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Extrayendo anuncios de inversión de DataMéxico API")
        # Datos nacionales por año desde la API
        nacional = []
        try:
            r = httpx.get(DATAMEX_URL, timeout=15, follow_redirects=True)
            r.raise_for_status()
            nacional = r.json().get("data", [])
        except Exception as exc:
            logger.warning("Error al consultar DataMéxico: %s", exc)

        return [{"nacional": nacional, "estados": DATOS_ESTADOS_2024}]

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not raw:
            return []
        records = []

        # Nacional por año (últimos 6 años)
        nacional = sorted(raw[0]["nacional"], key=lambda x: x["Year"])
        for row in nacional[-6:]:
            anio = int(row["Year"])
            # Monto en MDD
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": "nac:monto",
                    "valor": round(float(row["Investment"]), 2),
                    "unidad": "MDD",
                    "periodo": anio,
                }
            )
            # Cantidad de anuncios
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": "nac:anuncios",
                    "valor": float(row["Count"]),
                    "unidad": "Anuncios",
                    "periodo": anio,
                }
            )

        # Por estado (2024)
        for estado, mdd in raw[0]["estados"]:
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": f"edo:{estado}",
                    "valor": float(mdd),
                    "unidad": "MDD",
                    "periodo": 2024,
                }
            )

        return records
