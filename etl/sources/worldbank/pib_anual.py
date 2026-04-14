# etl/sources/worldbank/pib_anual.py
"""ETL: PIB Nacional total y per cápita anual en MXN — World Bank API."""

from typing import Any

import httpx

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "worldbank.pib_anual_mxn"
WB_BASE = "https://api.worldbank.org/v2/country/MEX/indicator"
# NY.GDP.MKTP.CN = PIB nominal en moneda local (MXN corrientes)
# NY.GDP.PCAP.CN = PIB per cápita en moneda local (MXN corrientes)
SERIE_PIB_TOTAL = "NY.GDP.MKTP.CN"
SERIE_PIB_PC = "NY.GDP.PCAP.CN"


def _fetch_wb(indicator: str, years: int = 12) -> list[dict[str, Any]]:
    url = f"{WB_BASE}/{indicator}?format=json&per_page={years}&date=2014:2025"
    r = httpx.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    if len(data) > 1 and data[1]:
        return data[1]
    return []


class PIBAnualWorldBankExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Extrayendo PIB anual MXN de World Bank API")
        pib_total = _fetch_wb(SERIE_PIB_TOTAL)
        pib_pc = _fetch_wb(SERIE_PIB_PC)
        return [{"total": pib_total, "percapita": pib_pc}]

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not raw:
            return []
        records = []

        for obs in raw[0]["total"]:
            if obs.get("value") is None:
                continue
            try:
                anio = int(obs["date"])
                valor = float(obs["value"])
            except (ValueError, TypeError):
                continue
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": "PIB Total",
                    "valor": round(valor, 0),
                    "unidad": "Pesos corrientes",
                    "periodo": anio,
                }
            )

        for obs in raw[0]["percapita"]:
            if obs.get("value") is None:
                continue
            try:
                anio = int(obs["date"])
                valor = float(obs["value"])
            except (ValueError, TypeError):
                continue
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": "PIB Per Cápita",
                    "valor": round(valor, 2),
                    "unidad": "Pesos corrientes por persona",
                    "periodo": anio,
                }
            )

        return records
