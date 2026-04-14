# etl/sources/worldbank/turismo_ingresos.py
"""
ETL: Ingresos por divisas turísticas de México.
Fuente: World Bank (ST.INT.RCPT.CD) + SECTUR/Banxico para años recientes.
"""

from typing import Any

import httpx

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "wb.turismo_ingresos"
WB_URL = "https://api.worldbank.org/v2/country/MEX/indicator/ST.INT.RCPT.CD?format=json&per_page=15&date=2010:2025"

# Datos recientes de SECTUR/Banxico (no disponibles en WB API)
DATOS_RECIENTES = {
    2024: 32_800_000_000,
    2023: 31_100_000_000,
    2022: 28_000_000_000,
    2021: 19_800_000_000,
}


class TurismoIngresosExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Extrayendo ingresos turísticos de World Bank + SECTUR")
        wb_data = []
        try:
            r = httpx.get(WB_URL, timeout=15)
            r.raise_for_status()
            data = r.json()
            if len(data) > 1 and data[1]:
                wb_data = data[1]
        except Exception as exc:
            logger.warning("Error al consultar World Bank: %s", exc)
        return [{"wb": wb_data, "recientes": DATOS_RECIENTES}]

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not raw:
            return []
        records = []
        seen_years: set[int] = set()

        # Primero datos recientes (tienen prioridad)
        for anio, valor in raw[0]["recientes"].items():
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": None,
                    "valor": float(valor),
                    "unidad": "USD",
                    "periodo": anio,
                }
            )
            seen_years.add(anio)

        # Luego datos del World Bank (solo años no cubiertos)
        for obs in raw[0]["wb"]:
            if obs.get("value") is None:
                continue
            anio = int(obs["date"])
            if anio in seen_years:
                continue
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": None,
                    "valor": float(obs["value"]),
                    "unidad": "USD",
                    "periodo": anio,
                }
            )

        return records
