# etl/sources/imf/pib_proyeccion.py
"""
ETL: Proyección del PIB Nacional — FMI World Economic Outlook.
Los datos del FMI vienen en USD y se convierten a MXN usando el tipo de cambio
FIX promedio anual de Banxico. Para años futuros (proyecciones) se usa el TC
más reciente disponible.
"""

import datetime
from typing import Any

import httpx

from core.base_extractor import BaseExtractor
from core.logger import get_logger
from sources.banxico.client import fetch_serie as fetch_banxico

logger = get_logger(__name__)

INDICADOR_CLAVE = "imf.pib_proyeccion"
IMF_BASE = "https://www.imf.org/external/datamapper/api/v1"
SERIE_TC_BANXICO = "SF43718"


def _fetch_imf(indicator: str) -> dict[str, float]:
    url = f"{IMF_BASE}/{indicator}/MEX"
    r = httpx.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data.get("values", {}).get(indicator, {}).get("MEX", {})


def _build_tc_promedio_anual() -> dict[int, float]:
    """
    Construye un mapa de tipo de cambio FIX promedio anual desde Banxico.
    Para años futuros, usa el TC más reciente disponible.
    """
    tc_raw = fetch_banxico(SERIE_TC_BANXICO)
    tc_por_anio: dict[int, list[float]] = {}
    ultimo_tc: float = 18.0  # fallback

    for row in tc_raw:
        dato = row.get("dato", "N/E")
        if dato in ("N/E", "N/D", "", None):
            continue
        try:
            fecha = datetime.datetime.strptime(row["fecha"], "%d/%m/%Y").date()
            valor = float(str(dato).replace(",", ""))
            tc_por_anio.setdefault(fecha.year, []).append(valor)
            ultimo_tc = valor
        except (ValueError, KeyError):
            continue

    # Promediar por año
    promedios: dict[int, float] = {}
    for anio, valores in tc_por_anio.items():
        promedios[anio] = sum(valores) / len(valores)

    # Para años futuros sin datos, usar el TC más reciente
    current_year = datetime.date.today().year
    for anio in range(current_year, current_year + 10):
        if anio not in promedios:
            promedios[anio] = ultimo_tc

    return promedios


class PIBProyeccionIMFExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Extrayendo proyecciones PIB de FMI WEO")
        pib_total = _fetch_imf("NGDPD")  # miles de millones USD
        pib_pc = _fetch_imf("NGDPDPC")  # USD per cápita
        crecimiento = _fetch_imf("NGDP_RPCH")  # % crecimiento real

        logger.info("Obteniendo tipo de cambio FIX de Banxico para conversión a MXN")
        tc_anual = _build_tc_promedio_anual()

        return [
            {"total": pib_total, "percapita": pib_pc, "crecimiento": crecimiento, "tc": tc_anual}
        ]

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not raw:
            return []
        records = []
        current_year = datetime.date.today().year
        tc_anual: dict[int, float] = raw[0]["tc"]

        for year_str, valor in raw[0]["total"].items():
            anio = int(year_str)
            if anio < current_year - 5:
                continue
            if valor is None:
                continue
            valor_usd = float(valor)
            tc = tc_anual.get(anio, tc_anual.get(current_year, 18.0))
            # FMI da miles de millones USD → convertir a miles de millones MXN
            valor_mxn = round(valor_usd * tc, 3)
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": "PIB Total MXN",
                    "valor": valor_mxn,
                    "unidad": "Miles de millones MXN",
                    "periodo": anio,
                }
            )

        for year_str, valor in raw[0]["percapita"].items():
            anio = int(year_str)
            if anio < current_year - 5:
                continue
            if valor is None:
                continue
            valor_usd = float(valor)
            tc = tc_anual.get(anio, tc_anual.get(current_year, 18.0))
            valor_mxn = round(valor_usd * tc, 2)
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": "PIB Per Cápita MXN",
                    "valor": valor_mxn,
                    "unidad": "MXN por persona",
                    "periodo": anio,
                }
            )

        for year_str, valor in raw[0]["crecimiento"].items():
            anio = int(year_str)
            if anio < current_year - 5:
                continue
            if valor is None:
                continue
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": "Crecimiento Real %",
                    "valor": round(float(valor), 3),
                    "unidad": "%",
                    "periodo": anio,
                }
            )

        return records
