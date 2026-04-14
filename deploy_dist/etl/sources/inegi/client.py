# etl/sources/inegi/client.py
"""Cliente HTTP para la API BIE-BISE de INEGI."""

import os
from typing import Any

import httpx

BASE_URL = "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml"
_TOKEN: str | None = None


def get_token() -> str:
    global _TOKEN
    if _TOKEN is None:
        _TOKEN = os.environ["INEGI_TOKEN"]
    return _TOKEN


def fetch_serie(
    serie: str,
    reciente: bool = False,
    fuente: str = "BIE-BISE",
) -> list[dict[str, Any]]:
    """
    Consulta una serie del BIE/BISE de INEGI.
    reciente=True devuelve solo el dato más reciente.
    Retorna lista de dicts con claves: 'TIME_PERIOD', 'OBS_VALUE', 'OBS_STATUS'.
    """
    flag = "true" if reciente else "false"
    url = f"{BASE_URL}/INDICATOR/{serie}/es/00/{flag}/{fuente}/2.0/{get_token()}?type=json"

    with httpx.Client(timeout=30) as client:
        response = client.get(url)
        response.raise_for_status()

    data = response.json()
    series_data = data.get("Series", [])
    if not series_data:
        return []

    return series_data[0].get("OBSERVATIONS", [])
