# etl/sources/banxico/client.py
"""Cliente HTTP para la API SIE de Banxico."""

import os
from typing import Any

import httpx

BASE_URL = "https://www.banxico.org.mx/SieAPIRest/service/v1"
_TOKEN: str | None = None


def get_token() -> str:
    global _TOKEN
    if _TOKEN is None:
        _TOKEN = os.environ["BANXICO_TOKEN"]
    return _TOKEN


def fetch_serie(
    serie: str, fecha_inicio: str = "2000-01-01", fecha_fin: str = ""
) -> list[dict[str, Any]]:
    """
    Consulta una serie del SIE de Banxico.
    Retorna lista de dicts con claves: 'fecha', 'dato'.
    """
    import datetime

    if not fecha_fin:
        fecha_fin = datetime.date.today().strftime("%Y-%m-%d")

    url = f"{BASE_URL}/series/{serie}/datos/{fecha_inicio}/{fecha_fin}"
    headers = {"Bmx-Token": get_token(), "Accept": "application/json"}

    with httpx.Client(timeout=30) as client:
        response = client.get(url, headers=headers)
        response.raise_for_status()

    data = response.json()
    series_data = data.get("bmx", {}).get("series", [])
    if not series_data:
        return []

    datos = series_data[0].get("datos", [])
    return [{"fecha": d["fecha"], "dato": d["dato"]} for d in datos]
