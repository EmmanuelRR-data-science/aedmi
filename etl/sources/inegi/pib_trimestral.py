# etl/sources/inegi/pib_trimestral.py
"""ETL: PIB trimestral a precios corrientes (MXN y USD) — INEGI BIE serie 735879."""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger
from sources.banxico.client import fetch_serie as fetch_banxico
from sources.inegi.client import fetch_serie as fetch_inegi

logger = get_logger(__name__)

# INEGI BIE: PIB trimestral a precios corrientes (millones de pesos)
SERIE_PIB_MXN = "735879"
# Banxico: tipo de cambio FIX para calcular PIB en USD
SERIE_TC_BANXICO = "SF43718"

INDICADOR_CLAVE_MXN = "banxico.pib_trimestral_mxn"
INDICADOR_CLAVE_USD = "banxico.pib_trimestral_usd"


def _parse_trimestre(time_period: str) -> tuple[int, int] | None:
    """
    Parsea el formato YYYY/QQ de INEGI.
    Retorna (anio, mes_inicio_trimestre) o None si no es válido.
    Ejemplo: '2025/04' -> (2025, 10) — 4T empieza en octubre
    """
    parts = time_period.split("/")
    if len(parts) != 2:
        return None
    try:
        anio = int(parts[0])
        trimestre = int(parts[1])
        if trimestre not in (1, 2, 3, 4):
            return None
        mes_inicio = (trimestre - 1) * 3 + 1
        return anio, mes_inicio
    except ValueError:
        return None


class PIBTrimestralINEGIExtractor(BaseExtractor):
    """
    Extrae PIB trimestral en MXN desde INEGI BIE (serie 735879) y calcula
    PIB en USD usando el tipo de cambio FIX promedio del trimestre de Banxico.
    Los datos se almacenan en mensual.datos usando el primer mes del trimestre.
    """

    periodicidad = "mensual"
    schema = "mensual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE_MXN
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Extrayendo PIB trimestral MXN de INEGI BIE serie %s", SERIE_PIB_MXN)
        pib_raw = fetch_inegi(SERIE_PIB_MXN)

        logger.info("Extrayendo tipo de cambio FIX de Banxico para cálculo PIB USD")
        tc_raw = fetch_banxico(SERIE_TC_BANXICO)

        return [{"pib": pib_raw, "tc": tc_raw}]

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not raw:
            return []

        pib_raw: list[dict] = raw[0]["pib"]
        tc_raw: list[dict] = raw[0]["tc"]

        # Construir mapa de tipo de cambio promedio por (año, mes)
        from datetime import datetime

        tc_por_mes: dict[tuple[int, int], list[float]] = {}
        for row in tc_raw:
            dato = row.get("dato", "N/E")
            if dato in ("N/E", "N/D", "", None):
                continue
            try:
                fecha = datetime.strptime(row["fecha"], "%d/%m/%Y").date()
                valor_tc = float(str(dato).replace(",", ""))
                key = (fecha.year, fecha.month)
                tc_por_mes.setdefault(key, []).append(valor_tc)
            except (ValueError, KeyError):
                continue

        def tc_promedio_trimestre(anio: int, trimestre: int) -> float | None:
            mes_inicio = (trimestre - 1) * 3 + 1
            valores = []
            for m in range(mes_inicio, mes_inicio + 3):
                valores.extend(tc_por_mes.get((anio, m), []))
            return sum(valores) / len(valores) if valores else None

        records = []
        for row in pib_raw:
            obs_value = row.get("OBS_VALUE")
            time_period = row.get("TIME_PERIOD", "")

            if not obs_value or obs_value in ("N/E", "N/D", "", None):
                continue

            parsed = _parse_trimestre(time_period)
            if parsed is None:
                logger.warning("Formato de período no reconocido: %s", time_period)
                continue

            anio, mes_ref = parsed
            trimestre = (mes_ref - 1) // 3 + 1

            try:
                pib_mxn = float(str(obs_value).replace(",", ""))
            except ValueError:
                logger.warning("Valor no numérico en PIB: %s", obs_value)
                continue

            # Registro PIB MXN
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": None,
                    "valor": round(pib_mxn, 4),
                    "unidad": "Millones de pesos",
                    "anio": anio,
                    "mes": mes_ref,
                }
            )

            # Registro PIB USD calculado
            tc = tc_promedio_trimestre(anio, trimestre)
            if tc and tc > 0:
                pib_usd = round(pib_mxn / tc, 4)
                records.append(
                    {
                        "indicador_id": None,
                        "nivel_geografico": "nacional",
                        "entidad_clave": "usd_calculado",
                        "valor": pib_usd,
                        "unidad": "Millones de dolares",
                        "anio": anio,
                        "mes": mes_ref,
                    }
                )

        return records
