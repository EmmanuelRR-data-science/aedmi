# etl/sources/banxico/pib_trimestral.py
"""ETL: PIB trimestral MXN y USD calculado — Banxico SIE SR16734 + SF43718."""

from datetime import datetime
from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger
from sources.banxico.client import fetch_serie

logger = get_logger(__name__)

# PIB trimestral a precios corrientes (millones de pesos)
SERIE_PIB_MXN = "SR16734"
# Tipo de cambio FIX para calcular PIB en USD
SERIE_TC = "SF43718"

INDICADOR_CLAVE_MXN = "banxico.pib_trimestral_mxn"
INDICADOR_CLAVE_USD = "banxico.pib_trimestral_usd"


def _parse_fecha_banxico(fecha_str: str) -> datetime | None:
    """Parsea fecha en formato DD/MM/YYYY de Banxico."""
    try:
        return datetime.strptime(fecha_str, "%d/%m/%Y")
    except ValueError:
        return None


def _trimestre_de_fecha(fecha: datetime) -> tuple[int, int]:
    """Retorna (año, trimestre) dado un datetime."""
    trimestre = (fecha.month - 1) // 3 + 1
    return fecha.year, trimestre


class PIBTrimestralExtractor(BaseExtractor):
    """
    Extrae PIB trimestral en MXN desde Banxico SIE y calcula PIB en USD
    usando el tipo de cambio FIX promedio del trimestre.
    Carga ambos indicadores en la tabla mensual.datos usando anio+mes del
    primer mes del trimestre como referencia.
    """

    periodicidad = "mensual"
    schema = "mensual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE_MXN
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Extrayendo PIB trimestral MXN de Banxico SIE %s", SERIE_PIB_MXN)
        pib_raw = fetch_serie(SERIE_PIB_MXN)

        logger.info(
            "Extrayendo tipo de cambio FIX de Banxico SIE %s para cálculo PIB USD", SERIE_TC
        )
        tc_raw = fetch_serie(SERIE_TC)

        return [{"pib": pib_raw, "tc": tc_raw}]

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not raw:
            return []

        pib_raw: list[dict] = raw[0]["pib"]
        tc_raw: list[dict] = raw[0]["tc"]

        # Construir mapa de tipo de cambio por (año, mes)
        tc_por_mes: dict[tuple[int, int], list[float]] = {}
        for row in tc_raw:
            dato = row.get("dato", "N/E")
            if dato in ("N/E", "N/D", "", None):
                continue
            fecha = _parse_fecha_banxico(row["fecha"])
            if fecha is None:
                continue
            try:
                valor_tc = float(str(dato).replace(",", ""))
            except ValueError:
                continue
            key = (fecha.year, fecha.month)
            tc_por_mes.setdefault(key, []).append(valor_tc)

        # Promedio TC por trimestre
        def tc_promedio_trimestre(anio: int, trimestre: int) -> float | None:
            mes_inicio = (trimestre - 1) * 3 + 1
            valores = []
            for m in range(mes_inicio, mes_inicio + 3):
                valores.extend(tc_por_mes.get((anio, m), []))
            return sum(valores) / len(valores) if valores else None

        records = []
        for row in pib_raw:
            dato = row.get("dato", "N/E")
            if dato in ("N/E", "N/D", "", None):
                continue
            fecha = _parse_fecha_banxico(row["fecha"])
            if fecha is None:
                continue
            try:
                pib_mxn = float(str(dato).replace(",", ""))
            except ValueError:
                logger.warning("Valor no numérico en PIB: %s", dato)
                continue

            anio, trimestre = _trimestre_de_fecha(fecha)
            mes_ref = (trimestre - 1) * 3 + 1  # primer mes del trimestre

            # Registro PIB MXN
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": None,
                    "valor": pib_mxn,
                    "unidad": "Millones de pesos",
                    "anio": anio,
                    "mes": mes_ref,
                }
            )

            # Registro PIB USD (calculado)
            tc = tc_promedio_trimestre(anio, trimestre)
            if tc and tc > 0:
                pib_usd = round(pib_mxn / tc, 2)
                records.append(
                    {
                        "indicador_id": None,
                        "nivel_geografico": "nacional",
                        "entidad_clave": "usd_calculado",
                        "valor": pib_usd,
                        "unidad": "Millones de dólares",
                        "anio": anio,
                        "mes": mes_ref,
                    }
                )

        return records
