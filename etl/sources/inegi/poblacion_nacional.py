# etl/sources/inegi/poblacion_nacional.py
"""ETL: Población total nacional — INEGI BISE serie 1002000001 (censos y conteos)."""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger
from sources.inegi.client import fetch_serie

logger = get_logger(__name__)

SERIE_ID = "1002000001"
INDICADOR_CLAVE = "inegi.poblacion_total_nacional"


class PoblacionNacionalExtractor(BaseExtractor):
    """
    Extrae la población total nacional de México desde los censos y conteos
    de población publicados por INEGI (BISE serie 1002000001).
    Periodicidad: quinquenal/decenal según el año del censo.
    Los datos se almacenan en anual.datos usando el año del censo como período.
    """

    periodicidad = "quinquenal"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Extrayendo población total nacional de INEGI BISE serie %s", SERIE_ID)
        return fetch_serie(SERIE_ID, fuente="BISE")

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            obs_value = row.get("OBS_VALUE")
            time_period = row.get("TIME_PERIOD", "")

            if not obs_value or obs_value in ("N/E", "N/D", "", None):
                continue

            try:
                anio = int(time_period)
                valor = float(str(obs_value).replace(",", ""))
            except (ValueError, TypeError):
                logger.warning("Dato inválido: periodo=%s valor=%s", time_period, obs_value)
                continue

            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": None,
                    "valor": round(valor, 0),
                    "unidad": "Personas",
                    "periodo": anio,
                }
            )

        # Ordenar por año ascendente
        records.sort(key=lambda r: r["periodo"])
        return records
