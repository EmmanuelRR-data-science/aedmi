# etl/sources/banxico/inflacion.py
"""ETL: Inflación INPC variación anual — Banxico SIE SP1."""

from datetime import datetime
from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger
from sources.banxico.client import fetch_serie

logger = get_logger(__name__)

SERIE_ID = "SP30578"  # Variación anual del INPC (inflación general %)
INDICADOR_CLAVE = "banxico.inflacion_inpc_anual"


class InflacionExtractor(BaseExtractor):
    periodicidad = "mensual"
    schema = "mensual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Extrayendo inflación INPC de Banxico SIE %s", SERIE_ID)
        return fetch_serie(SERIE_ID)

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            dato = row.get("dato", "N/E")
            if dato in ("N/E", "N/D", "", None):
                continue
            try:
                valor = float(str(dato).replace(",", ""))
            except ValueError:
                logger.warning("Valor no numérico en inflación: %s", dato)
                continue

            try:
                fecha = datetime.strptime(row["fecha"], "%d/%m/%Y").date()
            except ValueError:
                logger.warning("Fecha inválida: %s", row["fecha"])
                continue

            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": None,
                    "valor": valor,
                    "unidad": "%",
                    "anio": fecha.year,
                    "mes": fecha.month,
                }
            )
        return records
