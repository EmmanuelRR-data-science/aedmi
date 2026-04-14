# etl/sources/banxico/tipo_cambio.py
"""ETL: Tipo de cambio USD/MXN FIX — Banxico SIE SF43718."""

from datetime import datetime
from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger
from sources.banxico.client import fetch_serie

logger = get_logger(__name__)

# Banxico SIE: tipo de cambio FIX (pesos por dólar)
SERIE_ID = "SF43718"
INDICADOR_CLAVE = "banxico.tipo_cambio_usd_mxn"


class TipoCambioExtractor(BaseExtractor):
    periodicidad = "diario"
    schema = "diario"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0  # se asigna desde la BD al registrar

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Extrayendo tipo de cambio USD/MXN de Banxico SIE %s", SERIE_ID)
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
                logger.warning("Valor no numérico en tipo de cambio: %s", dato)
                continue

            # Banxico fecha formato: DD/MM/YYYY
            try:
                fecha = datetime.strptime(row["fecha"], "%d/%m/%Y").date()
            except ValueError:
                logger.warning("Fecha inválida: %s", row["fecha"])
                continue

            records.append(
                {
                    "indicador_id": None,  # se resuelve en load via clave
                    "nivel_geografico": "nacional",
                    "entidad_clave": None,
                    "valor": valor,
                    "unidad": "Pesos por dólar",
                    "fecha": fecha,
                }
            )
        return records
