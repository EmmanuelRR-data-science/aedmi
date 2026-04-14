"""
ETL: Crecimiento anual de pasajeros AIQ — Querétaro (sección Ciudades).

Variación porcentual anual (YoY) de pasajeros del Aeropuerto
Intercontinental de Querétaro (AIQ), calculada a partir de la serie
anual de pasajeros.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger
from sources.afac.pasajeros_anuales_aiq_queretaro import _SERIE_PASAJEROS

logger = get_logger(__name__)

INDICADOR_CLAVE = "afac.crecimiento_anual_pasajeros_aiq_queretaro"


class CrecimientoAnualPasajerosAIQQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        years = sorted(_SERIE_PASAJEROS.keys())
        rows: list[dict[str, Any]] = []
        for i in range(1, len(years)):
            prev_year = years[i - 1]
            curr_year = years[i]
            prev_val = float(_SERIE_PASAJEROS[prev_year])
            curr_val = float(_SERIE_PASAJEROS[curr_year])
            if prev_val <= 0:
                growth = 0.0
            else:
                growth = ((curr_val - prev_val) / prev_val) * 100.0
            rows.append({"anio": curr_year, "valor": growth})
        logger.info("Serie crecimiento anual pasajeros AIQ preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:queretaro",
                "valor": float(row["valor"]),
                "unidad": "Porcentaje",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
