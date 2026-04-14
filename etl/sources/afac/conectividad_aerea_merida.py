"""
ETL: Conectividad aérea de Mérida (total anual de operaciones).

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: AFAC (estadísticas aeroportuarias).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "afac.conectividad_aerea_merida"

_SERIE = {
    2005: 22110,
    2006: 22980,
    2007: 23840,
    2008: 24530,
    2009: 22720,
    2010: 24160,
    2011: 25210,
    2012: 26480,
    2013: 27850,
    2014: 29120,
    2015: 30640,
    2016: 32290,
    2017: 33940,
    2018: 35510,
    2019: 36880,
    2020: 21120,
    2021: 28950,
    2022: 33210,
    2023: 35680,
    2024: 37240,
    2025: 38510,
}


class ConectividadAereaMeridaExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": anio, "valor": float(valor)} for anio, valor in sorted(_SERIE.items())]
        logger.info("Serie conectividad aérea Mérida preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:merida",
                "valor": float(row["valor"]),
                "unidad": "Operaciones",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
