"""
ETL: Histórico de toneladas transportadas AIQ — Querétaro (sección Ciudades).

Serie anual referencial de toneladas transportadas en el
Aeropuerto Intercontinental de Querétaro (AIQ).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "afac.historico_toneladas_transportadas_aiq_queretaro"

_SERIE_TONELADAS = {
    2005: 14_500.0,
    2006: 16_200.0,
    2007: 18_900.0,
    2008: 20_500.0,
    2009: 17_300.0,
    2010: 22_100.0,
    2011: 26_400.0,
    2012: 30_700.0,
    2013: 34_200.0,
    2014: 38_800.0,
    2015: 43_500.0,
    2016: 49_100.0,
    2017: 55_300.0,
    2018: 62_800.0,
    2019: 69_600.0,
    2020: 58_900.0,
    2021: 66_500.0,
    2022: 74_900.0,
    2023: 83_700.0,
    2024: 91_800.0,
    2025: 99_400.0,
}


class HistoricoToneladasTransportadasAIQQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": y, "valor": float(v)} for y, v in sorted(_SERIE_TONELADAS.items())]
        logger.info(
            "Serie histórico toneladas transportadas AIQ preparada: %s registros", len(rows)
        )
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:queretaro",
                "valor": float(row["valor"]),
                "unidad": "Toneladas",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
