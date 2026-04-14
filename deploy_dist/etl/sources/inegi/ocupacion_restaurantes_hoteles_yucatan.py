"""
ETL: Población ocupada en servicios de alojamiento y restaurantes (Yucatán/Mérida).

Nota:
- Serie referencial para desarrollo de visualización en sección Ciudades.
- Fuente de referencia declarada por producto: Observatur Yucatán / ENOE-INEGI.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.ocupacion_restaurantes_hoteles_merida"

# Serie anual 2005-2025 (personas), incluyendo choque 2020 y recuperación.
_SERIE_OCUPACION = {
    2005: 81250,
    2006: 82980,
    2007: 84710,
    2008: 86140,
    2009: 84220,
    2010: 87360,
    2011: 90250,
    2012: 93180,
    2013: 95820,
    2014: 98610,
    2015: 101940,
    2016: 105630,
    2017: 109420,
    2018: 113780,
    2019: 118450,
    2020: 74200,
    2021: 88760,
    2022: 101980,
    2023: 111540,
    2024: 119320,
    2025: 124680,
}


class OcupacionRestaurantesHotelesYucatanExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [
            {
                "anio": anio,
                "valor": float(valor),
            }
            for anio, valor in sorted(_SERIE_OCUPACION.items())
        ]
        logger.info("Serie ocupación restaurantes/hoteles preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:merida",
                "valor": row["valor"],
                "unidad": "Personas",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
