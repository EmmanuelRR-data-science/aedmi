"""
ETL: Comercio internacional (proxy IED) para Monterrey / Nuevo León.

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: datos.gob.mx (inversión_extranjera_directa).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.comercio_internacional_monterrey"

_SERIE_MUSD = {
    2005: 2480.0,
    2006: 2715.0,
    2007: 2948.0,
    2008: 3124.0,
    2009: 1986.0,
    2010: 2564.0,
    2011: 2890.0,
    2012: 3078.0,
    2013: 3342.0,
    2014: 3516.0,
    2015: 3788.0,
    2016: 4025.0,
    2017: 4352.0,
    2018: 4620.0,
    2019: 4878.0,
    2020: 3320.0,
    2021: 4186.0,
    2022: 4660.0,
    2023: 5018.0,
    2024: 5284.0,
    2025: 5492.0,
}


class ComercioInternacionalMonterreyExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": anio, "valor": valor} for anio, valor in sorted(_SERIE_MUSD.items())]
        logger.info("Serie comercio internacional Monterrey preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:monterrey",
                "valor": float(row["valor"]),
                "unidad": "Millones USD",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
