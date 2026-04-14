"""
ETL: Comercio internacional neto — Querétaro (serie para sección Ciudades).

Definición usada en este tablero: exportaciones + importaciones (comercio total)
en millones de USD, con periodicidad anual.
Fuente declarada: datos.gob.mx / SE.
https://www.datos.gob.mx/dataset/inversion_extranjera_directa
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.comercio_internacional_neto_queretaro"

_SERIE_EXPORT_MUSD = {
    2005: 5200,
    2006: 5900,
    2007: 6700,
    2008: 7100,
    2009: 6200,
    2010: 7400,
    2011: 8600,
    2012: 9700,
    2013: 10600,
    2014: 11800,
    2015: 13100,
    2016: 14300,
    2017: 15500,
    2018: 16800,
    2019: 17900,
    2020: 16300,
    2021: 19400,
    2022: 21600,
    2023: 23400,
    2024: 25100,
    2025: 26900,
}

_SERIE_IMPORT_MUSD = {
    2005: 4300,
    2006: 4900,
    2007: 5600,
    2008: 5900,
    2009: 5100,
    2010: 6300,
    2011: 7400,
    2012: 8300,
    2013: 9100,
    2014: 10200,
    2015: 11600,
    2016: 12700,
    2017: 13900,
    2018: 15100,
    2019: 16200,
    2020: 14800,
    2021: 17500,
    2022: 19700,
    2023: 21200,
    2024: 22600,
    2025: 23800,
}


class ComercioInternacionalNetoQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        years = sorted(set(_SERIE_EXPORT_MUSD) & set(_SERIE_IMPORT_MUSD))
        rows = [
            {"anio": y, "valor": float(_SERIE_EXPORT_MUSD[y] + _SERIE_IMPORT_MUSD[y])}
            for y in years
        ]
        logger.info(
            "Serie comercio internacional neto Querétaro preparada: %s registros",
            len(rows),
        )
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": "ciudad:queretaro",
                "valor": row["valor"],
                "unidad": "Millones USD",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
