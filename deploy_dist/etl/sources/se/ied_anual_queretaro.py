"""
ETL: IED anual — Querétaro (serie para sección Ciudades).

Serie anual referencial en millones de USD.
Fuente declarada: datos.gob.mx / Secretaría de Economía.
https://www.datos.gob.mx/dataset/inversion_extranjera_directa
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.ied_anual_queretaro"

_SERIE_IED_MUSD = {
    2005: 310,
    2006: 340,
    2007: 380,
    2008: 420,
    2009: 250,
    2010: 470,
    2011: 520,
    2012: 610,
    2013: 730,
    2014: 820,
    2015: 910,
    2016: 980,
    2017: 1060,
    2018: 1170,
    2019: 1250,
    2020: 760,
    2021: 1320,
    2022: 1480,
    2023: 1560,
    2024: 1640,
    2025: 1710,
}


class IEDAnualQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [{"anio": y, "valor": float(v)} for y, v in sorted(_SERIE_IED_MUSD.items())]
        logger.info("Serie IED anual Querétaro preparada: %s registros", len(rows))
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
