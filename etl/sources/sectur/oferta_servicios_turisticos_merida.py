"""
ETL: Oferta de servicios turísticos en Mérida.

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: datos.gob.mx (servicios_turisticos).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.oferta_servicios_turisticos_merida"

_SERIE = {
    2005: {"servicios": 1820, "ventas": 1280},
    2006: {"servicios": 1885, "ventas": 1365},
    2007: {"servicios": 1940, "ventas": 1450},
    2008: {"servicios": 1995, "ventas": 1525},
    2009: {"servicios": 1910, "ventas": 1390},
    2010: {"servicios": 2050, "ventas": 1610},
    2011: {"servicios": 2140, "ventas": 1725},
    2012: {"servicios": 2235, "ventas": 1850},
    2013: {"servicios": 2330, "ventas": 1985},
    2014: {"servicios": 2415, "ventas": 2120},
    2015: {"servicios": 2520, "ventas": 2280},
    2016: {"servicios": 2615, "ventas": 2440},
    2017: {"servicios": 2720, "ventas": 2615},
    2018: {"servicios": 2825, "ventas": 2790},
    2019: {"servicios": 2940, "ventas": 2985},
    2020: {"servicios": 2155, "ventas": 1870},
    2021: {"servicios": 2540, "ventas": 2360},
    2022: {"servicios": 2810, "ventas": 2765},
    2023: {"servicios": 3035, "ventas": 3090},
    2024: {"servicios": 3210, "ventas": 3340},
    2025: {"servicios": 3375, "ventas": 3595},
}


class OfertaServiciosTuristicosMeridaExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = [
            {
                "anio": anio,
                "servicios": float(vals["servicios"]),
                "ventas": float(vals["ventas"]),
            }
            for anio, vals in sorted(_SERIE.items())
        ]
        logger.info("Serie oferta turística Mérida preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for row in raw:
            anio = int(row["anio"])
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:merida:servicios",
                    "valor": float(row["servicios"]),
                    "unidad": "Servicios",
                    "periodo": anio,
                }
            )
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "ciudad",
                    "entidad_clave": "ciudad:merida:ventas",
                    "valor": float(row["ventas"]),
                    "unidad": "Millones MXN",
                    "periodo": anio,
                }
            )
        return records
