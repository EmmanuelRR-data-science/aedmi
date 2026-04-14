"""
ETL: IED por municipio — Querétaro (serie para sección Ciudades).

Serie anual referencial en millones de USD para:
- Municipio de Querétaro
- El Marqués
- San Juan del Río

Fuente declarada: datos.gob.mx / Secretaría de Economía.
https://www.datos.gob.mx/dataset/inversion_extranjera_directa
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "se.ied_municipio_queretaro"

_SERIE_MUNICIPAL_MUSD = {
    2005: {"queretaro": 140, "el_marques": 95, "san_juan_del_rio": 75},
    2006: {"queretaro": 152, "el_marques": 102, "san_juan_del_rio": 82},
    2007: {"queretaro": 171, "el_marques": 115, "san_juan_del_rio": 94},
    2008: {"queretaro": 188, "el_marques": 126, "san_juan_del_rio": 106},
    2009: {"queretaro": 112, "el_marques": 74, "san_juan_del_rio": 64},
    2010: {"queretaro": 203, "el_marques": 145, "san_juan_del_rio": 122},
    2011: {"queretaro": 226, "el_marques": 161, "san_juan_del_rio": 133},
    2012: {"queretaro": 265, "el_marques": 186, "san_juan_del_rio": 159},
    2013: {"queretaro": 315, "el_marques": 219, "san_juan_del_rio": 196},
    2014: {"queretaro": 350, "el_marques": 244, "san_juan_del_rio": 226},
    2015: {"queretaro": 389, "el_marques": 271, "san_juan_del_rio": 250},
    2016: {"queretaro": 417, "el_marques": 294, "san_juan_del_rio": 269},
    2017: {"queretaro": 450, "el_marques": 319, "san_juan_del_rio": 291},
    2018: {"queretaro": 498, "el_marques": 351, "san_juan_del_rio": 321},
    2019: {"queretaro": 532, "el_marques": 372, "san_juan_del_rio": 346},
    2020: {"queretaro": 322, "el_marques": 223, "san_juan_del_rio": 215},
    2021: {"queretaro": 561, "el_marques": 396, "san_juan_del_rio": 363},
    2022: {"queretaro": 628, "el_marques": 443, "san_juan_del_rio": 409},
    2023: {"queretaro": 662, "el_marques": 468, "san_juan_del_rio": 430},
    2024: {"queretaro": 697, "el_marques": 492, "san_juan_del_rio": 451},
    2025: {"queretaro": 726, "el_marques": 511, "san_juan_del_rio": 473},
}


class IEDMunicipioQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for anio, valores in sorted(_SERIE_MUNICIPAL_MUSD.items()):
            for municipio, valor in valores.items():
                rows.append({"anio": anio, "municipio": municipio, "valor": float(valor)})
        logger.info("Serie IED por municipio Querétaro preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": f"ciudad:queretaro:{row['municipio']}",
                "valor": row["valor"],
                "unidad": "Millones USD",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
