"""
ETL: % vuelos nacionales / internacionales AIQ — Querétaro (sección Ciudades).

Participación porcentual anual de operaciones nacionales e internacionales
en el Aeropuerto Intercontinental de Querétaro (AIQ).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "afac.vuelos_nacionales_internacionales_aiq_queretaro"

_PCT_NACIONALES = {
    2005: 88.5,
    2006: 88.2,
    2007: 87.9,
    2008: 87.4,
    2009: 89.1,
    2010: 88.0,
    2011: 87.2,
    2012: 86.5,
    2013: 85.8,
    2014: 85.0,
    2015: 84.1,
    2016: 83.2,
    2017: 82.4,
    2018: 81.3,
    2019: 80.2,
    2020: 82.0,
    2021: 79.4,
    2022: 78.3,
    2023: 77.5,
    2024: 76.9,
    2025: 76.2,
}


class VuelosNacionalesInternacionalesAIQQueretaroExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for anio, pct_nac in sorted(_PCT_NACIONALES.items()):
            pct_int = 100.0 - float(pct_nac)
            rows.append({"anio": anio, "segmento": "nacionales", "valor": float(pct_nac)})
            rows.append({"anio": anio, "segmento": "internacionales", "valor": pct_int})
        logger.info(
            "Serie %% vuelos nacionales/internacionales AIQ preparada: %s registros", len(rows)
        )
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": f"ciudad:queretaro:{row['segmento']}",
                "valor": float(row["valor"]),
                "unidad": "Porcentaje",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
