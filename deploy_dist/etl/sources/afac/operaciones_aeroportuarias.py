# etl/sources/afac/operaciones_aeroportuarias.py
"""
ETL: Operaciones aeroportuarias nacionales.
Fuente: AFAC / Datatur — Estadísticas de aviación civil.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "afac.operaciones_aeroportuarias"

# Total de operaciones por año (miles)
TOTAL_ANUAL = [
    (2024, 1_850),
    (2023, 1_780),
    (2022, 1_650),
    (2021, 1_320),
    (2020, 890),
    (2019, 1_810),
]

# Por grupo aeroportuario 2024 (miles de operaciones)
GRUPOS = [
    ("OMA", 320),
    ("GAP", 410),
    ("ASUR", 380),
    ("AICM/AIFA", 450),
    ("Otros", 290),
]

# Top 10 aeropuertos 2024 (miles de operaciones)
TOP_AEROPUERTOS = [
    ("AICM (MEX)", 380),
    ("Cancún (CUN)", 210),
    ("Guadalajara (GDL)", 165),
    ("Monterrey (MTY)", 155),
    ("Tijuana (TIJ)", 95),
    ("AIFA (NLU)", 70),
    ("Los Cabos (SJD)", 68),
    ("Puerto Vallarta (PVR)", 62),
    ("Mérida (MID)", 48),
    ("Chihuahua (CUU)", 38),
]


class OperacionesAeroportuariasExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando operaciones aeroportuarias (AFAC)")
        rows: list[dict[str, Any]] = []
        for anio, total in TOTAL_ANUAL:
            rows.append({"tipo": "total", "nombre": "Total", "valor": total, "anio": anio})
        for nombre, valor in GRUPOS:
            rows.append({"tipo": "grupo", "nombre": nombre, "valor": valor, "anio": 2024})
        for nombre, valor in TOP_AEROPUERTOS:
            rows.append({"tipo": "apto", "nombre": nombre, "valor": valor, "anio": 2024})
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": f"{row['tipo']}:{row['nombre']}",
                    "valor": float(row["valor"]),
                    "unidad": "Miles de operaciones",
                    "periodo": row["anio"],
                }
            )
        return records
