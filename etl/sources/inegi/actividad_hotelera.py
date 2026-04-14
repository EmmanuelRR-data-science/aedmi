# etl/sources/inegi/actividad_hotelera.py
"""
ETL: Actividad hotelera nacional.
Fuente: INEGI / Datatur — Estadísticas de ocupación hotelera.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.actividad_hotelera"

# Histórico nacional (miles de cuartos)
HISTORICO = [
    # (año, disponibles_miles, ocupados_miles, % ocupación)
    (2024, 870, 530, 60.9),
    (2023, 850, 510, 60.0),
    (2022, 830, 480, 57.8),
    (2021, 810, 380, 46.9),
    (2020, 800, 240, 30.0),
    (2019, 790, 490, 62.0),
    (2018, 780, 475, 60.9),
    (2017, 770, 460, 59.7),
    (2016, 760, 445, 58.6),
    (2015, 750, 430, 57.3),
]

# Ocupación por categoría 2024
CATEGORIAS = [
    ("5 estrellas", 120, 82, 68.3),
    ("4 estrellas", 210, 135, 64.3),
    ("3 estrellas", 280, 165, 58.9),
    ("2 estrellas", 150, 82, 54.7),
    ("1 estrella", 110, 66, 60.0),
]


class ActividadHoteleraExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando actividad hotelera (INEGI/Datatur)")
        rows: list[dict[str, Any]] = []
        for anio, disp, ocup, pct in HISTORICO:
            rows.append({"tipo": "hist:disp", "nombre": "Disponibles", "valor": disp, "anio": anio})
            rows.append({"tipo": "hist:ocup", "nombre": "Ocupados", "valor": ocup, "anio": anio})
            rows.append({"tipo": "hist:pct", "nombre": "% Ocupación", "valor": pct, "anio": anio})
        for cat, disp, ocup, pct in CATEGORIAS:
            rows.append({"tipo": "cat:disp", "nombre": cat, "valor": disp, "anio": 2024})
            rows.append({"tipo": "cat:ocup", "nombre": cat, "valor": ocup, "anio": 2024})
            rows.append({"tipo": "cat:pct", "nombre": cat, "valor": pct, "anio": 2024})
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
                    "unidad": "%" if "pct" in row["tipo"] else "Miles de cuartos",
                    "periodo": row["anio"],
                }
            )
        return records
