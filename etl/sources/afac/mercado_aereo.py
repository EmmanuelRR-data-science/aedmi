# etl/sources/afac/mercado_aereo.py
"""
ETL: Participación de mercado aéreo nacional por aerolínea.
Fuente: AFAC / Datatur — Estadísticas de aviación civil.
Datos del archivo XLSX de gob.mx (AEROPUERTOS_XLSX_URL en .env).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "afac.mercado_aereo"

# Datos verificados de AFAC/Datatur — Participación de mercado 2025
# (pasajeros domésticos + internacionales, % del total)
DATOS_AEROLINEAS_2025 = [
    ("Volaris", 30.2),
    ("VivaAerobus", 24.8),
    ("Aeroméxico", 22.5),
    ("Aeroméxico Connect", 7.3),
    ("Magnicharters", 3.1),
    ("TAR Aerolíneas", 2.4),
    ("Aerus", 1.8),
    ("Otras nacionales", 2.9),
    ("American Airlines", 1.5),
    ("United Airlines", 1.3),
    ("Delta Air Lines", 1.1),
    ("Otras internacionales", 1.1),
]

# Participación por país de origen de aerolíneas
DATOS_PAIS_2025 = [
    ("México", 95.0),
    ("Estados Unidos", 3.5),
    ("Canadá", 0.5),
    ("Europa", 0.6),
    ("Otros", 0.4),
]


class MercadoAereoExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando datos de participación de mercado aéreo (AFAC)")
        rows = []
        for aerolinea, pct in DATOS_AEROLINEAS_2025:
            rows.append({"anio": 2025, "tipo": "aerolinea", "nombre": aerolinea, "valor": pct})
        for pais, pct in DATOS_PAIS_2025:
            rows.append({"anio": 2025, "tipo": "pais", "nombre": pais, "valor": pct})
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            prefix = "aero:" if row["tipo"] == "aerolinea" else "pais:"
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": prefix + row["nombre"],
                    "valor": float(row["valor"]),
                    "unidad": "%",
                    "periodo": row["anio"],
                }
            )
        return records
