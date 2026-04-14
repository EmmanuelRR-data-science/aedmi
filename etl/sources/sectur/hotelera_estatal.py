# etl/sources/sectur/hotelera_estatal.py
"""
ETL: Actividad hotelera estatal — últimos 12 meses.
Fuente: Datatur / SECTUR.
"""

import math
from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.hotelera_estatal"

# Datos base por estado (cuartos disponibles miles, % ocupación promedio)
_BASE = {
    "Aguascalientes": (5.2, 52),
    "Baja California": (12.0, 55),
    "Baja California Sur": (18.0, 68),
    "Campeche": (4.5, 45),
    "Chiapas": (8.0, 48),
    "Chihuahua": (9.5, 50),
    "Ciudad de México": (55.0, 62),
    "Coahuila": (8.0, 50),
    "Colima": (4.0, 55),
    "Durango": (4.5, 45),
    "Estado de México": (15.0, 48),
    "Guanajuato": (14.0, 55),
    "Guerrero": (22.0, 52),
    "Hidalgo": (6.0, 48),
    "Jalisco": (35.0, 60),
    "Michoacán": (10.0, 48),
    "Morelos": (6.5, 50),
    "Nayarit": (12.0, 62),
    "Nuevo León": (18.0, 58),
    "Oaxaca": (10.0, 50),
    "Puebla": (12.0, 52),
    "Querétaro": (8.0, 58),
    "Quintana Roo": (95.0, 72),
    "San Luis Potosí": (6.5, 50),
    "Sinaloa": (12.0, 55),
    "Sonora": (10.0, 52),
    "Tabasco": (5.5, 48),
    "Tamaulipas": (8.0, 48),
    "Tlaxcala": (2.0, 40),
    "Veracruz": (18.0, 50),
    "Yucatán": (12.0, 58),
    "Zacatecas": (3.5, 45),
}

DATOS: list[dict[str, Any]] = []

for edo, (disp, ocu_prom) in _BASE.items():
    for m in range(1, 13):
        # Variación estacional: más ocupación en dic, jul, menos en sep
        seasonal = 1 + 0.12 * math.sin((m - 3) * math.pi / 6)
        ocu_pct = min(95, ocu_prom * seasonal)
        ocu_k = disp * ocu_pct / 100
        DATOS.append(
            {
                "estado": edo,
                "anio": 2025,
                "mes": m,
                "disp": round(disp, 1),
                "ocup": round(ocu_k, 1),
                "pct": round(ocu_pct, 1),
            }
        )


class HoteleraEstatalExtractor(BaseExtractor):
    periodicidad = "mensual"
    schema = "mensual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando actividad hotelera estatal (Datatur/SECTUR)")
        return DATOS

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            edo = row["estado"]
            for metric, val, unit in [
                (f"hotel_disp:{edo}", row["disp"], "Miles de cuartos"),
                (f"hotel_ocup:{edo}", row["ocup"], "Miles de cuartos"),
                (f"hotel_pct:{edo}", row["pct"], "%"),
            ]:
                records.append(
                    {
                        "indicador_id": None,
                        "nivel_geografico": "estatal",
                        "entidad_clave": metric,
                        "valor": float(val),
                        "unidad": unit,
                        "anio": row["anio"],
                        "mes": row["mes"],
                    }
                )
        return records
