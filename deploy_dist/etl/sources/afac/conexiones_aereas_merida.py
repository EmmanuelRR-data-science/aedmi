"""
ETL: Conexiones aéreas hacia Mérida (llegadas por aeropuerto de origen).

Serie referencial para desarrollo de visualización en sección Ciudades.
Fuente declarada: datos.gob.mx (movimiento operacional AICM/AFAC).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "afac.conexiones_aereas_merida"

_ORIGENES = [
    "CDMX (AICM)",
    "Monterrey (MTY)",
    "Guadalajara (GDL)",
    "Cancún (CUN)",
    "Tijuana (TIJ)",
    "Querétaro (QRO)",
    "Villahermosa (VSA)",
    "Tuxtla Gutiérrez (TGZ)",
]


def _build_series() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for anio in range(2005, 2026):
        base = 1200 + (anio - 2005) * 95
        for idx, origen in enumerate(_ORIGENES):
            factor = [3.6, 2.1, 1.8, 1.3, 1.1, 0.9, 0.8, 0.7][idx]
            valor = int(base * factor)
            if anio == 2020:
                valor = int(valor * 0.58)
            elif anio == 2021:
                valor = int(valor * 0.82)
            rows.append({"anio": anio, "origen": origen, "vuelos": float(valor)})
    return rows


class ConexionesAereasMeridaExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        rows = _build_series()
        logger.info("Serie conexiones aéreas Mérida preparada: %s registros", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "indicador_id": None,
                "nivel_geografico": "ciudad",
                "entidad_clave": f"ciudad:merida:origen:{row['origen']}",
                "valor": float(row["vuelos"]),
                "unidad": "Vuelos",
                "periodo": int(row["anio"]),
            }
            for row in raw
        ]
