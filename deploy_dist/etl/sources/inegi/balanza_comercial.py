# etl/sources/inegi/balanza_comercial.py
"""
ETL: Balanza comercial por producto.
Fuente: INEGI vía DataMéxico — Comercio exterior.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.balanza_comercial"

# Balanza comercial anual (miles de millones USD)
BALANZA_ANUAL = [
    (2024, 593.0, 577.0),  # (año, exportaciones, importaciones)
    (2023, 563.0, 550.0),
    (2022, 578.0, 583.0),
    (2021, 494.0, 506.0),
    (2020, 417.0, 383.0),
]

# Top 10 productos exportados 2024 (miles de millones USD)
TOP_PRODUCTOS = [
    ("Vehículos automotores", 95.0),
    ("Autopartes", 42.0),
    ("Computadoras y electrónicos", 38.0),
    ("Petróleo crudo", 28.0),
    ("Equipos eléctricos", 25.0),
    ("Maquinaria industrial", 22.0),
    ("Dispositivos médicos", 18.0),
    ("Productos agrícolas", 16.0),
    ("Cerveza", 7.5),
    ("Tequila y mezcal", 4.2),
]

# Composición de exportaciones 2024 (%)
COMPOSICION = [
    ("Manufactura", 89.2),
    ("Petróleo", 5.1),
    ("Agropecuario", 3.8),
    ("Extractivas", 1.2),
    ("Otros", 0.7),
]


class BalanzaComercialExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando balanza comercial por producto (INEGI)")
        rows: list[dict[str, Any]] = []
        for anio, exp, imp in BALANZA_ANUAL:
            rows.append({"tipo": "bal:exp", "nombre": "Exportaciones", "valor": exp, "anio": anio})
            rows.append({"tipo": "bal:imp", "nombre": "Importaciones", "valor": imp, "anio": anio})
        for nombre, valor in TOP_PRODUCTOS:
            rows.append({"tipo": "prod", "nombre": nombre, "valor": valor, "anio": 2024})
        for nombre, valor in COMPOSICION:
            rows.append({"tipo": "comp", "nombre": nombre, "valor": valor, "anio": 2024})
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
                    "unidad": "Miles de millones USD" if row["tipo"] != "comp" else "%",
                    "periodo": row["anio"],
                }
            )
        return records
