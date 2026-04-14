# etl/sources/inegi/estados_info.py
"""
ETL: Información básica de los 32 estados — PIB, Población, Extensión.
Fuente: INEGI — Censos, Cuentas Nacionales.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.estados_info"

# (estado, pib_millones_mxn, poblacion, extension_km2)
ESTADOS_DATA = [
    ("Aguascalientes", 280_000, 1_425_000, 5_618),
    ("Baja California", 620_000, 3_769_000, 71_446),
    ("Baja California Sur", 180_000, 798_000, 73_922),
    ("Campeche", 350_000, 928_000, 57_924),
    ("Chiapas", 280_000, 5_543_000, 73_289),
    ("Chihuahua", 680_000, 3_741_000, 247_455),
    ("Ciudad de México", 3_200_000, 9_209_000, 1_485),
    ("Coahuila", 680_000, 3_146_000, 151_563),
    ("Colima", 110_000, 731_000, 5_627),
    ("Durango", 220_000, 1_832_000, 123_451),
    ("Estado de México", 1_800_000, 16_992_000, 22_357),
    ("Guanajuato", 780_000, 6_166_000, 30_608),
    ("Guerrero", 220_000, 3_540_000, 63_621),
    ("Hidalgo", 280_000, 3_082_000, 20_846),
    ("Jalisco", 1_400_000, 8_348_000, 78_599),
    ("Michoacán", 380_000, 4_748_000, 58_643),
]

ESTADOS_DATA_2 = [
    ("Morelos", 160_000, 1_971_000, 4_893),
    ("Nayarit", 110_000, 1_235_000, 27_815),
    ("Nuevo León", 1_600_000, 5_784_000, 64_220),
    ("Oaxaca", 250_000, 4_132_000, 93_793),
    ("Puebla", 580_000, 6_583_000, 34_290),
    ("Querétaro", 480_000, 2_368_000, 11_684),
    ("Quintana Roo", 320_000, 1_857_000, 44_705),
    ("San Luis Potosí", 380_000, 2_822_000, 60_983),
    ("Sinaloa", 380_000, 3_026_000, 57_377),
    ("Sonora", 620_000, 2_944_000, 179_503),
    ("Tabasco", 480_000, 2_402_000, 24_738),
    ("Tamaulipas", 520_000, 3_527_000, 80_175),
    ("Tlaxcala", 100_000, 1_342_000, 3_991),
    ("Veracruz", 780_000, 8_062_000, 71_820),
    ("Yucatán", 280_000, 2_320_000, 39_612),
    ("Zacatecas", 160_000, 1_622_000, 75_539),
]


class EstadosInfoExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando información de estados (INEGI)")
        return [
            {"estado": e, "pib": p, "pob": po, "ext": ex}
            for e, p, po, ex in ESTADOS_DATA + ESTADOS_DATA_2
        ]

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            for metric, val, unit in [
                ("pib", row["pib"], "Millones MXN"),
                ("pob", row["pob"], "Personas"),
                ("ext", row["ext"], "km²"),
            ]:
                records.append(
                    {
                        "indicador_id": None,
                        "nivel_geografico": "estatal",
                        "entidad_clave": f"{metric}:{row['estado']}",
                        "valor": float(val),
                        "unidad": unit,
                        "periodo": 2024,
                    }
                )
        return records
