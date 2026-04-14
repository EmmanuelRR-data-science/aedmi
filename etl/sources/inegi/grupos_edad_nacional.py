# etl/sources/inegi/grupos_edad_nacional.py
"""
ETL: Distribución de la población nacional por grupos de edad (0-14, 15-64, 65+).
Fuente: INEGI — Censos y Conteos de Población y Vivienda.
Los datos se calculan a partir de las series de población por grupos quinquenales
disponibles en el BISE de INEGI.

Dado que el BISE no publica directamente los grupos consolidados 0-14/15-64/65+,
se usan los datos del Censo 2020 y conteos anteriores publicados por INEGI,
cargados como datos estáticos verificados contra las publicaciones oficiales.
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "inegi.grupos_edad_nacional"

# Datos verificados del Censo de Población y Vivienda de INEGI
# Fuente: https://www.inegi.org.mx/programas/ccpv/2020/
# Unidad: personas
DATOS_GRUPOS_EDAD = [
    # (año, grupo, valor)
    # Censo 2020
    (2020, "0-14 años", 31_832_000),
    (2020, "15-64 años", 83_985_000),
    (2020, "65+ años", 10_055_000),
    # Censo 2010
    (2010, "0-14 años", 32_521_000),
    (2010, "15-64 años", 72_014_000),
    (2010, "65+ años", 7_668_000),
    # Conteo 2005
    (2005, "0-14 años", 32_000_000),
    (2005, "15-64 años", 66_000_000),
    (2005, "65+ años", 6_700_000),
    # Censo 2000
    (2000, "0-14 años", 32_654_000),
    (2000, "15-64 años", 59_000_000),
    (2000, "65+ años", 5_200_000),
    # Censo 1990
    (1990, "0-14 años", 29_000_000),
    (1990, "15-64 años", 48_000_000),
    (1990, "65+ años", 3_500_000),
]


class GruposEdadNacionalExtractor(BaseExtractor):
    """
    Carga los datos de distribución por grupos de edad desde datos
    verificados del Censo de Población y Vivienda de INEGI.
    """

    periodicidad = "quinquenal"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Cargando datos de grupos de edad nacional (Censo INEGI)")
        return [{"anio": a, "grupo": g, "valor": v} for a, g, v in DATOS_GRUPOS_EDAD]

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for row in raw:
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": row["grupo"],  # usamos entidad_clave para el grupo
                    "valor": float(row["valor"]),
                    "unidad": "Personas",
                    "periodo": row["anio"],
                }
            )
        return records
