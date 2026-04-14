# etl/sources/inegi/poblacion_sexo_nacional.py
"""ETL: Distribución de la población nacional por sexo — INEGI BISE series 1002000002/1002000003."""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger
from sources.inegi.client import fetch_serie

logger = get_logger(__name__)

SERIE_HOMBRES = "1002000002"
SERIE_MUJERES = "1002000003"
INDICADOR_CLAVE = "inegi.poblacion_sexo_nacional"


class PoblacionSexoNacionalExtractor(BaseExtractor):
    periodicidad = "quinquenal"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        logger.info("Extrayendo población por sexo de INEGI BISE")
        hombres = fetch_serie(SERIE_HOMBRES, fuente="BISE")
        mujeres = fetch_serie(SERIE_MUJERES, fuente="BISE")
        return [{"hombres": hombres, "mujeres": mujeres}]

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not raw:
            return []
        records = []
        for row in raw[0]["hombres"]:
            if not row.get("OBS_VALUE") or row["OBS_VALUE"] in ("N/E", "N/D", ""):
                continue
            try:
                anio = int(row["TIME_PERIOD"])
                valor = float(str(row["OBS_VALUE"]).replace(",", ""))
            except (ValueError, TypeError):
                continue
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": "Hombres",
                    "valor": round(valor, 0),
                    "unidad": "Personas",
                    "periodo": anio,
                }
            )
        for row in raw[0]["mujeres"]:
            if not row.get("OBS_VALUE") or row["OBS_VALUE"] in ("N/E", "N/D", ""):
                continue
            try:
                anio = int(row["TIME_PERIOD"])
                valor = float(str(row["OBS_VALUE"]).replace(",", ""))
            except (ValueError, TypeError):
                continue
            records.append(
                {
                    "indicador_id": None,
                    "nivel_geografico": "nacional",
                    "entidad_clave": "Mujeres",
                    "valor": round(valor, 0),
                    "unidad": "Personas",
                    "periodo": anio,
                }
            )
        return records
