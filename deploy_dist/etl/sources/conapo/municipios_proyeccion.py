# etl/sources/conapo/municipios_proyeccion.py
"""
ETL: Proyección poblacional municipal (total, hombres, mujeres).
Fuente: CONAPO (proyecciones municipales).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger
from sources.conapo.municipios_poblacion import (
    _URL_HOMBRES,
    _URL_MUJERES,
    _URL_TOTAL,
    _extract_sheet,
)

logger = get_logger(__name__)

INDICADOR_CLAVE = "conapo.municipios_proyeccion"
_YEARS = (2026, 2027, 2028, 2029, 2030)


class MunicipiosProyeccionExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        total = _extract_sheet(_URL_TOTAL, "total_proy", _YEARS)
        hombres = _extract_sheet(_URL_HOMBRES, "hombres_proy", _YEARS)
        mujeres = _extract_sheet(_URL_MUJERES, "mujeres_proy", _YEARS)

        keys = sorted(set(total.keys()) & set(hombres.keys()) & set(mujeres.keys()))
        rows: list[dict[str, Any]] = []
        for estado, municipio in keys:
            for y in _YEARS:
                t = total.get((estado, municipio), {}).get(y)
                h = hombres.get((estado, municipio), {}).get(y)
                m = mujeres.get((estado, municipio), {}).get(y)
                if t is None or h is None or m is None:
                    continue
                rows.append(
                    {
                        "estado": estado,
                        "municipio": municipio,
                        "anio": y,
                        "total": t,
                        "hombres": h,
                        "mujeres": m,
                    }
                )
        logger.info("Registros municipales proyección: %s", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for row in raw:
            estado = row["estado"]
            municipio = row["municipio"]
            anio = row["anio"]
            for sexo, valor in (
                ("total", row["total"]),
                ("hombres", row["hombres"]),
                ("mujeres", row["mujeres"]),
            ):
                records.append(
                    {
                        "indicador_id": None,
                        "nivel_geografico": "municipal",
                        "entidad_clave": f"mun_proy:{estado}:{municipio}:{sexo}",
                        "valor": float(valor),
                        "unidad": "Personas",
                        "periodo": anio,
                    }
                )
        return records
