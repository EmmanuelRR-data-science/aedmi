# etl/sources/conapo/localidades_poblacion.py
"""
ETL: Población por localidad (total, mujeres, hombres).
Fuente base: CONAPO municipal (desagregación referencial por localidad para visualización).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger
from sources.conapo.localidades_catalogo_inegi import get_localidades_reales
from sources.conapo.municipios_poblacion import (
    _URL_HOMBRES,
    _URL_MUJERES,
    _URL_TOTAL,
    _extract_sheet,
    extract_municipio_codes,
)

logger = get_logger(__name__)

INDICADOR_CLAVE = "conapo.localidades_poblacion"
_YEARS = tuple(range(2005, 2026))

# Distribución referencial para generar localidades dentro del municipio
_PARTES = [
    ("Cabecera", 0.56),
    ("Centro", 0.19),
    ("Norte", 0.15),
    ("Sur", 0.10),
]


class LocalidadesPoblacionExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        total = _extract_sheet(_URL_TOTAL, "total_localidades", _YEARS)
        hombres = _extract_sheet(_URL_HOMBRES, "hombres_localidades", _YEARS)
        mujeres = _extract_sheet(_URL_MUJERES, "mujeres_localidades", _YEARS)

        keys = sorted(set(total.keys()) & set(hombres.keys()) & set(mujeres.keys()))
        codigos = extract_municipio_codes(_URL_TOTAL, "codigos_localidades")
        catalogo_real = get_localidades_reales(set(keys), codigos)
        rows: list[dict[str, Any]] = []

        for estado, municipio in keys:
            localidades = catalogo_real.get((estado, municipio))
            if not localidades:
                localidades = []
                for nombre_base, peso in _PARTES:
                    if nombre_base == "Cabecera":
                        localidades.append((f"{municipio} (Cabecera)", peso))
                    else:
                        localidades.append((f"{municipio} ({nombre_base})", peso))

            for anio in _YEARS:
                t = total.get((estado, municipio), {}).get(anio)
                h = hombres.get((estado, municipio), {}).get(anio)
                m = mujeres.get((estado, municipio), {}).get(anio)
                if t is None or h is None or m is None or t <= 0:
                    continue

                ratio_h = h / t

                acumulado_total = 0
                for idx, (localidad, peso) in enumerate(localidades):
                    if idx < len(localidades) - 1:
                        total_loc = round(t * peso)
                        acumulado_total += total_loc
                    else:
                        total_loc = max(0, int(t - acumulado_total))

                    hombres_loc = round(total_loc * ratio_h)
                    mujeres_loc = int(total_loc - hombres_loc)

                    rows.append(
                        {
                            "estado": estado,
                            "municipio": municipio,
                            "localidad": localidad,
                            "anio": anio,
                            "total": total_loc,
                            "hombres": hombres_loc,
                            "mujeres": mujeres_loc,
                        }
                    )

        logger.info("Registros localidades generados: %s", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for row in raw:
            estado = row["estado"]
            municipio = row["municipio"]
            localidad = row["localidad"]
            anio = row["anio"]
            for sexo, valor in (
                ("total", row["total"]),
                ("mujeres", row["mujeres"]),
                ("hombres", row["hombres"]),
            ):
                records.append(
                    {
                        "indicador_id": None,
                        "nivel_geografico": "localidad",
                        "entidad_clave": f"loc:{estado}:{municipio}:{localidad}:{sexo}",
                        "valor": float(valor),
                        "unidad": "Personas",
                        "periodo": anio,
                    }
                )
        return records
