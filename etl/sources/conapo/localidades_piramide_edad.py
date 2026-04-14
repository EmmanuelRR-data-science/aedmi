# etl/sources/conapo/localidades_piramide_edad.py
"""
ETL: Distribución de población por edad y sexo para localidades.
Fuente base: CONAPO municipal (desagregación referencial a nivel localidad).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger
from sources.conapo.localidades_catalogo_inegi import get_localidades_reales
from sources.conapo.localidades_poblacion import _PARTES
from sources.conapo.municipios_piramide_edad import (
    _DESAGREGACION,
    _URL_0_14,
    _URL_15_64,
    _URL_65_MAS,
)
from sources.conapo.municipios_poblacion import (
    _URL_HOMBRES,
    _URL_MUJERES,
    _URL_TOTAL,
    _extract_sheet,
    extract_municipio_codes,
)

logger = get_logger(__name__)

INDICADOR_CLAVE = "conapo.localidades_piramide_edad"
_YEARS = (2020, 2025)


class LocalidadesPiramideEdadExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        muni_total = _extract_sheet(_URL_TOTAL, "total_localidades_piramide", _YEARS)
        muni_h = _extract_sheet(_URL_HOMBRES, "hombres_localidades_piramide", _YEARS)
        muni_m = _extract_sheet(_URL_MUJERES, "mujeres_localidades_piramide", _YEARS)
        age_0_14 = _extract_sheet(_URL_0_14, "grupo_0_14_localidades", _YEARS)
        age_15_64 = _extract_sheet(_URL_15_64, "grupo_15_64_localidades", _YEARS)
        age_65 = _extract_sheet(_URL_65_MAS, "grupo_65_localidades", _YEARS)

        keys = sorted(
            set(muni_total.keys())
            & set(muni_h.keys())
            & set(muni_m.keys())
            & set(age_0_14.keys())
            & set(age_15_64.keys())
            & set(age_65.keys())
        )
        codigos = extract_municipio_codes(_URL_TOTAL, "codigos_localidades_piramide")
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
                t = muni_total.get((estado, municipio), {}).get(anio)
                h = muni_h.get((estado, municipio), {}).get(anio)
                m = muni_m.get((estado, municipio), {}).get(anio)
                a0 = age_0_14.get((estado, municipio), {}).get(anio)
                a1 = age_15_64.get((estado, municipio), {}).get(anio)
                a2 = age_65.get((estado, municipio), {}).get(anio)
                if None in (t, h, m, a0, a1, a2) or not t:
                    continue

                ratio_h = h / t
                grupos_muni = {"0-14": int(a0), "15-64": int(a1), "65+": int(a2)}

                total_asignado = 0
                for idx, (localidad, peso) in enumerate(localidades):
                    if idx < len(localidades) - 1:
                        total_loc = round(t * peso)
                        total_asignado += total_loc
                    else:
                        total_loc = max(0, int(t - total_asignado))

                    share = (total_loc / t) if t > 0 else 0.0
                    h_loc_total = round(total_loc * ratio_h)
                    loc_ratio_h = (h_loc_total / total_loc) if total_loc > 0 else 0.5

                    for bloque, tramo_pesos in _DESAGREGACION.items():
                        bloque_loc = round(grupos_muni[bloque] * share)
                        acum = 0
                        for j, (tramo, p) in enumerate(tramo_pesos):
                            if j < len(tramo_pesos) - 1:
                                tramo_total = round(bloque_loc * p)
                                acum += tramo_total
                            else:
                                tramo_total = int(bloque_loc - acum)

                            h_tramo = round(tramo_total * loc_ratio_h)
                            m_tramo = int(tramo_total - h_tramo)
                            rows.append(
                                {
                                    "estado": estado,
                                    "municipio": municipio,
                                    "localidad": localidad,
                                    "anio": anio,
                                    "grupo": tramo,
                                    "hombres": h_tramo,
                                    "mujeres": m_tramo,
                                }
                            )
        logger.info("Registros localidad pirámide: %s", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for row in raw:
            estado = row["estado"]
            municipio = row["municipio"]
            localidad = row["localidad"]
            anio = row["anio"]
            grupo = row["grupo"]
            for sexo, valor in (("hombres", row["hombres"]), ("mujeres", row["mujeres"])):
                records.append(
                    {
                        "indicador_id": None,
                        "nivel_geografico": "localidad",
                        "entidad_clave": f"loc_age:{estado}:{municipio}:{localidad}:{sexo}:{grupo}",
                        "valor": float(valor),
                        "unidad": "Personas",
                        "periodo": anio,
                    }
                )
        return records
