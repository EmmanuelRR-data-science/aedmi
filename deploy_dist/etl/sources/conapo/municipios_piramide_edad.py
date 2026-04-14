# etl/sources/conapo/municipios_piramide_edad.py
"""
ETL: Distribución de población municipal por sexo y grupos de edad.
Fuente: CONAPO (base censal INEGI + proyecciones municipales).
"""

from typing import Any

from core.base_extractor import BaseExtractor
from core.logger import get_logger
from sources.conapo.municipios_poblacion import _YEARS, _extract_sheet

logger = get_logger(__name__)

INDICADOR_CLAVE = "conapo.municipios_piramide_edad"

_BASE = "http://www.conapo.gob.mx/work/models/CONAPO/Resource/05274c9d-157f-4dd4-9110-d0a0f864d96a/municipales_archivos"
_URL_0_14 = f"{_BASE}/sheet002.html"
_URL_15_64 = f"{_BASE}/sheet003.html"
_URL_65_MAS = f"{_BASE}/sheet004.html"
_URL_HOMBRES = f"{_BASE}/sheet005.html"
_URL_MUJERES = f"{_BASE}/sheet009.html"

_GRUPOS = (
    ("0-14", _URL_0_14),
    ("15-64", _URL_15_64),
    ("65+", _URL_65_MAS),
)

# Desagregación sintética consistente para visualizar mejor la pirámide:
# transforma bloques amplios de CONAPO en tramos de 10 años.
_DESAGREGACION: dict[str, list[tuple[str, float]]] = {
    "0-14": [("0-4", 0.34), ("5-9", 0.33), ("10-14", 0.33)],
    "15-64": [
        ("15-24", 0.22),
        ("25-34", 0.21),
        ("35-44", 0.20),
        ("45-54", 0.19),
        ("55-64", 0.18),
    ],
    "65+": [("65-74", 0.55), ("75-84", 0.30), ("85+", 0.15)],
}


class MunicipiosPiramideEdadExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        hombres = _extract_sheet(_URL_HOMBRES, "hombres_total")
        mujeres = _extract_sheet(_URL_MUJERES, "mujeres_total")
        grupos = {k: _extract_sheet(url, f"grupo_{k}") for k, url in _GRUPOS}

        common = set(hombres.keys()) & set(mujeres.keys())
        for d in grupos.values():
            common &= set(d.keys())

        rows: list[dict[str, Any]] = []
        for estado, municipio in sorted(common):
            for anio in _YEARS:
                h_total = hombres[(estado, municipio)].get(anio)
                m_total = mujeres[(estado, municipio)].get(anio)
                if h_total is None or m_total is None:
                    continue
                total = h_total + m_total
                if total <= 0:
                    continue
                ratio_h = h_total / total

                for grupo_amplio, _ in _GRUPOS:
                    age_total = grupos[grupo_amplio][(estado, municipio)].get(anio)
                    if age_total is None:
                        continue
                    # Repartir el bloque en tramos de edad más finos para la pirámide.
                    tramos = _DESAGREGACION[grupo_amplio]
                    acumulado = 0
                    for idx, (tramo, peso) in enumerate(tramos):
                        if idx < len(tramos) - 1:
                            tramo_total = round(age_total * peso)
                            acumulado += tramo_total
                        else:
                            # Ajuste final para conservar exactamente el total del bloque.
                            tramo_total = int(age_total - acumulado)

                        h_age = round(tramo_total * ratio_h)
                        m_age = int(tramo_total - h_age)
                        rows.append(
                            {
                                "estado": estado,
                                "municipio": municipio,
                                "anio": anio,
                                "grupo": tramo,
                                "hombres": h_age,
                                "mujeres": m_age,
                            }
                        )
        logger.info("Registros municipal pirámide: %s", len(rows))
        return rows

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for row in raw:
            estado = row["estado"]
            municipio = row["municipio"]
            anio = row["anio"]
            grupo = row["grupo"]
            for sexo, valor in (("hombres", row["hombres"]), ("mujeres", row["mujeres"])):
                records.append(
                    {
                        "indicador_id": None,
                        "nivel_geografico": "municipal",
                        "entidad_clave": f"mun_age:{estado}:{municipio}:{sexo}:{grupo}",
                        "valor": float(valor),
                        "unidad": "Personas",
                        "periodo": anio,
                    }
                )
        return records
