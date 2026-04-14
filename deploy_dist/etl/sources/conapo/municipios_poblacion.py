# etl/sources/conapo/municipios_poblacion.py
"""
ETL: Población municipal (total, hombres, mujeres) — CONAPO.
Fuente: Reconstrucción y proyecciones municipales 2005-2030.
"""

import io
import json
import re
import unicodedata
from typing import Any

import httpx
import pandas as pd

from core.base_extractor import BaseExtractor
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "conapo.municipios_poblacion"

_BASE = "http://www.conapo.gob.mx/work/models/CONAPO/Resource/05274c9d-157f-4dd4-9110-d0a0f864d96a/municipales_archivos"
_URL_TOTAL = f"{_BASE}/sheet001.html"
_URL_HOMBRES = f"{_BASE}/sheet005.html"
_URL_MUJERES = f"{_BASE}/sheet009.html"
_YEARS = (2020, 2025)
_CATGEO_MGEE_URL = "https://gaia.inegi.org.mx/wscatgeo/v2/mgee/"
_ESTADOS_CVE_CACHE: dict[str, str] | None = None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    txt = str(value).strip()
    if txt in {"", "nan", "None"}:
        return None
    cleaned = txt.replace(" ", "").replace(",", "")
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def _normalize_name(value: Any) -> str:
    return " ".join(str(value).strip().split())


def _normalize_key(value: str) -> str:
    txt = unicodedata.normalize("NFD", str(value))
    txt = "".join(ch for ch in txt if unicodedata.category(ch) != "Mn")
    txt = txt.lower().strip()
    txt = re.sub(r"[^a-z0-9\s]", " ", txt)
    txt = re.sub(r"\s+", " ", txt)
    return txt


def _decode_json_latin1(response: httpx.Response) -> dict[str, Any]:
    try:
        return json.loads(response.content.decode("utf-8"))
    except UnicodeDecodeError:
        return json.loads(response.content.decode("latin-1", errors="ignore"))


def _get_estados_cve() -> dict[str, str]:
    global _ESTADOS_CVE_CACHE
    if _ESTADOS_CVE_CACHE is not None:
        return _ESTADOS_CVE_CACHE

    response = httpx.get(_CATGEO_MGEE_URL, timeout=90, follow_redirects=True)
    response.raise_for_status()
    data = _decode_json_latin1(response).get("datos", [])
    out: dict[str, str] = {}
    for row in data:
        nombre = _normalize_key(str(row.get("nomgeo", "")))
        cve = str(row.get("cve_ent", "")).zfill(2)
        if nombre and cve:
            out[nombre] = cve
    aliases = {
        "coahuila": "coahuila de zaragoza",
        "michoacan": "michoacan de ocampo",
        "veracruz": "veracruz de ignacio de la llave",
        "distrito federal": "ciudad de mexico",
    }
    for alias, canonical in aliases.items():
        cve = out.get(canonical)
        if cve:
            out[alias] = cve
    _ESTADOS_CVE_CACHE = out
    return out


def _extract_sheet(
    url: str, label: str, years: tuple[int, ...] = _YEARS
) -> dict[tuple[str, str], dict[int, int]]:
    logger.info("Descargando tabla CONAPO: %s", label)
    response = httpx.get(url, timeout=90, follow_redirects=True)
    response.raise_for_status()
    html_text = response.content.decode("latin-1", errors="ignore")
    df = pd.read_html(io.StringIO(html_text), header=0)[0]

    header_idx = None
    for idx, row in df.iterrows():
        if any(str(v).strip().lower() == "clave" for v in row.values):
            header_idx = idx
            break
    if header_idx is None:
        raise RuntimeError(f"No se encontró encabezado de columnas en {label}")

    df.columns = [str(v).strip() for v in df.iloc[header_idx].values]
    df = df.iloc[header_idx + 1 :].copy()
    df = df.dropna(how="all")

    year_cols: dict[int, str] = {}
    for y in years:
        y_col = next((c for c in df.columns if str(y) in str(c)), None)
        if not y_col:
            raise RuntimeError(f"No se encontró columna {y} en {label}")
        year_cols[y] = y_col

    rows: dict[tuple[str, str], dict[int, int]] = {}
    current_state: str | None = None

    for _, row in df.iterrows():
        c0 = row.iloc[0] if len(row) > 0 else None
        c1 = row.iloc[1] if len(row) > 1 else None

        c0_txt = _normalize_name(c0) if pd.notna(c0) else ""
        c1_txt = _normalize_name(c1) if pd.notna(c1) else ""

        is_municipio = c0_txt.isdigit()
        if not is_municipio:
            state_name = c0_txt or c1_txt
            if state_name and state_name.lower() != "república mexicana":
                current_state = state_name
            continue

        if not current_state:
            continue

        municipio = c1_txt
        if not municipio:
            continue

        values: dict[int, int] = {}
        for y, col in year_cols.items():
            val = _to_int(row[col])
            if val is not None:
                values[y] = val
        if values:
            rows[(current_state, municipio)] = values

    logger.info("Tabla %s: %s municipios parseados", label, len(rows))
    return rows


def extract_municipio_codes(
    url: str = _URL_TOTAL, label: str = "total_codigos_localidades"
) -> dict[tuple[str, str], tuple[str, str]]:
    logger.info("Descargando tabla CONAPO para códigos municipales: %s", label)
    response = httpx.get(url, timeout=90, follow_redirects=True)
    response.raise_for_status()
    html_text = response.content.decode("latin-1", errors="ignore")
    df = pd.read_html(io.StringIO(html_text), header=0)[0]

    header_idx = None
    for idx, row in df.iterrows():
        if any(str(v).strip().lower() == "clave" for v in row.values):
            header_idx = idx
            break
    if header_idx is None:
        raise RuntimeError(f"No se encontró encabezado de columnas en {label}")

    df.columns = [str(v).strip() for v in df.iloc[header_idx].values]
    df = df.iloc[header_idx + 1 :].copy()
    df = df.dropna(how="all")

    estados_cve = _get_estados_cve()
    current_state: str | None = None
    codes: dict[tuple[str, str], tuple[str, str]] = {}

    for _, row in df.iterrows():
        c0 = row.iloc[0] if len(row) > 0 else None
        c1 = row.iloc[1] if len(row) > 1 else None

        c0_txt = _normalize_name(c0) if pd.notna(c0) else ""
        c1_txt = _normalize_name(c1) if pd.notna(c1) else ""

        is_municipio = c0_txt.isdigit()
        if not is_municipio:
            state_name = c0_txt or c1_txt
            if state_name and state_name.lower() != "república mexicana":
                current_state = state_name
            continue

        if not current_state:
            continue

        municipio = c1_txt
        if not municipio:
            continue

        cve_ent = estados_cve.get(_normalize_key(current_state))
        cve_mun = c0_txt[-3:].zfill(3)
        if cve_ent and cve_mun:
            codes[(current_state, municipio)] = (cve_ent, cve_mun)

    logger.info("Códigos municipales mapeados: %s", len(codes))
    return codes


class MunicipiosPoblacionExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "anual"
    tabla = "datos"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        total = _extract_sheet(_URL_TOTAL, "total")
        hombres = _extract_sheet(_URL_HOMBRES, "hombres")
        mujeres = _extract_sheet(_URL_MUJERES, "mujeres")

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
        logger.info("Registros municipales combinados: %s", len(rows))
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
                        "entidad_clave": f"mun:{estado}:{municipio}:{sexo}",
                        "valor": float(valor),
                        "unidad": "Personas",
                        "periodo": anio,
                    }
                )
        return records
