"""
Utilidades para obtener catálogo real de localidades (INEGI CatGeo v2).
"""

from __future__ import annotations

import json
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx

from core.logger import get_logger

logger = get_logger(__name__)

_CATGEO_BASE = "https://gaia.inegi.org.mx/wscatgeo/v2"
_MAX_LOCALIDADES_POR_MUNICIPIO = 4
_TIMEOUT = 30.0
_MAX_WORKERS = 12
_ARTICULOS = ("la", "el", "los", "las")
_CACHE_BY_NAME: dict[tuple[str, str], list[tuple[str, float]]] | None = None
_CACHE_BY_CODE: dict[tuple[str, str], list[tuple[str, float]]] | None = None


def _normalize(value: str) -> str:
    txt = unicodedata.normalize("NFD", str(value))
    txt = "".join(ch for ch in txt if unicodedata.category(ch) != "Mn")
    txt = txt.lower().strip()
    txt = re.sub(r"[^a-z0-9\s]", " ", txt)
    txt = re.sub(r"\s+", " ", txt)
    return txt


def _variants(value: str) -> set[str]:
    base = _normalize(value)
    out = {base}
    m = re.match(r"^(.*?),\s*(la|el|los|las)$", base)
    if m:
        body, article = m.group(1).strip(), m.group(2).strip()
        out.add(f"{article} {body}".strip())
    for art in _ARTICULOS:
        pref = f"{art} "
        if base.startswith(pref):
            out.add(base[len(pref) :].strip())
        else:
            out.add(f"{art} {base}".strip())
    return {x for x in out if x}


def _decode_json_latin1(response: httpx.Response) -> dict[str, Any]:
    try:
        text = response.content.decode("utf-8")
    except UnicodeDecodeError:
        text = response.content.decode("latin-1", errors="ignore")
    return json.loads(text)


def _to_int(value: Any) -> int:
    txt = str(value or "").strip()
    if not txt:
        return 0
    txt = txt.replace(",", "")
    try:
        return int(float(txt))
    except ValueError:
        return 0


def _fetch_catalogo_full() -> tuple[
    dict[tuple[str, str], list[tuple[str, float]]],
    dict[tuple[str, str], list[tuple[str, float]]],
]:
    with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client:
        estados_resp = client.get(f"{_CATGEO_BASE}/mgee/")
        estados_resp.raise_for_status()
        estados_data = _decode_json_latin1(estados_resp).get("datos", [])

        municipios_data: list[dict[str, Any]] = []
        for estado in estados_data:
            cve_ent = str(estado.get("cve_ent", "")).zfill(2)
            if not cve_ent:
                continue
            mgem_resp = client.get(f"{_CATGEO_BASE}/mgem/{cve_ent}")
            mgem_resp.raise_for_status()
            for row in _decode_json_latin1(mgem_resp).get("datos", []):
                row["_nom_ent"] = estado.get("nomgeo", "")
                municipios_data.append(row)

        resultados_by_name: dict[tuple[str, str], list[tuple[str, float]]] = {}
        resultados_by_code: dict[tuple[str, str], list[tuple[str, float]]] = {}

        def fetch_localidades(
            item: dict[str, Any],
        ) -> tuple[tuple[str, str], tuple[str, str], list[tuple[str, float]]]:
            nom_ent = str(item.get("_nom_ent", "")).strip()
            nom_mun = str(item.get("nomgeo", "")).strip()
            par_conapo = (_normalize(nom_ent), _normalize(nom_mun))

            cve_ent = str(item.get("cve_ent", "")).zfill(2)
            cve_mun = str(item.get("cve_mun", "")).zfill(3)
            par_codigo = (cve_ent, cve_mun)

            datos: list[dict[str, Any]] = []
            last_exc: Exception | None = None
            for attempt in range(3):
                try:
                    resp = httpx.get(
                        f"{_CATGEO_BASE}/localidades/{cve_ent}/{cve_mun}",
                        timeout=_TIMEOUT,
                        follow_redirects=True,
                    )
                    resp.raise_for_status()
                    datos = _decode_json_latin1(resp).get("datos", [])
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    if attempt < 2:
                        time.sleep(0.2 * (attempt + 1))
            if last_exc is not None:
                logger.warning(
                    "No se pudo obtener localidades INEGI para %s/%s: %s",
                    cve_ent,
                    cve_mun,
                    last_exc,
                )
                return par_conapo, par_codigo, []

            bucket: list[tuple[str, int]] = []
            seen: set[str] = set()
            for loc in datos:
                nombre = str(loc.get("nomgeo", "")).strip()
                if not nombre:
                    continue
                key = _normalize(nombre)
                if key in seen:
                    continue
                seen.add(key)
                pob = _to_int(loc.get("pob_total"))
                bucket.append((nombre, pob))

            bucket.sort(key=lambda x: (-x[1], x[0]))
            top = bucket[:_MAX_LOCALIDADES_POR_MUNICIPIO]
            total_top = sum(max(0, p) for _, p in top)
            if not top:
                return par_conapo, par_codigo, []
            if total_top <= 0:
                peso = 1.0 / len(top)
                return par_conapo, par_codigo, [(n, peso) for n, _ in top]
            return par_conapo, par_codigo, [(n, max(0, p) / total_top) for n, p in top]

        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            for key_nombre, key_codigo, locs in executor.map(fetch_localidades, municipios_data):
                if key_nombre and locs:
                    resultados_by_name[key_nombre] = locs
                if key_codigo and locs:
                    resultados_by_code[key_codigo] = locs

    logger.info(
        "Catálogo INEGI de localidades cargado: %s municipios con localidades",
        len(resultados_by_code),
    )
    return resultados_by_name, resultados_by_code


def get_localidades_reales(
    keys: set[tuple[str, str]],
    cve_por_municipio: dict[tuple[str, str], tuple[str, str]] | None = None,
) -> dict[tuple[str, str], list[tuple[str, float]]]:
    global _CACHE_BY_NAME, _CACHE_BY_CODE
    if _CACHE_BY_NAME is None or _CACHE_BY_CODE is None:
        _CACHE_BY_NAME, _CACHE_BY_CODE = _fetch_catalogo_full()

    resolved: dict[tuple[str, str], list[tuple[str, float]]] = {}
    faltantes = 0
    for estado, municipio in keys:
        asignado: list[tuple[str, float]] | None = None
        if cve_por_municipio:
            codigo = cve_por_municipio.get((estado, municipio))
            if codigo:
                asignado = _CACHE_BY_CODE.get(codigo)
        if asignado:
            resolved[(estado, municipio)] = asignado
            continue
        for ev in _variants(estado):
            for mv in _variants(municipio):
                hit = _CACHE_BY_NAME.get((ev, mv))
                if hit:
                    asignado = hit
                    break
            if asignado:
                break
        if asignado:
            resolved[(estado, municipio)] = asignado
        else:
            faltantes += 1

    if faltantes:
        logger.warning(
            "Catálogo INEGI sin match para %s municipios CONAPO; se usará fallback referencial",
            faltantes,
        )
    return resolved
