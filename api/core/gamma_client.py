# api/core/gamma_client.py
"""Cliente mínimo para la API pública de Gamma (generación asíncrona)."""
from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

GAMMA_API_BASE = "https://public-api.gamma.app/v1.0"


class GammaApiError(Exception):
    """Error devuelto por Gamma o fallo de transporte."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def _headers(api_key: str) -> dict[str, str]:
    return {"X-API-KEY": api_key, "Content-Type": "application/json"}


def _extract_error_message(resp: httpx.Response) -> str:
    try:
        body = resp.json()
        if isinstance(body, dict):
            return str(body.get("message") or body.get("error") or body.get("detail") or resp.text)
    except Exception:
        pass
    return resp.text or resp.reason_phrase


async def create_generation_from_text(
    client: httpx.AsyncClient,
    *,
    api_key: str,
    input_text: str,
    theme_id: str | None,
    num_cards: int,
    export_as: str = "pptx",
) -> str:
    payload: dict[str, Any] = {
        "inputText": input_text,
        "textMode": "preserve",
        "format": "presentation",
        "numCards": num_cards,
        "exportAs": export_as,
        "imageOptions": {"source": "noImages"},
        "textOptions": {
            "language": "es-mx",
            "tone": "profesional, claro, orientado a estudios de mercado",
            "audience": "tomadores de decisión y equipos de consultoría",
            # Evitar "amount" bajo: tiende a resumir el inputText; el detalle del análisis debe conservarse.
        },
    }
    if theme_id and theme_id.strip():
        payload["themeId"] = theme_id.strip()

    r = await client.post(
        f"{GAMMA_API_BASE}/generations",
        headers=_headers(api_key),
        json=payload,
    )
    if r.status_code not in (200, 201):
        raise GammaApiError(_extract_error_message(r), r.status_code)
    data = r.json()
    gid = data.get("generationId") or data.get("generation_id")
    if not gid:
        raise GammaApiError("Gamma no devolvió generationId", r.status_code)
    return str(gid)


async def create_generation_from_template(
    client: httpx.AsyncClient,
    *,
    api_key: str,
    gamma_id: str,
    prompt: str,
    theme_id: str | None,
    export_as: str = "pptx",
) -> str:
    payload: dict[str, Any] = {
        "gammaId": gamma_id.strip(),
        "prompt": prompt,
        "exportAs": export_as,
    }
    if theme_id and theme_id.strip():
        payload["themeId"] = theme_id.strip()

    r = await client.post(
        f"{GAMMA_API_BASE}/generations/from-template",
        headers=_headers(api_key),
        json=payload,
    )
    if r.status_code not in (200, 201):
        raise GammaApiError(_extract_error_message(r), r.status_code)
    data = r.json()
    gid = data.get("generationId") or data.get("generation_id")
    if not gid:
        raise GammaApiError("Gamma no devolvió generationId", r.status_code)
    return str(gid)


async def get_generation_status(
    client: httpx.AsyncClient,
    *,
    api_key: str,
    generation_id: str,
) -> dict[str, Any]:
    r = await client.get(
        f"{GAMMA_API_BASE}/generations/{generation_id}",
        headers=_headers(api_key),
    )
    if r.status_code != 200:
        raise GammaApiError(_extract_error_message(r), r.status_code)
    data = r.json()
    if not isinstance(data, dict):
        raise GammaApiError("Respuesta Gamma inválida", r.status_code)
    return data


async def wait_for_generation(
    client: httpx.AsyncClient,
    *,
    api_key: str,
    generation_id: str,
    poll_interval_s: float = 5.0,
    timeout_s: float = 420.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    while True:
        data = await get_generation_status(client, api_key=api_key, generation_id=generation_id)
        status = str(data.get("status") or data.get("state") or "").lower()
        if status in ("completed", "succeeded", "success"):
            return data
        if status in ("failed", "error", "cancelled", "canceled"):
            msg = (
                data.get("errorMessage")
                or data.get("message")
                or data.get("failureReason")
                or "generación fallida"
            )
            raise GammaApiError(str(msg), None)
        if time.monotonic() >= deadline:
            raise GammaApiError(
                f"Tiempo de espera agotado ({timeout_s:.0f}s) esperando a Gamma.",
                None,
            )
        await asyncio.sleep(poll_interval_s)
