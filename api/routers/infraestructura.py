import io
import unicodedata

import httpx
import pypdfium2 as pdfium
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import Response

from routers.auth import get_current_user

router = APIRouter(prefix="/infraestructura", tags=["infraestructura"])

_BASE = "https://micrs.sct.gob.mx/images/DireccionesGrales/DGP/Atlas/Mapas_2024"
_MAPAS_RED_CARRETERA = {
    "aguascalientes": f"{_BASE}/1-Aguascalientes_2024.pdf",
    "baja california": f"{_BASE}/2-Baja%20California_2024.pdf",
    "baja california sur": f"{_BASE}/3-Baja%20California%20Sur_2024.pdf",
    "campeche": f"{_BASE}/4-Campeche_2024.pdf",
    "coahuila": f"{_BASE}/5-Coahuila_2024.pdf",
    "colima": f"{_BASE}/6-Colima_2024.pdf",
    "chiapas": f"{_BASE}/7-Chiapas_2024.pdf",
    "chihuahua": f"{_BASE}/8-Chihuahua_2024.pdf",
    "ciudad de mexico": f"{_BASE}/8-Ciudad%20de%20Mexico_2024.pdf",
    "durango": f"{_BASE}/10-Durango_2024.pdf",
    "guanajuato": f"{_BASE}/11-Guanajuato_2024.pdf",
    "guerrero": f"{_BASE}/12-Guerrero_2024.pdf",
    "hidalgo": f"{_BASE}/13-Hidalgo_2024.pdf",
    "jalisco": f"{_BASE}/14-Jalisco_2024.pdf",
    "estado de mexico": f"{_BASE}/15-Mexico_2024.pdf",
    "michoacan": f"{_BASE}/16-Michoacan_2024.pdf",
    "morelos": f"{_BASE}/17-Morelos_2024.pdf",
    "nayarit": f"{_BASE}/18-Nayarit_2024.pdf",
    "nuevo leon": f"{_BASE}/19-Nuevo%20Leon_2024.pdf",
    "oaxaca": f"{_BASE}/20-Oaxaca_2024.pdf",
    "puebla": f"{_BASE}/21-Puebla_2024.pdf",
    "queretaro": f"{_BASE}/22-Queretaro_2024.pdf",
    "quintana roo": f"{_BASE}/23-Quintana%20Roo_2024.pdf",
    "san luis potosi": f"{_BASE}/24-San%20Luis%20Potosi_2024.pdf",
    "sinaloa": f"{_BASE}/25-Sinaloa_2024.pdf",
    "sonora": f"{_BASE}/26-Sonora_2024.pdf",
    "tabasco": f"{_BASE}/27-Tabasco_2024.pdf",
    "tamaulipas": f"{_BASE}/28-Tamaulipas_2024.pdf",
    "tlaxcala": f"{_BASE}/29-Tlaxcala_2024.pdf",
    "veracruz": f"{_BASE}/30-Veracruz_2024.pdf",
    "yucatan": f"{_BASE}/31-Yucatan_2024.pdf",
    "zacatecas": f"{_BASE}/32-Zacatecas_2024.pdf",
}


def _normalizar_estado(estado: str) -> str:
    normalized = unicodedata.normalize("NFD", estado)
    without_accents = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    compact = " ".join(without_accents.strip().lower().split())
    if compact == "mexico":
        return "estado de mexico"
    return compact


def _resolver_url(estado: str) -> str:
    key = _normalizar_estado(estado)
    url = _MAPAS_RED_CARRETERA.get(key)
    if not url:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No se encontró mapa carretero para '{estado}'.",
        )
    return url


@router.get("/red-carretera/{estado}")
async def get_red_carretera_info(
    estado: str,
    _: str = Depends(get_current_user),
) -> dict[str, str]:
    pdf_url = _resolver_url(estado)
    return {
        "estado": estado,
        "pdf_url": pdf_url,
        "fuente": "SICT — Mapas por Entidades Federativas 2024",
    }


@router.get("/red-carretera/{estado}/png")
async def get_red_carretera_png(
    estado: str,
    _: str = Depends(get_current_user),
) -> Response:
    pdf_url = _resolver_url(estado)
    try:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            pdf_response = await client.get(pdf_url)
        pdf_response.raise_for_status()

        doc = pdfium.PdfDocument(pdf_response.content)
        page = doc[0]
        bitmap = page.render(scale=2.0)
        image = bitmap.to_pil()
        output = io.BytesIO()
        image.save(output, format="PNG")
        page.close()
        doc.close()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"No fue posible convertir el PDF de red carretera: {exc}",
        ) from exc

    filename_estado = _normalizar_estado(estado).replace(" ", "-")
    headers = {
        "Content-Disposition": f'inline; filename="red-carretera-{filename_estado}.png"',
        "Cache-Control": "public, max-age=3600",
    }
    return Response(content=output.getvalue(), media_type="image/png", headers=headers)
