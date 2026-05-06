# api/routers/export_presentacion.py
from __future__ import annotations

import re
import unicodedata
import zipfile
from http import HTTPStatus
from io import BytesIO

import httpx
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from core.config import get_settings
from core.db import get_db
from core.gamma_client import GammaApiError, create_generation_from_text, wait_for_generation
from core.gamma_export import (
    GammaDatoSeriePunto,
    GammaIndicadorBloque,
    build_gamma_user_message,
    normalize_palette_hex,
    num_cards_sugerido,
)
from core.models import Analisis, Indicador
from core.presentacion_excel_export import construir_xlsx_lote_bytes
from core.presentacion_export import (
    OrigenAnalisis,
    SeccionPptx,
    construir_pptx_bytes,
    construir_pptx_lote,
    decodificar_png_base64,
    resolver_texto_analisis_exportacion,
    subtitulo_contexto_para_exportacion,
)
from core.presentacion_plantilla import PlantillaPptxError
from routers.auth import get_current_user
from schemas.export_presentacion import (
    MAX_PRESENTACION_LOTE,
    ExportPresentacionLoteRequest,
    ExportPresentacionRequest,
    GammaExportResponse,
)

router = APIRouter(prefix="/export", tags=["export"])


def _pptx_template_path_optional() -> str | None:
    p = (get_settings().pptx_template_path or "").strip()
    return p or None


async def _cargar_analisis_por_filtro(
    db: AsyncSession,
    grafica_id: int,
    nivel_geografico: str,
    entidad_clave: str | None,
) -> Analisis | None:
    stmt = select(Analisis).where(
        Analisis.indicador_id == grafica_id,
        Analisis.nivel_geografico == nivel_geografico,
    )
    if entidad_clave is None:
        stmt = stmt.where(Analisis.entidad_clave.is_(None))
    else:
        stmt = stmt.where(Analisis.entidad_clave == entidad_clave)
    r = await db.execute(stmt)
    return r.scalar_one_or_none()


def _analisis_tiene_texto_util(a: Analisis) -> bool:
    """True si resuelve a un cuerpo distinto del placeholder por defecto."""
    t, _ = resolver_texto_analisis_exportacion(a.analisis_revisado, a.analisis_ia)
    t = (t or "").strip()
    return bool(t) and t != "Sin análisis disponible para este indicador."


async def _cargar_analisis_para_export(
    db: AsyncSession,
    grafica_id: int,
    nivel_geografico: str,
    entidad_clave: str | None,
) -> Analisis | None:
    """
    Orden: fila con la misma `entidad_clave` que la cola, luego `entidad_clave` NULL.

    Así se alinea con la UI: el análisis compartido suele guardarse con entidad NULL
    aunque la cola distinga series (p. ej. PIB Total vs per cápita).
    """
    orden: list[str | None] = [entidad_clave]
    if entidad_clave is not None:
        orden.append(None)

    for ent in orden:
        a = await _cargar_analisis_por_filtro(db, grafica_id, nivel_geografico, ent)
        if a is not None and _analisis_tiene_texto_util(a):
            return a
    for ent in orden:
        a = await _cargar_analisis_por_filtro(db, grafica_id, nivel_geografico, ent)
        if a is not None:
            return a
    return None


async def _leyenda_fuente_resuelta(
    db: AsyncSession,
    grafica_id: int,
    leyenda_enviada: str | None,
) -> str | None:
    enviada = (leyenda_enviada or "").strip()
    if enviada:
        return unicodedata.normalize("NFC", enviada)
    r = await db.execute(
        select(Indicador)
        .options(joinedload(Indicador.fuente))
        .where(Indicador.id == grafica_id)
    )
    ind = r.unique().scalar_one_or_none()
    if ind is None:
        return None
    fuente = ind.fuente
    if fuente is None:
        return None
    nombre = getattr(fuente, "nombre", None)
    if not isinstance(nombre, str):
        return None
    nombre = unicodedata.normalize("NFC", nombre.strip())
    return f"Fuente: {nombre}" if nombre else None


@router.post("/presentacion")
async def exportar_presentacion(
    body: ExportPresentacionRequest,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> Response:
    """Genera PPTX: prioridad análisis revisado → análisis IA (Requisito 25)."""
    if (body.texto_analisis or "").strip():
        texto, origen = body.texto_analisis.strip(), "app"
    else:
        a = await _cargar_analisis_para_export(
            db, body.grafica_id, body.nivel_geografico, body.entidad_clave
        )
        if a is not None:
            texto, origen = resolver_texto_analisis_exportacion(
                a.analisis_revisado,
                a.analisis_ia,
            )
        else:
            texto, origen = resolver_texto_analisis_exportacion(None, None)

    imagen = decodificar_png_base64(body.imagen_grafica_png_base64)
    leyenda = await _leyenda_fuente_resuelta(db, body.grafica_id, body.leyenda_fuente)
    try:
        pptx_bytes = construir_pptx_bytes(
            body.titulo,
            texto,
            origen,
            imagen,
            leyenda,
            template_path=_pptx_template_path_optional(),
        )
    except PlantillaPptxError as exc:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc
    # Nombre de archivo solo ASCII (evita cabeceras inválidas en clientes y TestClient)
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", body.titulo).strip("._") or "aedmi"
    safe = f"{base[:64]}.pptx"
    dispo = f'attachment; filename="{safe}"'
    return Response(
        content=pptx_bytes,
        media_type=("application/vnd.openxmlformats-officedocument.presentationml.presentation"),
        headers={"Content-Disposition": dispo},
    )


@router.post("/presentacion/lote")
async def exportar_presentacion_lote(
    body: ExportPresentacionLoteRequest,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> Response:
    """PPTX, XLSX o ZIP (PPTX+XLSX) según ``modo_salida``."""
    if len(body.items) > MAX_PRESENTACION_LOTE:
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            detail=f"Solo se permiten hasta {MAX_PRESENTACION_LOTE} gráficas por lote.",
        )
    modo = body.modo_salida
    if modo in ("xlsx", "zip_pptx_xlsx"):
        faltan: list[dict[str, int]] = []
        for idx, it in enumerate(body.items):
            if not it.datos_serie or len(it.datos_serie) == 0:
                faltan.append({"index": idx, "grafica_id": it.grafica_id})
        if faltan:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                detail={"error": "datos_serie_required", "items": faltan},
            )

    secciones: list[SeccionPptx] = []
    excel_payload: list[
        tuple[
            str,
            str | None,
            str,
            OrigenAnalisis,
            str | None,
            str,
            list[tuple[object, float, str | None]],
        ]
    ] = []

    for it in body.items:
        if (it.texto_analisis or "").strip():
            texto, origen = it.texto_analisis.strip(), "app"
        else:
            a = await _cargar_analisis_para_export(
                db, it.grafica_id, it.nivel_geografico, it.entidad_clave
            )
            if a is not None:
                texto, origen = resolver_texto_analisis_exportacion(
                    a.analisis_revisado,
                    a.analisis_ia,
                )
            else:
                texto, origen = resolver_texto_analisis_exportacion(None, None)
        img = decodificar_png_base64(it.imagen_grafica_png_base64)
        leyenda = await _leyenda_fuente_resuelta(db, it.grafica_id, it.leyenda_fuente)
        sub_ctx = subtitulo_contexto_para_exportacion(it.subtitulo_contexto)
        secciones.append(
            SeccionPptx(
                titulo=it.titulo,
                subtitulo_contexto=sub_ctx,
                cuerpo_analisis=texto,
                origen=origen,
                imagen_png=img,
                leyenda_fuente=leyenda,
            )
        )
        puntos: list[tuple[object, float, str | None]] = []
        if it.datos_serie:
            for p in it.datos_serie:
                puntos.append((p.periodo, float(p.valor), p.entidad_clave))
        excel_payload.append(
            (
                it.titulo,
                sub_ctx,
                texto,
                origen,
                leyenda,
                it.excel_chart_kind,
                puntos,
            )
        )

    base = re.sub(
        r"[^A-Za-z0-9._-]+",
        "_",
        body.titulo_presentacion,
    ).strip("._") or "aedmi_lote"
    base = base[:64]

    if modo == "pptx":
        try:
            raw = construir_pptx_lote(
                body.titulo_presentacion,
                secciones,
                template_path=_pptx_template_path_optional(),
            )
        except PlantillaPptxError as exc:
            raise HTTPException(
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                detail=str(exc),
            ) from exc
        safe = f"{base}.pptx"
        return Response(
            content=raw,
            media_type=(
                "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            ),
            headers={"Content-Disposition": f'attachment; filename="{safe}"'},
        )

    xlsx_bytes = construir_xlsx_lote_bytes(body.titulo_presentacion, excel_payload)
    if modo == "xlsx":
        safe = f"{base}.xlsx"
        return Response(
            content=xlsx_bytes,
            media_type=(
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ),
            headers={"Content-Disposition": f'attachment; filename="{safe}"'},
        )

    try:
        pptx_bytes = construir_pptx_lote(
            body.titulo_presentacion,
            secciones,
            template_path=_pptx_template_path_optional(),
        )
    except PlantillaPptxError as exc:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{base}.pptx", pptx_bytes)
        zf.writestr(f"{base}.xlsx", xlsx_bytes)
    raw_zip = buf.getvalue()
    safe = f"{base}.zip"
    return Response(
        content=raw_zip,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{safe}"'},
    )


def _urls_desde_gamma_completado(data: dict) -> tuple[str | None, str | None]:
    return (
        data.get("gammaUrl") or data.get("gamma_url"),
        data.get("exportUrl") or data.get("export_url"),
    )


@router.post("/presentacion/gamma", response_model=GammaExportResponse)
async def exportar_presentacion_gamma(
    body: ExportPresentacionLoteRequest,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> GammaExportResponse:
    """Genera presentación en Gamma con el mismo cuerpo que el lote PPTX (texto desde BD)."""
    settings = get_settings()
    if not (settings.gamma_api_key or "").strip():
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail="Gamma no está configurado (GAMMA_API_KEY).",
        )
    if len(body.items) > MAX_PRESENTACION_LOTE:
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            detail=f"Solo se permiten hasta {MAX_PRESENTACION_LOTE} gráficas por lote.",
        )

    bloques: list[GammaIndicadorBloque] = []
    for idx, it in enumerate(body.items, start=1):
        if (it.texto_analisis or "").strip():
            texto, origen = it.texto_analisis.strip(), "app"
        else:
            a = await _cargar_analisis_para_export(
                db, it.grafica_id, it.nivel_geografico, it.entidad_clave
            )
            if a is not None:
                texto, origen = resolver_texto_analisis_exportacion(
                    a.analisis_revisado,
                    a.analisis_ia,
                )
            else:
                texto, origen = resolver_texto_analisis_exportacion(None, None)
        img = decodificar_png_base64(it.imagen_grafica_png_base64)
        datos_tuple: tuple[GammaDatoSeriePunto, ...] | None = None
        if it.datos_serie:
            datos_tuple = tuple(
                GammaDatoSeriePunto(
                    periodo=p.periodo,
                    valor=float(p.valor),
                    entidad_clave=p.entidad_clave,
                    unidad=p.unidad,
                )
                for p in it.datos_serie
            )
        bloques.append(
            GammaIndicadorBloque(
                orden=idx,
                titulo=it.titulo,
                subtitulo=subtitulo_contexto_para_exportacion(it.subtitulo_contexto),
                nivel_geografico=it.nivel_geografico,
                origen=origen,
                texto_analisis=texto,
                imagen_png=img,
                datos_serie=datos_tuple,
            )
        )

    try:
        mensaje = build_gamma_user_message(
            titulo_presentacion=body.titulo_presentacion,
            theme_id=settings.theme_id,
            bloques=bloques,
            paleta_hex=normalize_palette_hex(body.paleta_hex),
        )
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            detail=str(exc),
        ) from exc

    timeout = httpx.Timeout(90.0, connect=20.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            gid = await create_generation_from_text(
                client,
                api_key=settings.gamma_api_key.strip(),
                input_text=mensaje,
                theme_id=(settings.theme_id or "").strip() or None,
                num_cards=num_cards_sugerido(len(bloques)),
            )
            final = await wait_for_generation(
                client,
                api_key=settings.gamma_api_key.strip(),
                generation_id=gid,
            )
    except GammaApiError as exc:
        sc = exc.status_code
        if sc in (400, 401, 402, 403, 404, 422):
            raise HTTPException(status_code=sc, detail=str(exc)) from exc
        raise HTTPException(status_code=HTTPStatus.BAD_GATEWAY, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=HTTPStatus.BAD_GATEWAY,
            detail=f"Error de red con Gamma: {exc}",
        ) from exc

    g_url, ex_url = _urls_desde_gamma_completado(final)
    return GammaExportResponse(
        generation_id=gid,
        gamma_url=g_url,
        export_url=ex_url,
    )
