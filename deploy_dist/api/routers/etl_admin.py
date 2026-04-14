# api/routers/etl_admin.py
import io
from datetime import date, datetime, timezone

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.db import get_db
from core.models import ETLLog, FuenteDatos
from routers.auth import get_current_user
from schemas.etl import ETLLogResponse, ModuloETLResponse, UploadPreviewResponse

router = APIRouter(prefix="/admin/etl", tags=["etl-admin"])


@router.get("/modulos", response_model=list[ModuloETLResponse])
async def list_modulos(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> list[ModuloETLResponse]:
    stmt = select(FuenteDatos).where(FuenteDatos.activo.is_(True)).order_by(FuenteDatos.nombre)
    result = await db.execute(stmt)
    fuentes = result.scalars().all()

    modulos: list[ModuloETLResponse] = []
    for fuente in fuentes:
        log_stmt = (
            select(ETLLog)
            .where(ETLLog.fuente_id == fuente.id)
            .order_by(desc(ETLLog.inicio))
            .limit(1)
        )
        log_result = await db.execute(log_stmt)
        ultimo_log = log_result.scalar_one_or_none()

        modulos.append(
            ModuloETLResponse(
                id=fuente.id,
                nombre=fuente.nombre,
                fuente=fuente.url_referencia or "",
                periodicidad=fuente.periodicidad,
                ultima_ejecucion=ultimo_log.inicio if ultimo_log else None,
                exitoso=ultimo_log.exitoso if ultimo_log else None,
                estado=fuente.estado,
            )
        )
    return modulos


@router.post("/modulos/{fuente_id}/ejecutar", response_model=ETLLogResponse)
async def ejecutar_modulo(
    fuente_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: str = Depends(get_current_user),
) -> ETLLogResponse:
    fuente = await db.get(FuenteDatos, fuente_id)
    if fuente is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fuente no encontrada.")

    now = datetime.now(tz=timezone.utc)
    log = ETLLog(
        fuente_id=fuente_id,
        tipo_ejecucion="manual",
        inicio=now,
        exitoso=None,
        registros_cargados=0,
        errores=0,
        mensaje="Ejecución manual iniciada desde interfaz de administración.",
        usuario=current_user,
    )
    db.add(log)
    await db.commit()
    await db.refresh(log)

    return ETLLogResponse(
        id=log.id,
        fuente_id=log.fuente_id,
        fuente_nombre=fuente.nombre,
        tipo_ejecucion=log.tipo_ejecucion,
        inicio=log.inicio,
        fin=log.fin,
        exitoso=log.exitoso,
        registros_cargados=log.registros_cargados,
        errores=log.errores,
        mensaje=log.mensaje,
        usuario=log.usuario,
    )


@router.post("/modulos/{fuente_id}/upload", response_model=UploadPreviewResponse)
async def upload_archivo(
    fuente_id: int,
    archivo: UploadFile = File(...),
    columnas_esperadas: str = Form(default=""),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> UploadPreviewResponse:
    fuente = await db.get(FuenteDatos, fuente_id)
    if fuente is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fuente no encontrada.")

    filename = archivo.filename or "archivo"
    ext = filename.lower().rsplit(".", 1)[-1]
    if ext not in ("xlsx", "xls", "csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Formato no soportado. Use XLSX o CSV.",
        )

    content = await archivo.read()

    try:
        if ext in ("xlsx", "xls"):
            df = pd.read_excel(io.BytesIO(content))
            formato = "xlsx"
        else:
            df = pd.read_csv(io.BytesIO(content))
            formato = "csv"
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error al parsear el archivo: {exc}",
        ) from exc

    columnas_detectadas = list(df.columns)
    esperadas = [c.strip() for c in columnas_esperadas.split(",") if c.strip()]

    detectadas_set = set(columnas_detectadas)
    esperadas_set = set(esperadas)
    faltantes = sorted(esperadas_set - detectadas_set)
    nuevas = sorted(detectadas_set - esperadas_set)

    preview_df = df.head(5).where(pd.notnull(df.head(5)), None)
    filas_preview = preview_df.to_dict(orient="records")

    return UploadPreviewResponse(
        columnas_detectadas=columnas_detectadas,
        filas_preview=filas_preview,
        total_filas=len(df),
        formato=formato,
        hay_diferencias=bool(faltantes or nuevas),
        columnas_faltantes=faltantes,
        columnas_nuevas=nuevas,
    )


@router.get("/logs", response_model=list[ETLLogResponse])
async def list_logs(
    fuente_id: int | None = Query(None),
    fecha_desde: date | None = Query(None),
    exitoso: bool | None = Query(None),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> list[ETLLogResponse]:
    stmt = select(ETLLog, FuenteDatos.nombre).join(
        FuenteDatos, ETLLog.fuente_id == FuenteDatos.id, isouter=True
    )

    if fuente_id is not None:
        stmt = stmt.where(ETLLog.fuente_id == fuente_id)
    if fecha_desde is not None:
        stmt = stmt.where(
            ETLLog.inicio
            >= datetime(fecha_desde.year, fecha_desde.month, fecha_desde.day, tzinfo=timezone.utc)
        )
    if exitoso is not None:
        stmt = stmt.where(ETLLog.exitoso == exitoso)

    stmt = stmt.order_by(desc(ETLLog.inicio)).limit(100)
    result = await db.execute(stmt)
    rows = result.all()

    return [
        ETLLogResponse(
            id=log.id,
            fuente_id=log.fuente_id,
            fuente_nombre=nombre,
            tipo_ejecucion=log.tipo_ejecucion,
            inicio=log.inicio,
            fin=log.fin,
            exitoso=log.exitoso,
            registros_cargados=log.registros_cargados,
            errores=log.errores,
            mensaje=log.mensaje,
            usuario=log.usuario,
        )
        for log, nombre in rows
    ]
