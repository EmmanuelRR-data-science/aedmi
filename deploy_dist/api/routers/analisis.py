# api/routers/analisis.py
import json
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import get_settings
from core.db import get_db
from core.models import Analisis, Indicador
from routers.auth import get_current_user
from schemas.analisis import AnalisisIARequest, AnalisisResponse, AnalisisRevisadoRequest

router = APIRouter(prefix="/analisis", tags=["analisis"])
settings = get_settings()

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"


async def _call_groq(datos_json: str, nombre_indicador: str) -> str:
    prompt = (
        f"Eres un analista de datos experto en economía, demografía y estadística de México. "
        f"Se te proporcionan los datos del indicador '{nombre_indicador}' en formato JSON.\n\n"
        f"Tu tarea es:\n"
        f"1. Identificar patrones, tendencias y valores atípicos (outliers) en los datos.\n"
        f"2. Explicar el comportamiento observado relacionándolo con el contexto histórico, "
        f"económico o social de México en cada período.\n"
        f"3. Señalar los cambios más significativos y sus posibles causas.\n"
        f"4. Emitir una conclusión sobre la situación actual del indicador.\n\n"
        f"Responde en Español de México, de forma clara y estructurada. "
        f"No repitas los datos crudos, interprétalos.\n\n"
        f"Datos:\n{datos_json}"
    )

    headers = {
        "Authorization": f"Bearer {settings.groq_api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
    }

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(GROQ_API_URL, headers=headers, json=body)
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]


@router.get("/{grafica_id}", response_model=AnalisisResponse | None)
async def get_analisis(
    grafica_id: int,
    entidad_clave: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> Analisis | None:
    stmt = select(Analisis).where(Analisis.indicador_id == grafica_id)
    if entidad_clave is None:
        stmt = stmt.where(Analisis.entidad_clave.is_(None))
    else:
        stmt = stmt.where(Analisis.entidad_clave == entidad_clave)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


@router.post("/{grafica_id}/ia", response_model=AnalisisResponse)
async def generar_analisis_ia(
    grafica_id: int,
    body: AnalisisIARequest | None = None,
    entidad_clave: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> Analisis:
    # Verificar que el indicador existe
    ind_result = await db.execute(select(Indicador).where(Indicador.id == grafica_id))
    indicador = ind_result.scalar_one_or_none()
    if indicador is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Indicador no encontrado."
        )

    # Obtener análisis existente (si hay)
    stmt = select(Analisis).where(Analisis.indicador_id == grafica_id)
    if entidad_clave is None:
        stmt = stmt.where(Analisis.entidad_clave.is_(None))
    else:
        stmt = stmt.where(Analisis.entidad_clave == entidad_clave)
    result = await db.execute(stmt)
    analisis = result.scalar_one_or_none()

    # Llamar a Groq — si falla, no modificar análisis previo
    try:
        # Si el frontend envía datos filtrados (exactamente lo mostrado en pantalla),
        # esos son la prioridad para garantizar análisis específico por visualización.
        if body and body.datos_filtrados:
            payload = {
                "indicador": indicador.nombre,
                "unidad": indicador.unidad,
                "contexto": body.contexto or {},
                "datos": body.datos_filtrados,
            }
        else:
            # Fallback: datos reales del indicador desde la BD.
            from sqlalchemy import text

            from core.models import FuenteDatos

            fuente_result = await db.execute(
                select(FuenteDatos).where(FuenteDatos.id == indicador.fuente_id)
            )
            fuente = fuente_result.scalar_one_or_none()
            periodicidad = fuente.periodicidad if fuente else "mensual"

            schema_map = {
                "diario": "diario",
                "semanal": "mensual",
                "mensual": "mensual",
                "anual": "anual",
                "quinquenal": "anual",
                "otra": "anual",
            }
            schema = schema_map.get(periodicidad, "mensual")

            if schema == "diario":
                sql = text(
                    f"SELECT fecha::text AS periodo, valor, unidad "  # noqa: S608
                    f"FROM {schema}.datos WHERE indicador_id = :iid "
                    f"ORDER BY fecha DESC LIMIT 100"
                )
            elif schema == "mensual":
                sql = text(  # noqa: S608
                    "SELECT (anio::text || '-' || LPAD(mes::text,2,'0')) "
                    "AS periodo, valor, unidad "
                    f"FROM {schema}.datos WHERE indicador_id = :iid "
                    "ORDER BY anio DESC, mes DESC LIMIT 100"
                )
            else:
                sql = text(
                    f"SELECT periodo::text AS periodo, valor, unidad "  # noqa: S608
                    f"FROM {schema}.datos WHERE indicador_id = :iid "
                    f"ORDER BY periodo ASC LIMIT 200"
                )

            datos_result = await db.execute(sql, {"iid": grafica_id})
            filas = [dict(r) for r in datos_result.mappings().all()]

            payload = {
                "indicador": indicador.nombre,
                "unidad": indicador.unidad,
                "periodicidad": periodicidad,
                "fuente": fuente.nombre if fuente else "INEGI/Banxico",
                "contexto": {"entidad_clave": entidad_clave} if entidad_clave else {},
                "datos": filas,
            }
        datos_json = json.dumps(payload, ensure_ascii=False, default=str)
        texto_ia = await _call_groq(datos_json, indicador.nombre)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Error al contactar Groq API: {exc}",
        ) from exc

    now = datetime.now(tz=timezone.utc)

    if analisis is None:
        analisis = Analisis(
            indicador_id=grafica_id,
            nivel_geografico=indicador.nivel_geografico,
            entidad_clave=entidad_clave,
            analisis_ia=texto_ia,
            ia_generado_at=now,
            updated_at=now,
        )
        db.add(analisis)
    else:
        analisis.analisis_ia = texto_ia
        analisis.ia_generado_at = now
        analisis.updated_at = now

    await db.commit()
    await db.refresh(analisis)
    return analisis


@router.put("/{grafica_id}/revisado", response_model=AnalisisResponse)
async def guardar_analisis_revisado(
    grafica_id: int,
    body: AnalisisRevisadoRequest,
    entidad_clave: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> Analisis:
    stmt = select(Analisis).where(Analisis.indicador_id == grafica_id)
    if entidad_clave is None:
        stmt = stmt.where(Analisis.entidad_clave.is_(None))
    else:
        stmt = stmt.where(Analisis.entidad_clave == entidad_clave)
    result = await db.execute(stmt)
    analisis = result.scalar_one_or_none()

    now = datetime.now(tz=timezone.utc)

    if analisis is None:
        ind_result = await db.execute(select(Indicador).where(Indicador.id == grafica_id))
        indicador = ind_result.scalar_one_or_none()
        if indicador is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Indicador no encontrado."
            )
        analisis = Analisis(
            indicador_id=grafica_id,
            nivel_geografico=indicador.nivel_geografico,
            entidad_clave=entidad_clave,
            analisis_revisado=body.texto,
            revisado_at=now,
            updated_at=now,
        )
        db.add(analisis)
    else:
        analisis.analisis_revisado = body.texto
        analisis.revisado_at = now
        analisis.updated_at = now

    await db.commit()
    await db.refresh(analisis)
    return analisis
