# etl/core/fuente_check.py
"""Requisito 15.6: consultar public.fuentes_datos antes de insertar carga (ETL)."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)


def fuente_activa_y_registrada(session: Any, fuente_id: int) -> bool:
    """
    Retorna True si existe la fila en public.fuentes_datos, está activa, y se puede
    continuar con la carga. Si no, registra aviso (no excepción) y retorna False.
    """
    row = session.execute(
        text("SELECT id, activo FROM public.fuentes_datos WHERE id = :id"),
        {"id": fuente_id},
    ).fetchone()
    if row is None:
        logger.warning(
            "ETL: fuente_id %s no figura en public.fuentes_datos; se omite carga",
            fuente_id,
        )
        return False
    if not row[1]:
        logger.warning("ETL: fuente_id %s está inactiva; se omite carga", fuente_id)
        return False
    return True
