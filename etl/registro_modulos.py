# etl/registro_modulos.py
"""Registro de módulos ETL cuyo `fuente_id` debe resolverse desde la base de datos."""

from __future__ import annotations

from sqlalchemy import text

from core.db import connect_with_retry, get_db_session
from core.logger import get_logger
from scheduler import registrar_modulo

logger = get_logger(__name__)


def registrar_modulo_si_fuente_existe(modulo: str, clase: str) -> None:
    """Solo registra si existe una fila en public.fuentes_datos con ese `modulo_etl`."""
    try:
        connect_with_retry()
        with get_db_session() as s:
            r = s.execute(
                text("SELECT id FROM public.fuentes_datos WHERE modulo_etl = :m LIMIT 1"),
                {"m": modulo},
            )
            row = r.fetchone()
        if not row:
            logger.warning("Fuente no encontrada; omito registro ETL: %s", modulo)
            return
        registrar_modulo(modulo, clase, fuente_id=row[0])
    except Exception as exc:  # noqa: BLE001
        logger.warning("No se pudo resolver fuente para %s: %s", modulo, exc)
