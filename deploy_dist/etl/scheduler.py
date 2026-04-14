import importlib
from typing import TYPE_CHECKING, Any

from core.logger import get_logger

if TYPE_CHECKING:
    from apscheduler.schedulers.blocking import BlockingScheduler

logger = get_logger(__name__)

# Registro de módulos ETL activos.
# Cada entrada: {"modulo": "sources.inegi.poblacion", "clase": "PoblacionExtractor", "fuente_id": 1}
MODULOS_REGISTRADOS: list[dict[str, Any]] = []


def registrar_modulo(modulo: str, clase: str, fuente_id: int) -> None:
    """Registra un módulo ETL para ejecución programada."""
    MODULOS_REGISTRADOS.append({"modulo": modulo, "clase": clase, "fuente_id": fuente_id})
    logger.info("Módulo registrado: %s.%s (fuente_id=%d)", modulo, clase, fuente_id)


def run_module(fuente_id: int, usuario: str | None = None) -> None:
    """Ejecuta manualmente el módulo ETL de una fuente específica."""
    for entry in MODULOS_REGISTRADOS:
        if entry["fuente_id"] == fuente_id:
            mod = importlib.import_module(entry["modulo"])
            cls = getattr(mod, entry["clase"])
            extractor = cls()
            extractor.fuente_id = entry["fuente_id"]
            result = extractor.run(tipo_ejecucion="manual", usuario=usuario)
            logger.info(
                "Ejecución manual completada: fuente_id=%d, exitoso=%s",
                fuente_id,
                result.exitoso,
            )
            return
    logger.warning("No se encontró módulo para fuente_id=%d", fuente_id)


def run_all_modules() -> None:
    """Ejecuta todos los módulos ETL registrados (ejecución programada)."""
    logger.info("Iniciando ejecución programada de %d módulos.", len(MODULOS_REGISTRADOS))
    for entry in MODULOS_REGISTRADOS:
        try:
            mod = importlib.import_module(entry["modulo"])
            cls = getattr(mod, entry["clase"])
            extractor = cls()
            extractor.fuente_id = entry["fuente_id"]
            result = extractor.run(tipo_ejecucion="programada")
            logger.info(
                "Módulo %s completado: exitoso=%s, registros=%d",
                entry["modulo"],
                result.exitoso,
                result.registros,
            )
        except Exception as exc:
            logger.error("Error en módulo %s: %s", entry["modulo"], exc)


def run_tipo_cambio() -> None:
    """Ejecuta solo el módulo de tipo de cambio (FIX Banxico, lunes-viernes 12:30 MX)."""
    for entry in MODULOS_REGISTRADOS:
        if entry["modulo"] == "sources.banxico.tipo_cambio":
            try:
                mod = importlib.import_module(entry["modulo"])
                cls = getattr(mod, entry["clase"])
                extractor = cls()
                extractor.fuente_id = entry["fuente_id"]
                result = extractor.run(tipo_ejecucion="programada")
                logger.info(
                    "Tipo de cambio actualizado: exitoso=%s, registros=%d",
                    result.exitoso,
                    result.registros,
                )
            except Exception as exc:
                logger.error("Error actualizando tipo de cambio: %s", exc)
            return


def create_scheduler() -> "BlockingScheduler":
    """Requiere el paquete opcional `apscheduler` (servicio ETL en segundo plano)."""
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger

    scheduler = BlockingScheduler(timezone="America/Mexico_City")

    # Job principal: todos los módulos a las 19:00 MX (lunes a domingo)
    scheduler.add_job(
        run_all_modules,
        trigger=CronTrigger(hour=19, minute=0, timezone="America/Mexico_City"),
        id="etl_diario",
        name="ETL Diario 19:00 MX",
        replace_existing=True,
    )

    # Job específico: tipo de cambio FIX a las 12:30 MX (lunes a viernes)
    # Banxico publica el FIX a las 12:00 hrs en días hábiles
    scheduler.add_job(
        run_tipo_cambio,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=12,
            minute=30,
            timezone="America/Mexico_City",
        ),
        id="tipo_cambio_mediodia",
        name="Tipo de Cambio FIX 12:30 MX (lun-vie)",
        replace_existing=True,
    )

    logger.info("Scheduler configurado: ETL diario 19:00 MX + Tipo de cambio 12:30 MX lun-vie")
    return scheduler
