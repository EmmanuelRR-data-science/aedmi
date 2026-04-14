from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from core.db import get_db_session
from core.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ExtractorResult:
    indicador: str
    registros: int = 0
    errores: int = 0
    inicio: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    fin: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    exitoso: bool = False
    mensaje: str = ""


class BaseExtractor(ABC):
    """Clase base para todos los módulos ETL de la AEDMI."""

    periodicidad: str  # "diario" | "mensual" | "anual" | "quinquenal"
    fuente_id: int
    schema: str  # schema de PostgreSQL destino
    tabla: str  # tabla destino dentro del schema
    indicador_clave: str  # clave del indicador en public.indicadores

    @abstractmethod
    def extract(self) -> list[dict[str, Any]]:
        """Extrae datos crudos de la fuente. Retorna lista de dicts."""
        ...

    @abstractmethod
    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transforma datos crudos al esquema de la tabla destino."""
        ...

    def _resolve_indicador_id(self, session: Any, clave: str) -> int | None:
        """Resuelve el id de un indicador por su clave."""
        result = session.execute(
            text("SELECT id FROM public.indicadores WHERE clave = :clave"),
            {"clave": clave},
        )
        row = result.fetchone()
        return row[0] if row else None

    def load(self, records: list[dict[str, Any]]) -> tuple[int, int]:
        """
        Carga registros en la tabla destino usando INSERT ... ON CONFLICT DO NOTHING.
        Retorna (registros_cargados, errores).
        """
        if not records:
            return 0, 0

        cargados = 0
        errores = 0

        with get_db_session() as session:
            # Resolver indicador_id si no está seteado
            indicador_id_cache: dict[str, int | None] = {}

            for record in records:
                try:
                    # Resolver indicador_id desde clave si es None
                    if record.get("indicador_id") is None:
                        clave = getattr(self, "indicador_clave", None)
                        # Para PIB USD usamos entidad_clave como discriminador
                        if clave and record.get("entidad_clave") == "usd_calculado":
                            clave_usd = clave.replace("_mxn", "_usd")
                            if clave_usd not in indicador_id_cache:
                                indicador_id_cache[clave_usd] = self._resolve_indicador_id(
                                    session, clave_usd
                                )
                            record = {**record, "indicador_id": indicador_id_cache[clave_usd]}
                        elif clave:
                            if clave not in indicador_id_cache:
                                indicador_id_cache[clave] = self._resolve_indicador_id(
                                    session, clave
                                )
                            record = {**record, "indicador_id": indicador_id_cache[clave]}

                    if record.get("indicador_id") is None:
                        errores += 1
                        logger.warning(
                            "No se encontró indicador_id para clave '%s'",
                            getattr(self, "indicador_clave", "?"),
                        )
                        continue
                    cols = ", ".join(record.keys())
                    placeholders = ", ".join(f":{k}" for k in record.keys())
                    sql = text(
                        f"INSERT INTO {self.schema}.{self.tabla} ({cols}) "
                        f"VALUES ({placeholders}) ON CONFLICT DO NOTHING"
                    )
                    result = session.execute(sql, record)
                    if result.rowcount > 0:
                        cargados += 1
                except Exception as exc:
                    errores += 1
                    logger.warning(
                        "Error al cargar registro: %s",
                        exc,
                        extra={"fuente_id": self.fuente_id, "registro": record},
                    )
                    logger.debug("Detalle del error", exc_info=True)

        return cargados, errores

    def _log_ejecucion(
        self,
        result: ExtractorResult,
        tipo: str = "programada",
        usuario: str | None = None,
    ) -> None:
        with get_db_session() as session:
            session.execute(
                text(
                    "INSERT INTO public.etl_logs "
                    "(fuente_id, tipo_ejecucion, inicio, fin, exitoso, "
                    "registros_cargados, errores, mensaje, usuario) "
                    "VALUES (:fuente_id, :tipo, :inicio, :fin, :exitoso, "
                    ":registros, :errores, :mensaje, :usuario)"
                ),
                {
                    "fuente_id": self.fuente_id,
                    "tipo": tipo,
                    "inicio": result.inicio,
                    "fin": result.fin,
                    "exitoso": result.exitoso,
                    "registros": result.registros,
                    "errores": result.errores,
                    "mensaje": result.mensaje,
                    "usuario": usuario,
                },
            )

    def _update_fuente_ultima_carga(self) -> None:
        with get_db_session() as session:
            session.execute(
                text(
                    "UPDATE public.fuentes_datos "
                    "SET ultima_carga = NOW(), updated_at = NOW() "
                    "WHERE id = :id"
                ),
                {"id": self.fuente_id},
            )

    def run(
        self, tipo_ejecucion: str = "programada", usuario: str | None = None
    ) -> ExtractorResult:
        """Ejecuta el ciclo completo: extract → transform → load."""
        result = ExtractorResult(indicador=self.indicador_clave)
        result.inicio = datetime.now(tz=timezone.utc)

        logger.info(
            "Iniciando ETL para '%s'", self.indicador_clave, extra={"fuente_id": self.fuente_id}
        )

        try:
            raw = self.extract()
            records = self.transform(raw)
            cargados, errores = self.load(records)

            result.registros = cargados
            result.errores = errores
            result.exitoso = True
            result.mensaje = f"OK: {cargados} registros cargados, {errores} errores."

            if result.exitoso:
                self._update_fuente_ultima_carga()

        except Exception as exc:
            result.exitoso = False
            result.mensaje = str(exc)
            logger.error(
                "Error en ETL '%s': %s",
                self.indicador_clave,
                exc,
                extra={"fuente_id": self.fuente_id},
            )
        finally:
            result.fin = datetime.now(tz=timezone.utc)
            self._log_ejecucion(result, tipo=tipo_ejecucion, usuario=usuario)

        return result
