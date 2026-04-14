"""
ETL: Catálogo de pueblos mágicos (SECTUR).

Carga/actualiza un catálogo geográfico para consultas de proximidad en el módulo mapa.
Permite fuente CSV configurable vía `PUEBLOS_MAGICOS_CSV_URL` y fallback local.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import text

from core.base_extractor import BaseExtractor
from core.db import get_db_session
from core.logger import get_logger

logger = get_logger(__name__)

INDICADOR_CLAVE = "sectur.pueblos_magicos_catalogo"

FALLBACK_PUEBLOS: list[dict[str, Any]] = [
    {
        "clave": "pm_bernal",
        "nombre": "Bernal",
        "entidad": "Querétaro",
        "lat": 20.7417,
        "lng": -99.9417,
    },
    {
        "clave": "pm_tequisquiapan",
        "nombre": "Tequisquiapan",
        "entidad": "Querétaro",
        "lat": 20.5205,
        "lng": -99.8917,
    },
    {
        "clave": "pm_san_miguel",
        "nombre": "San Miguel de Allende",
        "entidad": "Guanajuato",
        "lat": 20.9144,
        "lng": -100.7436,
    },
    {
        "clave": "pm_dolores",
        "nombre": "Dolores Hidalgo",
        "entidad": "Guanajuato",
        "lat": 21.1578,
        "lng": -100.9302,
    },
    {
        "clave": "pm_real_catorce",
        "nombre": "Real de Catorce",
        "entidad": "San Luis Potosí",
        "lat": 23.6904,
        "lng": -100.8857,
    },
    {
        "clave": "pm_tlalpujahua",
        "nombre": "Tlalpujahua",
        "entidad": "Michoacán",
        "lat": 19.8055,
        "lng": -100.1723,
    },
    {
        "clave": "pm_mineral_chico",
        "nombre": "Mineral del Chico",
        "entidad": "Hidalgo",
        "lat": 20.2144,
        "lng": -98.7313,
    },
    {
        "clave": "pm_cholula",
        "nombre": "San Pedro Cholula",
        "entidad": "Puebla",
        "lat": 19.0610,
        "lng": -98.3074,
    },
    {
        "clave": "pm_valle_bravo",
        "nombre": "Valle de Bravo",
        "entidad": "Estado de México",
        "lat": 19.1926,
        "lng": -100.1329,
    },
    {
        "clave": "pm_taxco",
        "nombre": "Taxco",
        "entidad": "Guerrero",
        "lat": 18.5550,
        "lng": -99.6064,
    },
]


class PueblosMagicosCatalogoExtractor(BaseExtractor):
    periodicidad = "anual"
    schema = "public"
    tabla = "pueblos_magicos_catalogo"
    indicador_clave = INDICADOR_CLAVE
    fuente_id: int = 0

    def extract(self) -> list[dict[str, Any]]:
        csv_url = os.getenv("PUEBLOS_MAGICOS_CSV_URL", "").strip()
        if not csv_url:
            logger.info("Sin URL externa de pueblos mágicos; usando fallback local.")
            return FALLBACK_PUEBLOS
        try:
            df = pd.read_csv(csv_url)
            logger.info("CSV de pueblos mágicos cargado desde URL: %s", csv_url)
            return df.to_dict(orient="records")
        except Exception as exc:
            logger.warning(
                "No se pudo consumir PUEBLOS_MAGICOS_CSV_URL (%s). "
                "Usando fallback local. Error: %s",
                csv_url,
                exc,
            )
            return FALLBACK_PUEBLOS

    def transform(self, raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        now = datetime.now(tz=timezone.utc)
        records: list[dict[str, Any]] = []

        for idx, row in enumerate(raw):
            nombre = str(
                row.get("nombre")
                or row.get("pueblo_magico")
                or row.get("nombre_pueblo")
                or ""
            ).strip()
            entidad = str(
                row.get("entidad")
                or row.get("estado")
                or row.get("nombre_entidad")
                or ""
            ).strip()
            if not nombre or not entidad:
                continue
            clave = str(row.get("clave") or f"pm_{nombre.lower().replace(' ', '_')}").strip()
            lat = row.get("lat") or row.get("latitud")
            lng = row.get("lng") or row.get("longitud") or row.get("lon")
            try:
                lat_val = float(lat)
                lng_val = float(lng)
            except Exception:
                logger.debug("Registro descartado por coordenadas inválidas: %s", row)
                continue

            records.append(
                {
                    "clave": clave or f"pm_{idx}",
                    "nombre": nombre,
                    "entidad": entidad,
                    "municipio": str(row.get("municipio") or "").strip() or None,
                    "lat": lat_val,
                    "lng": lng_val,
                    "fuente": str(row.get("fuente") or "SECTUR catálogo oficial").strip(),
                    "fecha_referencia": now,
                    "activo": True,
                    "updated_at": now,
                }
            )

        return records

    def load(self, records: list[dict[str, Any]]) -> tuple[int, int]:
        if not records:
            return 0, 0

        cargados = 0
        errores = 0
        ddl = """
        CREATE TABLE IF NOT EXISTS public.pueblos_magicos_catalogo (
            id SERIAL PRIMARY KEY,
            clave TEXT UNIQUE NOT NULL,
            nombre TEXT NOT NULL,
            entidad TEXT NOT NULL,
            municipio TEXT,
            lat DOUBLE PRECISION NOT NULL,
            lng DOUBLE PRECISION NOT NULL,
            fuente TEXT NOT NULL,
            fecha_referencia TIMESTAMPTZ,
            activo BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_pueblos_magicos_entidad
            ON public.pueblos_magicos_catalogo (entidad);
        CREATE INDEX IF NOT EXISTS idx_pueblos_magicos_activo
            ON public.pueblos_magicos_catalogo (activo);
        """
        upsert_sql = text(
            """
            INSERT INTO public.pueblos_magicos_catalogo
            (
                clave, nombre, entidad, municipio, lat,
                lng, fuente, fecha_referencia, activo, updated_at
            )
            VALUES
            (
                :clave, :nombre, :entidad, :municipio, :lat,
                :lng, :fuente, :fecha_referencia, :activo, :updated_at
            )
            ON CONFLICT (clave) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                entidad = EXCLUDED.entidad,
                municipio = EXCLUDED.municipio,
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng,
                fuente = EXCLUDED.fuente,
                fecha_referencia = EXCLUDED.fecha_referencia,
                activo = EXCLUDED.activo,
                updated_at = EXCLUDED.updated_at
            """
        )
        with get_db_session() as session:
            try:
                session.execute(text(ddl))
                for record in records:
                    try:
                        session.execute(upsert_sql, record)
                        cargados += 1
                    except Exception:
                        errores += 1
            except Exception as exc:
                logger.error("Error creando/actualizando catálogo de pueblos mágicos: %s", exc)
                raise
        return cargados, errores
