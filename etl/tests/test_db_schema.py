# etl/tests/test_db_schema.py
# Feature: market-study-app, Propiedad 24 (esquema inicial)
from __future__ import annotations

from pathlib import Path


def _init_sql() -> str:
    root = Path(__file__).resolve().parents[2]
    p = root / "db" / "init.sql"
    return p.read_text(encoding="utf-8", errors="replace")


def test_ddl_incluye_tablas_nucleo_y_referencia_indicador() -> None:
    t = _init_sql()
    assert "indicadores" in t
    assert "fuentes_datos" in t
    assert "nivel_geografico" in t
    assert "indicador_id" in t
