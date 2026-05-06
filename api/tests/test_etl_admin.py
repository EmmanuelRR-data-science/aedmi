# api/tests/test_etl_admin.py
# Feature: market-study-app, Propiedades 26, 27, 28
# Admin ETL: autenticación, listado, upload preview, logs
from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from fastapi.testclient import TestClient
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from core.db import get_db
from core.models import FuenteDatos
from main import app
from routers.auth import get_current_user
from tests.mocks import make_async_db_session, row_result


def test_modulos_requiere_bearer() -> None:
    c = TestClient(app)
    r = c.get("/admin/etl/modulos")
    assert r.status_code == 401


def _mock_fuente(fid: int = 1) -> MagicMock:
    f = MagicMock(spec=FuenteDatos)
    f.id = fid
    f.nombre = "Test"
    f.url_referencia = "https://test"
    f.periodicidad = "mensual"
    f.activo = True
    f.estado = "etl_listo"
    return f


def _async_db_modulos() -> AsyncGenerator[AsyncMock, None]:
    fu = _mock_fuente(1)
    r1 = row_result(scalars_list=[fu])
    r2 = row_result()
    r2.scalar_one_or_none.return_value = None
    s = make_async_db_session()
    s.execute = AsyncMock(side_effect=[r1, r2])
    try:
        yield s
    finally:
        pass


def test_modulos_200_fuentes_activas() -> None:
    app.dependency_overrides[get_db] = _async_db_modulos
    app.dependency_overrides[get_current_user] = lambda: "tester"
    c = TestClient(app)
    try:
        r = c.get("/admin/etl/modulos")
    finally:
        app.dependency_overrides.clear()
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["id"] == 1
    assert data[0]["nombre"] == "Test"
    assert data[0]["periodicidad"] == "mensual"
    assert data[0]["estado"] == "etl_listo"


def _async_db_ejecutar() -> AsyncGenerator[AsyncMock, None]:
    s = make_async_db_session()
    s.get = AsyncMock(return_value=_mock_fuente(1))
    try:
        yield s
    finally:
        pass


def test_ejecutar_manual_200_crea_log() -> None:
    app.dependency_overrides[get_db] = _async_db_ejecutar
    app.dependency_overrides[get_current_user] = lambda: "op"
    c = TestClient(app)
    try:
        r = c.post("/admin/etl/modulos/1/ejecutar")
    finally:
        app.dependency_overrides.clear()
    assert r.status_code == 200, r.text
    b = r.json()
    assert b["tipo_ejecucion"] == "manual"
    assert b["fuente_nombre"] == "Test"
    assert b["registros_cargados"] == 0
    assert "administración" in (b.get("mensaje") or "").lower() or b.get("mensaje", "")


def _async_db_upload() -> AsyncGenerator[AsyncMock, None]:
    s = make_async_db_session()
    s.get = AsyncMock(return_value=_mock_fuente(1))
    try:
        yield s
    finally:
        pass


def test_propiedad_26_27_upload_csv_de_vuelve_preview_y_marca_diferencias() -> None:
    app.dependency_overrides[get_db] = _async_db_upload
    app.dependency_overrides[get_current_user] = lambda: "op"
    c = TestClient(app)
    try:
        r = c.post(
            "/admin/etl/modulos/1/upload",
            files={"archivo": ("p.csv", b"col1,col2\n1,2\n2,2\n", "text/csv")},
            data={"columnas_esperadas": "col1,col2"},
        )
        assert r.status_code == 200, r.text
        b = r.json()
        assert b["formato"] == "csv"
        assert b["total_filas"] == 2
        assert len(b["filas_preview"]) == 2
        assert b["columnas_detectadas"] == ["col1", "col2"]
        assert b["hay_diferencias"] is False
        r2 = c.post(
            "/admin/etl/modulos/1/upload",
            files={"archivo": ("d.csv", b"una,dos\n1,2", "text/csv")},
            data={"columnas_esperadas": "a,b,c"},
        )
        assert r2.status_code == 200, r2.text
        d = r2.json()
        assert d["hay_diferencias"] is True
        assert d.get("columnas_faltantes", [])
    finally:
        app.dependency_overrides.clear()


@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    n=st.integers(min_value=0, max_value=9),
    col=st.sampled_from(("a", "b", "x1")),
)
def test_propiedad_26_27_subida_csv_preview_tamano_bajo_5(
    n: int,
    col: str,
) -> None:
    # header + n+1 filas de datos → total_filas (pandas) = n+1
    filas = [f"{col},y"] + [f"{i},9" for i in range(n + 1)]
    cuerpo = "\n".join(filas).encode("utf-8")
    app.dependency_overrides[get_db] = _async_db_upload
    app.dependency_overrides[get_current_user] = lambda: "h"
    c = TestClient(app)
    try:
        r = c.post(
            "/admin/etl/modulos/1/upload",
            files={"archivo": (f"u{n}.csv", cuerpo, "text/csv")},
            data={"columnas_esperadas": f"{col},y"},
        )
    finally:
        app.dependency_overrides.clear()
    assert r.status_code == 200
    b = r.json()
    assert b["total_filas"] == n + 1
    assert b["formato"] == "csv"
    assert len(b["filas_preview"]) == min(5, b["total_filas"])


def _log_row(fuente_id: int = 1) -> tuple[MagicMock, str | None]:
    log = MagicMock()
    t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    log.id = 1
    log.fuente_id = fuente_id
    log.tipo_ejecucion = "auto"
    log.inicio = t0
    log.fin = t0
    log.exitoso = True
    log.registros_cargados = 0
    log.errores = 0
    log.mensaje = None
    log.usuario = "sys"
    return (log, "F1")


def _async_db_logs() -> AsyncGenerator[AsyncMock, None]:
    log, nombre = _log_row(1)
    r_exec = MagicMock()
    r_exec.all.return_value = [(log, nombre)]
    s = make_async_db_session()
    s.execute = AsyncMock(return_value=r_exec)
    try:
        yield s
    finally:
        pass


def test_logs_200_filtro_fuente() -> None:
    app.dependency_overrides[get_db] = _async_db_logs
    app.dependency_overrides[get_current_user] = lambda: "op"
    c = TestClient(app)
    try:
        r = c.get("/admin/etl/logs?fuente_id=1&exitoso=true&fecha_desde=2020-01-01")
    finally:
        app.dependency_overrides.clear()
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["fuente_id"] == 1
    assert rows[0]["exitoso"] is True
    assert rows[0]["fuente_nombre"] == "F1"
