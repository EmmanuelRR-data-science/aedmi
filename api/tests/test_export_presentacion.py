# api/tests/test_export_presentacion.py
# Feature: market-study-app, Requisito 25 — integración mínima sin BD real
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from core.db import get_db
from main import app
from routers import export_presentacion as export_presentacion_router
from routers.auth import get_current_user
from tests.mocks import make_async_db_session


def _fila_analisis(rev: str | None, ia: str | None) -> MagicMock:
    m = MagicMock()
    m.analisis_revisado = rev
    m.analisis_ia = ia
    return m


def test_ordenar_puntos_excel_cronologico() -> None:
    from core.presentacion_excel_export import _ordenar_puntos_cronologico

    raw: list[tuple[object, float, str | None]] = [
        (2022, 1.0, None),
        (2018, 2.0, None),
        (2020, 3.0, None),
    ]
    out = _ordenar_puntos_cronologico(raw)
    assert [p[0] for p in out] == [2018, 2020, 2022]

    fechas: list[tuple[object, float, str | None]] = [
        ("2024-06-01", 1.0, None),
        ("2023-12-15", 2.0, None),
    ]
    assert [p[0] for p in _ordenar_puntos_cronologico(fechas)] == ["2023-12-15", "2024-06-01"]


def test_post_export_401_sin_token() -> None:
    c = TestClient(app)
    r = c.post(
        "/export/presentacion",
        json={"grafica_id": 1, "titulo": "Indicador A"},
    )
    assert r.status_code == 401


def test_post_export_devuelve_pptx_revisado_prioritario() -> None:
    res = MagicMock()
    res.scalar_one_or_none.return_value = _fila_analisis("Texto experto", "Solo IA")
    session = make_async_db_session(execute_handler=AsyncMock(return_value=res))

    async def sgen():
        yield session

    app.dependency_overrides[get_db] = sgen
    app.dependency_overrides[get_current_user] = lambda: "PhiQus"
    c = TestClient(app)
    try:
        r = c.post(
            "/export/presentacion",
            json={"grafica_id": 1, "titulo": "Mi título", "entidad_clave": None},
        )
        assert r.status_code == 200
        assert "presentationml" in r.headers.get("content-type", "")
        data = r.content
        assert data[:2] == b"PK"
    finally:
        app.dependency_overrides.clear()


def test_post_export_prefiere_ia_si_revisado_solo_espacios() -> None:
    res = MagicMock()
    res.scalar_one_or_none.return_value = _fila_analisis("  \n\t  ", "Contenido IA")
    session = make_async_db_session(execute_handler=AsyncMock(return_value=res))

    async def sgen():
        yield session

    app.dependency_overrides[get_db] = sgen
    app.dependency_overrides[get_current_user] = lambda: "PhiQus"
    c = TestClient(app)
    try:
        r = c.post(
            "/export/presentacion",
            json={"grafica_id": 1, "titulo": "T", "entidad_clave": None},
        )
        assert r.status_code == 200
        assert len(r.content) > 2_000
    finally:
        app.dependency_overrides.clear()


def test_post_export_lote_401_sin_token() -> None:
    c = TestClient(app)
    r = c.post(
        "/export/presentacion/lote",
        json={
            "titulo_presentacion": "L",
            "items": [
                {
                    "grafica_id": 1,
                    "titulo": "Una",
                    "nivel_geografico": "nacional",
                }
            ],
        },
    )
    assert r.status_code == 401


def test_post_export_lote_devuelve_pptx() -> None:
    res = MagicMock()
    res.scalar_one_or_none.return_value = _fila_analisis("Lote A", "IA A")
    session = make_async_db_session(execute_handler=AsyncMock(return_value=res))

    async def sgen():
        yield session

    app.dependency_overrides[get_db] = sgen
    app.dependency_overrides[get_current_user] = lambda: "PhiQus"
    c = TestClient(app)
    try:
        r = c.post(
            "/export/presentacion/lote",
            json={
                "titulo_presentacion": "Mi lote",
                "items": [
                    {
                        "grafica_id": 1,
                        "titulo": "Gráfica 1",
                        "nivel_geografico": "nacional",
                        "entidad_clave": None,
                    },
                    {
                        "grafica_id": 2,
                        "titulo": "Gráfica 2",
                        "nivel_geografico": "nacional",
                        "entidad_clave": None,
                    },
                ],
            },
        )
        assert r.status_code == 200
        assert "presentationml" in r.headers.get("content-type", "")
        assert r.content[:2] == b"PK"
        assert len(r.content) > 2_000
    finally:
        app.dependency_overrides.clear()


def test_post_export_lote_xlsx_422_sin_datos_serie() -> None:
    res = MagicMock()
    res.scalar_one_or_none.return_value = _fila_analisis("A", "IA")
    session = make_async_db_session(execute_handler=AsyncMock(return_value=res))

    async def sgen():
        yield session

    app.dependency_overrides[get_db] = sgen
    app.dependency_overrides[get_current_user] = lambda: "PhiQus"
    c = TestClient(app)
    try:
        r = c.post(
            "/export/presentacion/lote",
            json={
                "titulo_presentacion": "L",
                "modo_salida": "xlsx",
                "items": [
                    {
                        "grafica_id": 1,
                        "titulo": "Gráfica 1",
                        "nivel_geografico": "nacional",
                        "entidad_clave": None,
                    },
                ],
            },
        )
        assert r.status_code == 422
        assert r.json()["detail"]["error"] == "datos_serie_required"
    finally:
        app.dependency_overrides.clear()


def test_post_export_lote_xlsx_devuelve_xlsx() -> None:
    res = MagicMock()
    res.scalar_one_or_none.return_value = _fila_analisis("Análisis", "IA")
    session = make_async_db_session(execute_handler=AsyncMock(return_value=res))

    async def sgen():
        yield session

    app.dependency_overrides[get_db] = sgen
    app.dependency_overrides[get_current_user] = lambda: "PhiQus"
    c = TestClient(app)
    try:
        r = c.post(
            "/export/presentacion/lote",
            json={
                "titulo_presentacion": "L",
                "modo_salida": "xlsx",
                "items": [
                    {
                        "grafica_id": 1,
                        "titulo": "Gráfica 1",
                        "nivel_geografico": "nacional",
                        "entidad_clave": None,
                        "datos_serie": [{"periodo": 2020, "valor": 123.45}],
                        "excel_chart_kind": "column",
                    },
                ],
            },
        )
        assert r.status_code == 200
        assert "spreadsheetml" in r.headers.get("content-type", "")
        assert r.content[:2] == b"PK"
    finally:
        app.dependency_overrides.clear()


def test_post_export_lote_zip_devuelve_zip() -> None:
    res = MagicMock()
    res.scalar_one_or_none.return_value = _fila_analisis("A", "IA")
    session = make_async_db_session(execute_handler=AsyncMock(return_value=res))

    async def sgen():
        yield session

    app.dependency_overrides[get_db] = sgen
    app.dependency_overrides[get_current_user] = lambda: "PhiQus"
    c = TestClient(app)
    try:
        r = c.post(
            "/export/presentacion/lote",
            json={
                "titulo_presentacion": "L",
                "modo_salida": "zip_pptx_xlsx",
                "items": [
                    {
                        "grafica_id": 1,
                        "titulo": "Gráfica 1",
                        "nivel_geografico": "nacional",
                        "entidad_clave": None,
                        "datos_serie": [{"periodo": 2020, "valor": 1.0}],
                    },
                ],
            },
        )
        assert r.status_code == 200
        assert r.headers.get("content-type") == "application/zip"
        assert r.content[:2] == b"PK"
    finally:
        app.dependency_overrides.clear()


def test_post_export_gamma_503_sin_api_key() -> None:
    res = MagicMock()
    res.scalar_one_or_none.return_value = None
    session = make_async_db_session(execute_handler=AsyncMock(return_value=res))

    async def sgen():
        yield session

    sm = MagicMock()
    sm.gamma_api_key = ""
    sm.theme_id = "t"
    sm.gamma_id = ""

    app.dependency_overrides[get_db] = sgen
    app.dependency_overrides[get_current_user] = lambda: "PhiQus"
    c = TestClient(app)
    try:
        with patch.object(export_presentacion_router, "get_settings", return_value=sm):
            r = c.post(
                "/export/presentacion/gamma",
                json={
                    "titulo_presentacion": "T",
                    "items": [
                        {
                            "grafica_id": 1,
                            "titulo": "Una",
                            "nivel_geografico": "nacional",
                        }
                    ],
                },
            )
        assert r.status_code == 503
    finally:
        app.dependency_overrides.clear()


def test_build_gamma_user_message_incluye_tabla_y_theme() -> None:
    from core.gamma_export import (
        GammaDatoSeriePunto,
        GammaIndicadorBloque,
        build_gamma_user_message,
    )

    bloques = [
        GammaIndicadorBloque(
            orden=1,
            titulo="Indicador X",
            subtitulo=None,
            nivel_geografico="nacional",
            origen="vacio",
            texto_analisis="Texto",
            imagen_png=None,
            datos_serie=(
                GammaDatoSeriePunto(
                    periodo=2020,
                    valor=1_234_567.89,
                    entidad_clave="Serie A",
                    unidad="MXN",
                ),
            ),
        )
    ]
    msg = build_gamma_user_message(
        titulo_presentacion="Lote",
        theme_id="theme-abc",
        bloques=bloques,
    )
    assert "theme-abc" in msg
    assert "Tabla de datos (origen aplicación)" in msg
    assert "2020" in msg and "Serie A" in msg
    assert "Plantilla base" not in msg
    assert "{{PALETTE_INSTRUCTIONS}}" not in msg
    assert "secundario" in msg.lower() or "acentos" in msg.lower()


def test_build_gamma_incluye_paleta_dashboard() -> None:
    from core.gamma_export import GammaIndicadorBloque, build_gamma_user_message

    bloques = [
        GammaIndicadorBloque(
            orden=1,
            titulo="X",
            subtitulo=None,
            nivel_geografico="nacional",
            origen="vacio",
            texto_analisis="—",
            imagen_png=None,
        )
    ]
    msg = build_gamma_user_message(
        titulo_presentacion="T",
        theme_id="th1",
        bloques=bloques,
        paleta_hex=["#111111", "#222222", "#333333"],
    )
    assert "paleta activa" in msg.lower()
    assert "#111111" in msg and "#222222" in msg
    assert "primario (app)" in msg
