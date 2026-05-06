# api/tests/test_analisis.py
# Feature: market-study-app, Propiedades 9–14 (muestras: GET vacío, POST IA con Groq mock)
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient
from hypothesis import given, settings
from hypothesis import strategies as st

from core.db import get_db
from main import app
from routers.auth import get_current_user
from tests.mocks import make_async_db_session, row_result


def test_get_analisis_devuelve_null_si_no_hay_fila() -> None:
    rmock = row_result(one=None)
    session = make_async_db_session(execute_handler=AsyncMock(return_value=rmock))

    async def sgen():
        yield session

    app.dependency_overrides[get_db] = sgen
    app.dependency_overrides[get_current_user] = lambda: "PhiQus"
    c = TestClient(app)
    try:
        res = c.get("/analisis/1")
        assert res.status_code == 200
        assert res.json() is None
    finally:
        app.dependency_overrides.clear()


def _fake_indicador() -> SimpleNamespace:
    return SimpleNamespace(
        id=1,
        nombre="N",
        unidad="U",
        nivel_geografico="nacional",
        fuente_id=1,
    )


@settings(
    max_examples=100,
    deadline=None,
)
@given(
    st.lists(
        st.dictionaries(
            keys=st.text(min_size=1, max_size=8, alphabet="abcdefghijklmnopqrstuvwxyz_"),
            values=st.text(max_size=20),
            min_size=1,
        ),
        min_size=1,
        max_size=5,
    )
)
def test_post_ia_groq_mock_persiste_texto(
    filas: list[dict],
) -> None:
    m_ind = MagicMock()
    m_ind.scalar_one_or_none.return_value = _fake_indicador()
    m_ana = MagicMock()
    m_ana.scalar_one_or_none.return_value = None
    session = make_async_db_session(execute_handler=AsyncMock(side_effect=[m_ind, m_ana]))

    async def sgen():
        yield session

    app.dependency_overrides[get_db] = sgen
    app.dependency_overrides[get_current_user] = lambda: "PhiQus"
    c = TestClient(app)
    try:
        with patch("routers.analisis._call_groq", new_callable=AsyncMock) as g:
            g.return_value = "Explicación IA de prueba"
            res = c.post(
                "/analisis/1/ia",
                json={"datos_filtrados": list(filas)[:5]},
            )
        assert res.status_code == 200, res.text
        body = res.json()
        assert body.get("analisis_ia") == "Explicación IA de prueba"
    finally:
        app.dependency_overrides.clear()
