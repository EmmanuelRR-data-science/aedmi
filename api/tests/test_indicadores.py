# api/tests/test_indicadores.py
# Feature: market-study-app, Propiedades 19, 20 (filtros y 400)
from __future__ import annotations

from fastapi.testclient import TestClient
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

NIVELES_NO_VALIDOS = st.sampled_from(("europeo", "NACIONAL", "nacionalx", "xxx", "regional"))
CATEG_NO_VALIDAS = st.sampled_from(("clima", "finanzas", "DEMOGRAFIA", "otra", "9"))


@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(NIVELES_NO_VALIDOS)
def test_invalid_nivel_400(client_with_mocks: TestClient, nivel: str) -> None:
    r = client_with_mocks.get(f"/indicadores?nivel_geografico={nivel}")
    assert r.status_code == 400, r.text


@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(CATEG_NO_VALIDAS)
def test_invalid_categoria_400(client_with_mocks: TestClient, categ: str) -> None:
    r = client_with_mocks.get(f"/indicadores?categoria={categ}")
    assert r.status_code == 400, r.text


@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    st.sampled_from(
        [
            "nacional",
            "estatal",
            "municipal",
            "localidad",
            "ciudad",
        ]
    )
)
def test_valid_niveles_200(client_with_mocks: TestClient, nivel: str) -> None:
    r = client_with_mocks.get(f"/indicadores?nivel_geografico={nivel}")
    assert r.status_code == 200
    assert r.json() == [] or isinstance(r.json(), list)
