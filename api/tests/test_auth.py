# api/tests/test_auth.py
# Feature: market-study-app — Propiedades 2, 3, 4, 21 (autenticación)
from __future__ import annotations

import string

from fastapi.testclient import TestClient
from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.strategies import composite

from core.config import get_settings
from main import app

USER = "PhiQus"


@composite
def wrong_passwords(draw) -> str:
    s = draw(st.text(alphabet=string.printable, min_size=0, max_size=32))
    cfg = get_settings()
    if s == cfg.admin_password:
        return s + "x"
    return s


@settings(max_examples=100, deadline=None)
@given(wrong_passwords())
def test_invalid_credentials_generic_error(wrong: str) -> None:
    c = TestClient(app)
    r = c.post(
        "/auth/login",
        json={"username": get_settings().admin_user, "password": wrong},
    )
    assert r.status_code == 401
    assert "detail" in r.json()


@settings(max_examples=100, deadline=None)
@given(st.text(min_size=0, max_size=8))
def test_valid_password_returns_bearer(_salt: str) -> None:
    c = TestClient(app)
    cfg = get_settings()
    r = c.post(
        "/auth/login",
        json={"username": cfg.admin_user, "password": cfg.admin_password},
    )
    assert r.status_code == 200
    data = r.json()
    assert "access_token" in data
    assert data.get("token_type") == "bearer"
    assert len(data["access_token"]) > 20


def test_logout_invalidates_token(clear_tokens, client_db_only: TestClient) -> None:
    """Propiedad 4: tras logout, el token ya no acepta rutas protegidas."""
    cfg = get_settings()
    r1 = client_db_only.post(
        "/auth/login",
        json={"username": cfg.admin_user, "password": cfg.admin_password},
    )
    assert r1.status_code == 200
    token = r1.json()["access_token"]
    r2 = client_db_only.post(
        "/auth/logout",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r2.status_code in (200, 204)
    r3 = client_db_only.get(
        "/indicadores",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r3.status_code == 401


def test_malformed_token_401(clear_tokens, client_db_only: TestClient) -> None:
    # Propiedad 21 (token inválido)
    r = client_db_only.get(
        "/indicadores",
        headers={"Authorization": "Bearer not-a-real.jwt.here"},
    )
    assert r.status_code == 401
