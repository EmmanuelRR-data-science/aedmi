# api/tests/conftest.py
from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

# Variables mínimas antes de importar la app
os.environ.setdefault("POSTGRES_USER", "test")
os.environ.setdefault("POSTGRES_PASSWORD", "test")
os.environ.setdefault("POSTGRES_DB", "test")
os.environ.setdefault("POSTGRES_HOST", "127.0.0.1")
os.environ.setdefault("JWT_SECRET", "a" * 32)
os.environ.setdefault("ADMIN_PASSWORD", "TestAdminSecret!")
os.environ.setdefault("GROQ_API_KEY", "test-groq-key")

from core.db import get_db
from main import app
from routers.auth import get_current_user
from tests.mocks import make_async_db_session


async def empty_get_db() -> AsyncGenerator[AsyncMock, None]:
    session = make_async_db_session()
    try:
        yield session
    finally:
        pass


def clear_auth_state() -> None:
    from core import auth

    auth._token_blacklist.clear()  # noqa: SLF001


@pytest.fixture
def clear_tokens() -> None:
    clear_auth_state()
    yield
    clear_auth_state()


@pytest.fixture
def client_with_mocks() -> TestClient:
    app.dependency_overrides[get_db] = empty_get_db
    app.dependency_overrides[get_current_user] = lambda: "PhiQus"
    c = TestClient(app)
    yield c
    app.dependency_overrides.clear()


@pytest.fixture
def client_db_only() -> TestClient:
    """Mock de BD; JWT y blacklist reales (para probar 401 con token inválido)."""
    app.dependency_overrides[get_db] = empty_get_db
    c = TestClient(app)
    yield c
    app.dependency_overrides.clear()


@pytest.fixture
def test_client() -> TestClient:
    return TestClient(app)
