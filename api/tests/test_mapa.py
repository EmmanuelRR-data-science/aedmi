# api/tests/test_mapa.py
# Feature: market-study-app — módulo mapa (fuentes, catálogo, export)
from __future__ import annotations

from fastapi.testclient import TestClient

from main import app


def test_mapa_fuentes_requiere_auth() -> None:
    c = TestClient(app)
    r = c.get("/mapa/fuentes")
    assert r.status_code == 401


def test_mapa_fuentes_200(client_with_mocks: TestClient) -> None:
    r = client_with_mocks.get("/mapa/fuentes")
    assert r.status_code == 200
    data = r.json()
    assert len(data) >= 3
    assert "nombre" in data[0] and "url" in data[0]


def test_mapa_indicadores_catalogo_200(client_with_mocks: TestClient) -> None:
    r = client_with_mocks.get("/mapa/indicadores/catalogo")
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 10
    assert rows[0].get("clave")


def test_mapa_export_pdf_200(client_with_mocks: TestClient) -> None:
    r = client_with_mocks.post(
        "/mapa/export/pdf",
        json={
            "lat": 20.5,
            "lng": -100.3,
            "radio_m": 3000,
            "ciudad": "Test Ciudad",
            "capas": [],
            "capas_datos": [],
            "titulo": "Informe test",
            "notas": "Nota",
        },
    )
    assert r.status_code == 200
    assert r.headers.get("content-type", "").startswith("application/pdf")
    assert r.content[:4] == b"%PDF"
