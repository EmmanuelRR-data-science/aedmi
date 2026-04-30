# api/tests/test_api_general.py
# Feature: market-study-app, Propiedad 22: Respuestas exitosas JSON válido (muestra /health)
from __future__ import annotations

import json

from fastapi.testclient import TestClient


def test_health_returns_json_object(test_client: TestClient) -> None:
    r = test_client.get("/health")
    assert r.status_code == 200
    text = r.text
    data = json.loads(text)
    assert data == {"status": "ok"}
