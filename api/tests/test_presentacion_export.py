# api/tests/test_presentacion_export.py
from __future__ import annotations

from core.presentacion_export import (
    construir_pptx_bytes,
    resolver_texto_analisis_exportacion,
)


def test_prioridad_revisado_sobre_ia() -> None:
    t, o = resolver_texto_analisis_exportacion("  Mi texto  ", "IA descartada")
    assert o == "revisado"
    assert t == "Mi texto"


def test_fallback_ia_si_revisado_vacio() -> None:
    t, o = resolver_texto_analisis_exportacion("   \n  ", "Solo IA")
    assert o == "ia"
    assert t == "Solo IA"


def test_vacio_si_ambos_ausentes() -> None:
    t, o = resolver_texto_analisis_exportacion(None, None)
    assert o == "vacio"
    assert "Sin análisis" in t


def test_pptx_no_vacio() -> None:
    raw = construir_pptx_bytes("T", "Cuerpo", "revisado", None)
    assert raw.startswith(b"PK")
    assert len(raw) > 2_000
