# etl/tests/test_etl_core.py
# Feature: market-study-app, Propiedades 16, 17, 25
# Contrato mínimo del núcleo ETL (incl. verificación fuentes_datos)
from __future__ import annotations

import inspect
from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

import core.fuente_check as fuente_check
from core.base_extractor import BaseExtractor, ExtractorResult
from core.fuente_check import fuente_activa_y_registrada


def test_load_verifica_fuentes_y_on_conflict() -> None:
    src = inspect.getsource(BaseExtractor.load)
    assert "fuente_activa_y_registrada" in src
    assert "ON CONFLICT" in src
    assert "DO NOTHING" in src
    fsrc = inspect.getsource(fuente_check.fuente_activa_y_registrada)
    assert "fuentes_datos" in fsrc


def test_extractor_result_tiene_tiempos_y_mensaje() -> None:
    r = ExtractorResult(indicador="x", registros=1, errores=0, exitoso=True, mensaje="ok")
    assert r.indicador == "x"
    assert r.registros == 1
    assert r.inicio
    assert r.fin


@settings(max_examples=100, deadline=None)
@given(st.integers(min_value=1, max_value=1_000_000), st.booleans(), st.booleans())
def test_propiedad_25_fuentes_activa_y_registrada_por_fila(
    fuente_id: int, tiene_fila: bool, activo: bool
) -> None:
    s = MagicMock()
    ex = s.execute.return_value
    ex.fetchone.return_value = None
    if tiene_fila:
        ex.fetchone.return_value = (fuente_id, activo)
    out = fuente_activa_y_registrada(s, fuente_id)
    if not tiene_fila or not activo:
        assert out is False
    else:
        assert out is True
    s.execute.assert_called_once()
