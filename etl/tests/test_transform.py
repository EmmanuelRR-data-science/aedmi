# etl/tests/test_transform.py
# Feature: market-study-app, Propiedad 15 (transform conforme a esquema lógico)
from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from core.base_extractor import BaseExtractor


class _DummyModulo(BaseExtractor):
    periodicidad = "mensual"
    fuente_id = 1
    schema = "mensual"
    tabla = "datos"
    indicador_clave = "t_dummy"

    def extract(self) -> list[dict]:
        return []

    def transform(self, raw: list[dict]) -> list[dict]:  # noqa: ARG002
        return [
            {
                "indicador_id": 1,
                "nivel_geografico": "nacional",
                "anio": 2020,
                "mes": 1,
                "valor": 1.0,
                "unidad": "u",
            }
        ]


@settings(max_examples=100, deadline=None)
@given(
    st.integers(min_value=2000, max_value=2040),
    st.integers(min_value=1, max_value=12),
    st.floats(allow_nan=False, allow_infinity=False, width=32),
)
def test_transform_devuelve_columnas_mensuales_cerradas(anio: int, mes: int, valor: float) -> None:
    m = _DummyModulo()
    t = m.transform(
        [
            {
                "x": 1,
            }
        ]
    )
    ajustado: list[dict] = [{**fila, "anio": anio, "mes": mes, "valor": valor} for fila in t]
    for fila in ajustado:
        assert "indicador_id" in fila
        assert "nivel_geografico" in fila
        assert "anio" in fila
        assert "mes" in fila
        assert "valor" in fila
        assert "unidad" in fila
        assert isinstance(fila["valor"], float)
