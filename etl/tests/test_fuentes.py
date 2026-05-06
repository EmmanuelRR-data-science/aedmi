# etl/tests/test_fuentes.py
# Feature: market-study-app, Propiedades 23, 29 (fuentes / estados iniciales)
from __future__ import annotations

from datetime import datetime, timezone

from hypothesis import given, settings
from hypothesis import strategies as st

from core.models import FuenteDatos, Indicador

EST = st.sampled_from(("pendiente", "etl_listo", "api_lista", "grafica_lista", "completo"))
PER = st.sampled_from(("diario", "semanal", "mensual", "anual", "quinquenal", "otra"))
CATS = st.sampled_from(("demografia", "economia", "turismo", "conectividad_aerea"))
NIV = st.sampled_from(("nacional", "estatal", "municipal", "localidad", "ciudad"))


@settings(max_examples=100, deadline=None)
@given(EST, PER, st.booleans())
def test_nueva_fuente_tiene_estado_en_catalogo(
    estado: str, periodicidad: str, activo: bool
) -> None:
    f = FuenteDatos(
        nombre="F",
        periodicidad=periodicidad,
        modulo_etl="m.x",
        estado=estado,
        activo=activo,
        url_referencia="https://x",
        ultima_carga=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    assert f.estado in {
        "pendiente",
        "etl_listo",
        "api_lista",
        "grafica_lista",
        "completo",
    }


@settings(max_examples=100, deadline=None)
@given(CATS, NIV)
def test_nuevo_indicador_activo_y_categoria_en_catalogo(cat: str, niv: str) -> None:
    i = Indicador(
        clave="k",
        nombre="N",
        categoria=cat,
        nivel_geografico=niv,
    )
    assert i.activo is True
    assert i.categoria in (
        "demografia",
        "economia",
        "turismo",
        "conectividad_aerea",
    )
