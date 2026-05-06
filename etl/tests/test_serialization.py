# etl/tests/test_serialization.py
# Feature: market-study-app, Propiedad 18 (round-trip)
from __future__ import annotations

import json
from datetime import datetime, timezone

from hypothesis import given, settings
from hypothesis import strategies as st

from core.models import FuenteDatos, Indicador, RegistroDato

PERIODS = st.sampled_from(("diario", "semanal", "mensual", "anual", "quinquenal", "otra"))
ESTADOS = st.sampled_from(("pendiente", "etl_listo", "api_lista", "grafica_lista", "completo"))
CATS = st.sampled_from(("demografia", "economia", "turismo", "conectividad_aerea"))
NIV = st.sampled_from(("nacional", "estatal", "municipal", "localidad", "ciudad"))


@settings(max_examples=100, deadline=None)
@given(PERIODS, ESTADOS, st.booleans())
def test_fuente_datos_model_dump_and_validate(periodicidad: str, estado: str, activo: bool) -> None:
    # Feature: market-study-app, Propiedad 18
    f = FuenteDatos(
        id=1,
        nombre="Fuente prueba",
        url_referencia="https://example.com/x",
        periodicidad=periodicidad,
        ultima_carga=datetime(2021, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        modulo_etl="m.test",
        estado=estado,
        activo=activo,
    )
    d = f.model_dump()
    s = json.dumps(d, default=str)
    f2 = FuenteDatos.model_validate(json.loads(s))
    assert f2 == f


@settings(max_examples=100, deadline=None)
@given(CATS, NIV, st.booleans())
def test_indicador_model_dump(categoria: str, nivel: str, activo: bool) -> None:
    i = Indicador(
        id=2,
        clave="clave_x",
        nombre="Nombre",
        categoria=categoria,
        nivel_geografico=nivel,
        unidad="u",
        fuente_id=1,
        descripcion="d",
        tipo_grafica="bar",
        activo=activo,
    )
    i2 = Indicador.model_validate(i.model_dump())
    assert i2 == i


@settings(max_examples=100, deadline=None)
@given(
    st.integers(min_value=1, max_value=10_000),
    NIV,
    st.floats(allow_nan=False, allow_infinity=False, width=32),
)
def test_registro_dato_model_dump(iid: int, nivel: str, valor: float) -> None:
    r = RegistroDato(
        indicador_id=iid,
        nivel_geografico=nivel,
        entidad_clave="00",
        valor=valor,
        unidad="x",
    )
    r2 = RegistroDato.model_validate(r.model_dump())
    assert r2 == r
