# api/routers/indicadores.py
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.db import get_db
from core.models import Indicador
from routers.auth import get_current_user
from schemas.indicador import IndicadorResponse

router = APIRouter(prefix="/indicadores", tags=["indicadores"])

NIVELES_VALIDOS = {"nacional", "estatal", "municipal", "localidad", "ciudad"}
CATEGORIAS_VALIDAS = {"demografia", "economia", "turismo", "conectividad_aerea"}


def _sanitize_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return text

    replacements = {
        "inversi??n": "inversión",
        "Inversi??n": "Inversión",
        "Proyecci??n": "Proyección",
        "Distribuci??n": "Distribución",
        "distribuci??n": "distribución",
        "Participaci??n": "Participación",
        "participaci??n": "participación",
        "Composici??n": "Composición",
        "Demograf??a": "Demografía",
        "Pa??s": "País",
        "Ocupaci??n": "Ocupación",
        "ocupaci??n": "ocupación",
        "Inflaci??n": "Inflación",
        "Variaci??n": "Variación",
        "variaci??n": "variación",
        "econ??mica": "económica",
        "econ??mico": "económico",
        "Econ??mico": "Económico",
        "Econ??micamente": "Económicamente",
        "a??rea": "aérea",
        "a??reas": "aéreas",
        "A??rea": "Aérea",
        "A??reas": "Aéreas",
        "a??reo": "aéreo",
        "A??reo": "Aéreo",
        "a??os": "años",
        "A??os": "Años",
        "n??mero": "número",
        "N??mero": "Número",
        "a??o": "año",
        "A??o": "Año",
        "pa??s": "país",
        "Le??n": "León",
        "m??s": "más",
        "M??s": "Más",
        "Extensi??n": "Extensión",
        "d??lares": "dólares",
        "D??lares": "Dólares",
        "d??lar": "dólar",
        "D??lar": "Dólar",
        "tur??sticos": "turísticos",
        "Tur??sticos": "Turísticos",
        "tur??sticas": "turísticas",
        "Tur??sticas": "Turísticas",
        "M??rida": "Mérida",
        "M??xico": "México",
        "Yucat??n": "Yucatán",
        "Quer??taro": "Querétaro",
        "c??pita": "cápita",
        "Poblaci??n": "Población",
        "poblaci??n": "población",
        "Poblaci�n": "Población",
        "poblaci�n": "población",
        "Inflaci��n": "Inflación",
        "variaci��n": "variación",
        "M��xico": "México",
        "Poblaci��n": "Población",
        "econ�mica": "económica",
        "a�rea": "aérea",
        "tur�sticos": "turísticos",
        "Yucat�n": "Yucatán",
        "Quer�taro": "Querétaro",
        "M�rida": "Mérida",
        "c�pita": "cápita",
        "participaci�n": "participación",
        "Participaci�n": "Participación",
        "a�reo": "aéreo",
        "A�reo": "Aéreo",
        "a�os": "años",
        "A�os": "Años",
        "n�mero": "número",
        "N�mero": "Número",
        "a�o": "año",
        "A�o": "Año",
        "pa�s": "país",
        "Pa�s": "País",
        "Le�n": "León",
        "m�s": "más",
        "M�s": "Más",
        "Extensi�n": "Extensión",
        "d�lares": "dólares",
        "D�lares": "Dólares",
        "d�lar": "dólar",
        "D�lar": "Dólar",
        "Composici�n": "Composición",
        "Demograf�a": "Demografía",
    }
    for bad, good in replacements.items():
        text = text.replace(bad, good)

    # Intentar corregir mojibake clásico UTF-8 interpretado como latin-1.
    if "Ã" in text:
        try:
            decoded = text.encode("latin-1").decode("utf-8")
            if decoded:
                text = decoded
        except Exception:
            pass
    return text


def _sanitize_obj(value):
    if isinstance(value, dict):
        return {k: _sanitize_obj(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_obj(v) for v in value]
    if isinstance(value, str):
        return _sanitize_text(value)
    return value


@router.get("", response_model=list[IndicadorResponse])
async def list_indicadores(
    nivel_geografico: str | None = Query(None),
    categoria: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> list[IndicadorResponse]:
    if nivel_geografico and nivel_geografico not in NIVELES_VALIDOS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"nivel_geografico inválido. Valores permitidos: {NIVELES_VALIDOS}",
        )
    if categoria and categoria not in CATEGORIAS_VALIDAS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"categoria inválida. Valores permitidos: {CATEGORIAS_VALIDAS}",
        )

    stmt = select(Indicador).where(Indicador.activo.is_(True))
    if nivel_geografico:
        stmt = stmt.where(Indicador.nivel_geografico == nivel_geografico)
    if categoria:
        stmt = stmt.where(Indicador.categoria == categoria)

    result = await db.execute(stmt)
    indicadores = list(result.scalars().all())
    return [
        IndicadorResponse(
            id=i.id,
            clave=i.clave,
            nombre=_sanitize_text(i.nombre) or i.nombre,
            categoria=i.categoria,
            nivel_geografico=i.nivel_geografico,
            unidad=_sanitize_text(i.unidad),
            descripcion=_sanitize_text(i.descripcion),
            tipo_grafica=i.tipo_grafica,
        )
        for i in indicadores
    ]


@router.get("/{indicador_id}/datos")
async def get_datos_indicador(
    indicador_id: int,
    entidad_prefix: str | None = Query(None),
    limit: int | None = Query(None, ge=1, le=1_000_000),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> dict:
    from sqlalchemy import text

    stmt = select(Indicador).where(Indicador.id == indicador_id, Indicador.activo.is_(True))
    result = await db.execute(stmt)
    indicador = result.scalar_one_or_none()

    if indicador is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Indicador no encontrado."
        )

    # Determinar schema y columna de período según periodicidad de la fuente
    from core.models import FuenteDatos

    fuente_result = await db.execute(
        select(FuenteDatos).where(FuenteDatos.id == indicador.fuente_id)
    )
    fuente = fuente_result.scalar_one_or_none()
    periodicidad = fuente.periodicidad if fuente else "mensual"

    schema_map = {
        "diario": ("diario", "fecha"),
        "semanal": ("mensual", "anio, mes"),
        "mensual": ("mensual", "anio, mes"),
        "anual": ("anual", "periodo"),
        # quinquenal y otra se almacenan en anual.datos (columna periodo = año)
        "quinquenal": ("anual", "periodo"),
        "otra": ("anual", "periodo"),
    }
    schema, _ = schema_map.get(periodicidad, ("mensual", "anio, mes"))
    annual_limit = 1000000 if indicador.nivel_geografico in {"municipal", "localidad"} else 100000
    effective_limit = limit if limit is not None else annual_limit
    base_where = "indicador_id = :iid"
    params: dict[str, object] = {"iid": indicador_id}
    if entidad_prefix:
        base_where += " AND COALESCE(entidad_clave, '') LIKE :ep"
        params["ep"] = f"{entidad_prefix}%"

    if schema == "diario":
        sql = text(
            f"WITH dedup AS ("  # noqa: S608
            f"SELECT DISTINCT ON (fecha, COALESCE(entidad_clave, '')) "
            f"id, indicador_id, nivel_geografico, entidad_clave, valor, unidad, fecha "
            f"FROM {schema}.datos "
            f"WHERE {base_where} "
            f"ORDER BY fecha, COALESCE(entidad_clave, ''), id DESC"
            f") "
            f"SELECT id, indicador_id, nivel_geografico, entidad_clave, valor, unidad, "
            f"fecha::text AS periodo FROM dedup "
            f"ORDER BY fecha DESC LIMIT 5000"
        )
    elif schema in ("anual", "quinquenal"):
        sql = text(
            f"WITH dedup AS ("  # noqa: S608
            f"SELECT DISTINCT ON (periodo, COALESCE(entidad_clave, '')) "
            f"id, indicador_id, nivel_geografico, entidad_clave, valor, unidad, periodo "
            f"FROM {schema}.datos "
            f"WHERE {base_where} "
            f"ORDER BY periodo, COALESCE(entidad_clave, ''), id DESC"
            f") "
            f"SELECT id, indicador_id, nivel_geografico, entidad_clave, valor, unidad, "
            f"periodo::text AS periodo FROM dedup "
            f"ORDER BY periodo DESC LIMIT {effective_limit}"
        )
    else:
        sql = text(
            f"WITH dedup AS ("  # noqa: S608
            f"SELECT DISTINCT ON (anio, mes, COALESCE(entidad_clave, '')) "
            f"id, indicador_id, nivel_geografico, entidad_clave, valor, unidad, anio, mes "
            f"FROM {schema}.datos "
            f"WHERE {base_where} "
            f"ORDER BY anio, mes, COALESCE(entidad_clave, ''), id DESC"
            f") "
            f"SELECT id, indicador_id, nivel_geografico, entidad_clave, valor, unidad, "
            f"anio, mes, (anio::text || '-' || LPAD(mes::text, 2, '0')) AS periodo "
            f"FROM dedup "
            f"ORDER BY anio DESC, mes DESC LIMIT 100000"
        )

    datos_result = await db.execute(sql, params)
    rows = datos_result.mappings().all()

    return {
        "indicador_id": indicador_id,
        "clave": indicador.clave,
        "nombre": _sanitize_text(indicador.nombre),
        "unidad": _sanitize_text(indicador.unidad),
        "periodicidad": periodicidad,
        "datos": [_sanitize_obj(dict(r)) for r in rows],
    }


@router.get("/{indicador_id}/opciones-geograficas")
async def get_opciones_geograficas(
    indicador_id: int,
    estado: str | None = Query(None),
    municipio: str | None = Query(None),
    q: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> dict[str, list[str]]:
    from sqlalchemy import text

    stmt = select(Indicador).where(Indicador.id == indicador_id, Indicador.activo.is_(True))
    result = await db.execute(stmt)
    indicador = result.scalar_one_or_none()
    if indicador is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Indicador no encontrado."
        )

    if indicador.nivel_geografico not in {"municipal", "localidad"}:
        return {"estados": [], "municipios": [], "localidades": []}

    if indicador.nivel_geografico == "municipal":
        # Formato esperado: mun:<estado>:<municipio>:<...> o mun_age:<estado>:<municipio>:<...>
        estados_sql = text(
            "SELECT DISTINCT split_part(entidad_clave, ':', 2) AS estado "
            "FROM anual.datos WHERE indicador_id = :iid AND entidad_clave IS NOT NULL "
            "ORDER BY estado"
        )
        estados_rows = await db.execute(estados_sql, {"iid": indicador_id})
        estados = [r["estado"] for r in estados_rows.mappings().all() if r.get("estado")]

        municipios: list[str] = []
        if estado:
            municipios_sql = text(
                "SELECT DISTINCT split_part(entidad_clave, ':', 3) AS municipio "
                "FROM anual.datos "
                "WHERE indicador_id = :iid AND split_part(entidad_clave, ':', 2) = :estado "
                "ORDER BY municipio"
            )
            municipios_rows = await db.execute(
                municipios_sql, {"iid": indicador_id, "estado": estado}
            )
            municipios = [
                r["municipio"] for r in municipios_rows.mappings().all() if r.get("municipio")
            ]

        return {"estados": estados, "municipios": municipios, "localidades": []}

    # localidad
    estados_sql = text(
        "SELECT DISTINCT split_part(entidad_clave, ':', 2) AS estado "
        "FROM anual.datos WHERE indicador_id = :iid AND entidad_clave IS NOT NULL "
        "ORDER BY estado"
    )
    estados_rows = await db.execute(estados_sql, {"iid": indicador_id})
    estados = [r["estado"] for r in estados_rows.mappings().all() if r.get("estado")]

    municipios: list[str] = []
    localidades: list[str] = []
    if estado:
        municipios_sql = text(
            "SELECT DISTINCT split_part(entidad_clave, ':', 3) AS municipio "
            "FROM anual.datos "
            "WHERE indicador_id = :iid AND split_part(entidad_clave, ':', 2) = :estado "
            "ORDER BY municipio"
        )
        municipios_rows = await db.execute(municipios_sql, {"iid": indicador_id, "estado": estado})
        municipios = [
            r["municipio"] for r in municipios_rows.mappings().all() if r.get("municipio")
        ]

    if estado and municipio:
        if q:
            localidades_sql = text(
                "SELECT DISTINCT split_part(entidad_clave, ':', 4) AS localidad "
                "FROM anual.datos "
                "WHERE indicador_id = :iid "
                "AND split_part(entidad_clave, ':', 2) = :estado "
                "AND split_part(entidad_clave, ':', 3) = :municipio "
                "AND split_part(entidad_clave, ':', 4) ILIKE :q "
                "ORDER BY localidad LIMIT 100"
            )
            localidades_rows = await db.execute(
                localidades_sql,
                {"iid": indicador_id, "estado": estado, "municipio": municipio, "q": f"%{q}%"},
            )
        else:
            localidades_sql = text(
                "SELECT DISTINCT split_part(entidad_clave, ':', 4) AS localidad "
                "FROM anual.datos "
                "WHERE indicador_id = :iid "
                "AND split_part(entidad_clave, ':', 2) = :estado "
                "AND split_part(entidad_clave, ':', 3) = :municipio "
                "ORDER BY localidad LIMIT 200"
            )
            localidades_rows = await db.execute(
                localidades_sql,
                {"iid": indicador_id, "estado": estado, "municipio": municipio},
            )
        localidades = [
            r["localidad"] for r in localidades_rows.mappings().all() if r.get("localidad")
        ]

    return {"estados": estados, "municipios": municipios, "localidades": localidades}


@router.get("/kpis/nacionales")
async def get_kpis_nacionales(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> dict:
    """Retorna el valor más reciente de los 4 KPIs nacionales."""
    from sqlalchemy import text

    claves = [
        "banxico.pib_trimestral_mxn",
        "banxico.pib_trimestral_usd",
        "banxico.tipo_cambio_usd_mxn",
        "banxico.inflacion_inpc_anual",
    ]

    kpis = {}
    for clave in claves:
        # Obtener indicador
        ind_result = await db.execute(
            select(Indicador).where(Indicador.clave == clave, Indicador.activo.is_(True))
        )
        indicador = ind_result.scalar_one_or_none()
        if indicador is None:
            kpis[clave] = {"valor": None, "fecha": None, "unidad": None}
            continue

        from core.models import FuenteDatos

        fuente_result = await db.execute(
            select(FuenteDatos).where(FuenteDatos.id == indicador.fuente_id)
        )
        fuente = fuente_result.scalar_one_or_none()
        periodicidad = fuente.periodicidad if fuente else "mensual"

        if periodicidad == "diario":
            sql = text(
                "SELECT valor, unidad, fecha::text AS fecha FROM diario.datos "  # noqa: S608
                "WHERE indicador_id = :iid ORDER BY fecha DESC LIMIT 1"
            )
        else:
            sql = text(
                "SELECT valor, unidad, "  # noqa: S608
                "(anio::text || '-' || LPAD(mes::text, 2, '0')) AS fecha "
                "FROM mensual.datos "
                "WHERE indicador_id = :iid ORDER BY anio DESC, mes DESC LIMIT 1"
            )

        row_result = await db.execute(sql, {"iid": indicador.id})
        row = row_result.mappings().first()

        kpis[clave] = {
            "nombre": _sanitize_text(indicador.nombre),
            "valor": float(row["valor"]) if row and row["valor"] is not None else None,
            "fecha": row["fecha"] if row else None,
            "unidad": _sanitize_text(indicador.unidad),
        }

    return kpis


@router.get("/estados/info")
async def get_estados_info(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
) -> dict:
    """Retorna PIB, población y extensión de los 32 estados."""
    from sqlalchemy import text

    sql = text(
        "SELECT entidad_clave, valor FROM anual.datos "  # noqa: S608
        "WHERE indicador_id = "
        "(SELECT id FROM public.indicadores WHERE clave = 'inegi.estados_info') "
        "ORDER BY entidad_clave"
    )
    result = await db.execute(sql)
    rows = result.mappings().all()

    estados: dict[str, dict] = {}
    for r in rows:
        clave = r["entidad_clave"]
        parts = clave.split(":", 1)
        if len(parts) != 2:
            continue
        metric, estado = parts
        if estado not in estados:
            estados[estado] = {
                "estado": estado,
                "pib": 0,
                "poblacion": 0,
                "extension": 0,
                "pib_percapita": 0,
            }
        if metric == "pib":
            estados[estado]["pib"] = float(r["valor"])
        elif metric == "pob":
            estados[estado]["poblacion"] = float(r["valor"])
        elif metric == "ext":
            estados[estado]["extension"] = float(r["valor"])

    # Calcular PIB per cápita
    for e in estados.values():
        if e["poblacion"] > 0:
            e["pib_percapita"] = round(e["pib"] * 1_000_000 / e["poblacion"], 0)

    return {"estados": sorted(estados.values(), key=lambda x: x["estado"])}
