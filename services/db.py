"""
Capa de acceso a PostgreSQL para KPIs.
"""

import os
from contextlib import contextmanager

import psycopg2


def _get_db_host():
    """Host de PostgreSQL. Usa localhost si 'db' no está definido (ejecución local fuera de Docker)."""
    host = os.getenv("POSTGRES_HOST", "db")
    if host == "db":
        # Fuera de Docker, 'db' no se resuelve; usar localhost (puerto expuesto)
        try:
            import socket
            socket.gethostbyname("db")
        except (socket.gaierror, OSError):
            return "localhost"
    return host


def _get_db_port():
    """Puerto: 5432 dentro de Docker, 5433 (o POSTGRES_PORT) cuando se usa localhost."""
    return os.getenv("POSTGRES_PORT", "5432")


def get_conn():
    """Conexión a PostgreSQL."""
    host = _get_db_host()
    port = os.getenv("POSTGRES_PORT", "5432")
    return psycopg2.connect(
        host=host,
        port=int(port),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "dash_db"),
    )


@contextmanager
def db_connection():
    """Context manager para conexión a la base de datos."""
    conn = None
    try:
        conn = get_conn()
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


REQUIRED_INDICATORS = ("pib_usd", "tipo_cambio", "inflacion", "pib_mxn")


def get_kpis_nacional_from_db() -> dict | None:
    """
    Obtiene los KPIs nacionales desde PostgreSQL.
    Retorna None si falla la conexión. Retorna dict (posiblemente incompleto) si hay datos.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT indicator, value, date FROM kpis_nacional "
                "WHERE indicator IN ('pib_usd', 'tipo_cambio', 'inflacion', 'pib_mxn')"
            )
            rows = cur.fetchall()
            return {row[0]: {"value": row[1], "date": row[2]} for row in rows}
    except Exception:
        return None


def is_kpis_complete(db_data: dict | None) -> bool:
    """Verifica si todos los KPIs tienen valor no nulo."""
    if not db_data:
        return False
    return all(db_data.get(k, {}).get("value") is not None for k in REQUIRED_INDICATORS)


def seed_kpis_from_other_tables() -> bool:
    """
    Rellena kpis_nacional desde tipo_cambio e inflacion cuando faltan valores.
    Ejecuta siempre que haya datos en esas tablas (no solo cuando kpis_nacional está vacío).
    Retorna True si se escribió algo.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            wrote = False
            # Tipo de cambio: último valor mensual
            cur.execute(
                "SELECT tc_prom_mes, fecha FROM tipo_cambio_banxico_mensual ORDER BY fecha DESC LIMIT 1"
            )
            row_tc = cur.fetchone()
            if row_tc:
                cur.execute(
                    """INSERT INTO kpis_nacional (indicator, value, date, updated_at)
                       VALUES ('tipo_cambio', %s, %s, CURRENT_TIMESTAMP)
                       ON CONFLICT (indicator) DO UPDATE SET value = EXCLUDED.value, date = EXCLUDED.date""",
                    (float(row_tc[0]), str(row_tc[1]) if row_tc[1] else "N/D"),
                )
                wrote = True
            # Inflación: último mes disponible
            cur.execute(
                """SELECT inflacion, texto_fecha FROM inflacion_nacional ORDER BY anio DESC, mes DESC LIMIT 1"""
            )
            row_inf = cur.fetchone()
            if row_inf:
                cur.execute(
                    """INSERT INTO kpis_nacional (indicator, value, date, updated_at)
                       VALUES ('inflacion', %s, %s, CURRENT_TIMESTAMP)
                       ON CONFLICT (indicator) DO UPDATE SET value = EXCLUDED.value, date = EXCLUDED.date""",
                    (float(row_inf[0]) if row_inf[0] else None, row_inf[1] or "N/D"),
                )
                wrote = True
            # Fallback tipo_cambio: si mensual no tenía datos, usar diario
            if not row_tc:
                cur.execute(
                    "SELECT tc, fecha FROM tipo_cambio_banxico_diario ORDER BY fecha DESC LIMIT 1"
                )
                row_diario = cur.fetchone()
                if row_diario:
                    cur.execute(
                        """INSERT INTO kpis_nacional (indicator, value, date, updated_at)
                           VALUES ('tipo_cambio', %s, %s, CURRENT_TIMESTAMP)
                           ON CONFLICT (indicator) DO UPDATE SET value = EXCLUDED.value, date = EXCLUDED.date""",
                        (float(row_diario[0]), str(row_diario[1]) if row_diario[1] else "N/D"),
                    )
                    wrote = True
            return wrote
    except Exception:
        return False


def get_estructura_poblacional_from_db() -> list[dict] | None:
    """
    Obtiene estructura poblacional por edad desde PostgreSQL.
    Retorna [{year, pob_0_14, pob_15_64, pob_65_plus}, ...] o None.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT year, pob_0_14, pob_15_64, pob_65_plus "
                "FROM estructura_poblacional_inegi ORDER BY year"
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {"year": r[0], "pob_0_14": r[1], "pob_15_64": r[2], "pob_65_plus": r[3]}
                for r in rows
            ]
    except Exception:
        return None


def get_crecimiento_poblacional_from_db() -> list[dict] | None:
    """
    Obtiene crecimiento poblacional nacional desde PostgreSQL.
    Retorna lista de {year, value} o None si no hay datos.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT year, value FROM crecimiento_poblacional_nacional ORDER BY year")
            rows = cur.fetchall()
            if not rows:
                return None
            return [{"year": r[0], "value": r[1]} for r in rows]
    except Exception:
        return None


def get_pib_nacional_from_db() -> list[dict] | None:
    """
    Obtiene PIB nacional histórico desde PostgreSQL.
    Retorna [{fecha, anio, trimestre, pib_total_millones, pib_per_capita}, ...] o None.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT fecha, anio, trimestre, pib_total_millones, pib_per_capita "
                "FROM pib_nacional ORDER BY anio, trimestre"
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {
                    "fecha": str(r[0]) if r[0] else f"{r[1]}-{(r[2]-1)*3+1:02d}-01",
                    "anio": r[1],
                    "trimestre": r[2],
                    "pib_total_millones": float(r[3]) if r[3] else 0,
                    "pib_per_capita": float(r[4]) if r[4] else 0,
                }
                for r in rows
            ]
    except Exception:
        return None


def get_pob_sector_actividad_from_db() -> list[dict] | None:
    """
    Obtiene población por sector de actividad desde PostgreSQL.
    Retorna [{sector, valor, pct, es_residual}, ...] o None.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT sector, valor, pct, es_residual FROM pob_sector_actividad ORDER BY sector")
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {"sector": r[0], "valor": r[1], "pct": float(r[2]) if r[2] else 0, "es_residual": bool(r[3]) if r[3] is not None else False}
                for r in rows
            ]
    except Exception:
        return None


def get_pea_from_db() -> list[dict] | None:
    """
    Obtiene PEA (Población Económicamente Activa) desde PostgreSQL.
    Retorna [{fecha_fmt, anio, trimestre, valor}, ...] o None.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT anio, trimestre, valor FROM pea_inegi ORDER BY anio, trimestre")
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {"fecha_fmt": f"{r[0]}-T{r[1]}", "anio": r[0], "trimestre": r[1], "valor": r[2]}
                for r in rows
            ]
    except Exception:
        return None


def get_distribucion_sexo_from_db() -> list[dict] | None:
    """
    Obtiene distribución de población por sexo desde PostgreSQL.
    Retorna [{year, male, female}, ...] o None.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT year, male, female FROM distribucion_sexo_inegi ORDER BY year")
            rows = cur.fetchall()
            if not rows:
                return None
            return [{"year": r[0], "male": r[1], "female": r[2]} for r in rows]
    except Exception:
        return None


def get_tipo_cambio_diario_from_db() -> list[dict] | None:
    """Obtiene tipo de cambio diario desde PostgreSQL. Retorna [{fecha, tc}, ...] o None."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT fecha, tc FROM tipo_cambio_banxico_diario ORDER BY fecha")
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {"fecha": str(r[0]) if r[0] else "", "tc": float(r[1]) if r[1] else 0}
                for r in rows
            ]
    except Exception:
        return None


def get_tipo_cambio_mensual_from_db() -> list[dict] | None:
    """Obtiene tipo de cambio mensual (promedio) desde PostgreSQL. Retorna [{fecha, tc_prom_mes}, ...] o None."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT fecha, tc_prom_mes FROM tipo_cambio_banxico_mensual ORDER BY fecha")
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {"fecha": str(r[0]) if r[0] else "", "tc_prom_mes": float(r[1]) if r[1] else 0}
                for r in rows
            ]
    except Exception:
        return None


def save_tipo_cambio_to_db(diario: list[dict], mensual: list[dict]) -> bool:
    """Guarda tipo de cambio diario y mensual en PostgreSQL."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM tipo_cambio_banxico_diario")
            for r in diario:
                cur.execute(
                    "INSERT INTO tipo_cambio_banxico_diario (fecha, tc, dato) VALUES (%s, %s, %s)",
                    (r.get("fecha"), r.get("tc"), r.get("tc")),
                )
            cur.execute("DELETE FROM tipo_cambio_banxico_mensual")
            for r in mensual:
                cur.execute(
                    "INSERT INTO tipo_cambio_banxico_mensual (fecha, tc_prom_mes) VALUES (%s, %s)",
                    (r.get("fecha"), r.get("tc_prom_mes")),
                )
            return True
    except Exception:
        return False


def get_inflacion_nacional_from_db() -> list[dict] | None:
    """
    Obtiene inflación nacional mensual desde PostgreSQL.
    Retorna [{anio, mes, inflacion, texto_fecha}, ...] o None.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT anio, mes, inflacion, texto_fecha FROM inflacion_nacional ORDER BY anio, mes"
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {
                    "anio": r[0],
                    "mes": r[1],
                    "inflacion": float(r[2]) if r[2] else 0,
                    "texto_fecha": r[3] or "",
                }
                for r in rows
            ]
    except Exception:
        return None


def save_inflacion_nacional_to_db(data: list[dict]) -> bool:
    """Guarda inflación nacional en PostgreSQL. Retorna True si tuvo éxito."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM inflacion_nacional")
            for r in data:
                cur.execute(
                    """INSERT INTO inflacion_nacional (anio, mes, inflacion, texto_fecha)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (anio, mes) DO UPDATE SET
                           inflacion = EXCLUDED.inflacion,
                           texto_fecha = EXCLUDED.texto_fecha""",
                    (r["anio"], r["mes"], r["inflacion"], r.get("texto_fecha", "")),
                )
            return True
    except Exception:
        return False


def save_crecimiento_poblacional_to_db(data: list[dict]) -> bool:
    """Guarda crecimiento poblacional nacional en PostgreSQL."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM crecimiento_poblacional_nacional")
            for r in data:
                cur.execute(
                    "INSERT INTO crecimiento_poblacional_nacional (year, value) VALUES (%s, %s)",
                    (r["year"], r["value"]),
                )
            return True
    except Exception:
        return False


def save_estructura_poblacional_to_db(data: list[dict]) -> bool:
    """Guarda estructura poblacional por edad (INEGI) en PostgreSQL."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM estructura_poblacional_inegi")
            for r in data:
                cur.execute(
                    """INSERT INTO estructura_poblacional_inegi (year, pob_0_14, pob_15_64, pob_65_plus)
                       VALUES (%s, %s, %s, %s)""",
                    (r["year"], r.get("pob_0_14"), r.get("pob_15_64"), r.get("pob_65_plus")),
                )
            return True
    except Exception:
        return False


def save_distribucion_sexo_to_db(data: list[dict]) -> bool:
    """Guarda distribución de población por sexo (INEGI) en PostgreSQL."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM distribucion_sexo_inegi")
            for r in data:
                cur.execute(
                    "INSERT INTO distribucion_sexo_inegi (year, male, female) VALUES (%s, %s, %s)",
                    (r["year"], r.get("male"), r.get("female")),
                )
            return True
    except Exception:
        return False


def save_pea_to_db(data: list[dict]) -> bool:
    """Guarda PEA (Población Económicamente Activa) en PostgreSQL."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM pea_inegi")
            for r in data:
                cur.execute(
                    "INSERT INTO pea_inegi (anio, trimestre, valor) VALUES (%s, %s, %s)",
                    (r["anio"], r["trimestre"], r["valor"]),
                )
            return True
    except Exception:
        return False


def save_pob_sector_actividad_to_db(data: list[dict]) -> bool:
    """Guarda población por sector de actividad en PostgreSQL."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM pob_sector_actividad")
            for r in data:
                cur.execute(
                    """INSERT INTO pob_sector_actividad (sector, valor, pct, es_residual)
                       VALUES (%s, %s, %s, %s)""",
                    (r["sector"], r["valor"], r.get("pct"), r.get("es_residual", False)),
                )
            return True
    except Exception:
        return False


def save_kpis_nacional_to_db(kpis: dict) -> bool:
    """Guarda KPIs nacionales (resumen) en PostgreSQL. kpis: {indicator: {value, date}}."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            for key, data in kpis.items():
                val = data.get("value")
                if val is not None:
                    try:
                        num_val = float(str(val).replace(",", ""))
                    except (ValueError, TypeError):
                        num_val = None
                else:
                    num_val = None
                date_str = data.get("date") or "N/D"
                cur.execute(
                    """INSERT INTO kpis_nacional (indicator, value, date, updated_at)
                       VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                       ON CONFLICT (indicator) DO UPDATE SET
                           value = EXCLUDED.value, date = EXCLUDED.date, updated_at = CURRENT_TIMESTAMP""",
                    (key, num_val, date_str),
                )
            return True
    except Exception:
        return False


def save_actividad_hotelera_nacional_to_db(por_anio: list[dict], por_categoria: list[dict]) -> bool:
    """Guarda actividad hotelera nacional (por año y por categoría) en PostgreSQL."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM actividad_hotelera_nacional")
            for r in por_anio:
                cur.execute(
                    """INSERT INTO actividad_hotelera_nacional
                       (anio, cuartos_disponibles_pd, cuartos_ocupados_pd, porc_ocupacion, updated_at)
                       VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)""",
                    (
                        r["anio"],
                        r.get("cuartos_disponibles_pd"),
                        r.get("cuartos_ocupados_pd"),
                        r.get("porc_ocupacion"),
                    ),
                )
            cur.execute("DELETE FROM actividad_hotelera_nacional_por_categoria")
            for r in por_categoria:
                cur.execute(
                    """INSERT INTO actividad_hotelera_nacional_por_categoria
                       (anio, categoria, cuartos_disponibles_pd, cuartos_ocupados_pd, porc_ocupacion)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (
                        r["anio"],
                        r.get("categoria"),
                        r.get("cuartos_disponibles_pd"),
                        r.get("cuartos_ocupados_pd"),
                        r.get("porc_ocupacion"),
                    ),
                )
            return True
    except Exception:
        return False


def get_proyeccion_pib_from_db() -> dict | None:
    """
    Obtiene proyección PIB (FMI WEO) desde PostgreSQL.
    Retorna {data: [...], tc_fix: float, tc_date: str} o None si no hay datos.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT anio, pib_total_mxn_billones, pib_total_usd_billones, "
                "pib_per_capita_mxn, pib_per_capita_usd FROM pib_proyeccion_fmi ORDER BY anio"
            )
            rows = cur.fetchall()
            if not rows:
                return None
            data = [
                {
                    "anio": r[0],
                    "pib_total_mxn_billones": float(r[1]) if r[1] else 0,
                    "pib_total_usd_billones": float(r[2]) if r[2] else 0,
                    "pib_per_capita_mxn": float(r[3]) if r[3] else 0,
                    "pib_per_capita_usd": float(r[4]) if r[4] else 0,
                }
                for r in rows
            ]
            cur.execute(
                "SELECT value, date FROM kpis_nacional WHERE indicator = 'proyeccion_tc_fix'"
            )
            tc_row = cur.fetchone()
            tc_fix = float(tc_row[0]) if tc_row and tc_row[0] is not None else 20.0
            tc_date = str(tc_row[1]) if tc_row and tc_row[1] else "N/D"
            return {"data": data, "tc_fix": tc_fix, "tc_date": tc_date}
    except Exception:
        return None


def get_ied_flujo_entidad_from_db() -> list[dict] | None:
    """
    Obtiene IED flujo por entidad federativa (últimos 4 trimestres) desde PostgreSQL.
    Retorna [{entidad, mdd_4t, rank, periodo}, ...] o None.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT entidad, mdd_4t, rank, periodo FROM ied_flujo_entidad ORDER BY rank"
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {
                    "entidad": r[0],
                    "mdd_4t": float(r[1]) if r[1] else 0,
                    "rank": r[2] or 0,
                    "periodo": r[3] or "",
                }
                for r in rows
            ]
    except Exception:
        return None


def save_ied_flujo_entidad_to_db(data: list[dict]) -> bool:
    """Guarda IED flujo por entidad en PostgreSQL. Retorna True si tuvo éxito."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM ied_flujo_entidad")
            for r in data:
                cur.execute(
                    "INSERT INTO ied_flujo_entidad (entidad, mdd_4t, rank, periodo) VALUES (%s, %s, %s, %s)",
                    (r.get("entidad", ""), r.get("mdd_4t", 0), r.get("rank", 0), r.get("periodo", "")),
                )
            return True
    except Exception:
        return False


def get_ranking_turismo_wb_from_db() -> list[dict] | None:
    """
    Obtiene ranking turismo mundial desde PostgreSQL.
    Retorna [{iso, country, year, val}, ...] o None.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT iso, country, year, val FROM ranking_turismo_wb ORDER BY year, val DESC"
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {
                    "iso": r[0],
                    "country": r[1],
                    "year": r[2],
                    "val": float(r[3]) if r[3] else 0,
                }
                for r in rows
            ]
    except Exception:
        return None


def save_ranking_turismo_wb_to_db(data: list[dict]) -> bool:
    """Guarda ranking turismo mundial en PostgreSQL. Retorna True si tuvo éxito."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM ranking_turismo_wb")
            for r in data:
                cur.execute(
                    "INSERT INTO ranking_turismo_wb (iso, country, year, val) VALUES (%s, %s, %s, %s)",
                    (r.get("iso", ""), r.get("country", ""), r.get("year", 0), r.get("val", 0)),
                )
            return True
    except Exception:
        return False


def get_balanza_visitantes_from_db() -> list[dict] | None:
    """
    Obtiene Balanza de Visitantes desde PostgreSQL.
    Retorna [{year, entradas, salidas, balance}, ...] o None.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT year, entradas, salidas, balance FROM balanza_visitantes_inegi ORDER BY year"
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {
                    "year": r[0],
                    "entradas": float(r[1]) if r[1] else 0,
                    "salidas": float(r[2]) if r[2] else 0,
                    "balance": float(r[3]) if r[3] else 0,
                }
                for r in rows
            ]
    except Exception:
        return None


def save_balanza_visitantes_to_db(data: list[dict]) -> bool:
    """Guarda Balanza de Visitantes en PostgreSQL. Retorna True si tuvo éxito."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM balanza_visitantes_inegi")
            for r in data:
                cur.execute(
                    "INSERT INTO balanza_visitantes_inegi (year, entradas, salidas, balance) VALUES (%s, %s, %s, %s)",
                    (r.get("year", 0), r.get("entradas", 0), r.get("salidas", 0), r.get("balance", 0)),
                )
            return True
    except Exception:
        return False


def get_balanza_comercial_producto_from_db() -> list[dict] | None:
    """
    Obtiene Balanza Comercial por Producto desde PostgreSQL.
    Retorna [{year, flow_id, trade_value, product}, ...] o None si no hay datos.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT year, flow_id, trade_value, product FROM balanza_comercial_producto ORDER BY year, flow_id, product"
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {
                    "year": int(r[0]),
                    "flow_id": int(r[1]),
                    "trade_value": float(r[2]) if r[2] is not None else 0.0,
                    "product": r[3] or "Total",
                }
                for r in rows
            ]
    except Exception:
        return None


def save_balanza_comercial_producto_to_db(data: list[dict]) -> bool:
    """Guarda Balanza Comercial por Producto en PostgreSQL. Retorna True si tuvo éxito."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM balanza_comercial_producto")
            for r in data:
                cur.execute(
                    "INSERT INTO balanza_comercial_producto (year, flow_id, product, trade_value) VALUES (%s, %s, %s, %s)",
                    (
                        r.get("year"),
                        r.get("flow_id", 1),
                        (r.get("product") or "Total")[:200],
                        r.get("trade_value", 0),
                    ),
                )
            return True
    except Exception:
        return False


def get_anuncios_combinados_from_db() -> list[dict] | None:
    """
    Obtiene Anuncios de Inversión Combinados desde PostgreSQL.
    Retorna [{anio, num_anuncios, monto_inversion, state}, ...] o None.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT anio, num_anuncios, monto_inversion, state "
                "FROM anuncios_inversion_combinados ORDER BY anio, state"
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {
                    "anio": r[0],
                    "num_anuncios": r[1] or 0,
                    "monto_inversion": float(r[2]) if r[2] else 0,
                    "state": r[3] or "",
                }
                for r in rows
            ]
    except Exception:
        return None


def save_anuncios_combinados_to_db(data: list[dict]) -> bool:
    """Guarda Anuncios de Inversión Combinados en PostgreSQL. Retorna True si tuvo éxito."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM anuncios_inversion_combinados")
            for r in data:
                cur.execute(
                    "INSERT INTO anuncios_inversion_combinados (anio, num_anuncios, monto_inversion, state) "
                    "VALUES (%s, %s, %s, %s)",
                    (
                        r.get("anio", 0),
                        r.get("num_anuncios", 0),
                        r.get("monto_inversion", 0),
                        r.get("state", ""),
                    ),
                )
            return True
    except Exception:
        return False


def get_anuncios_base_from_db() -> list[dict] | None:
    """
    Obtiene Anuncios de Inversión Base desde PostgreSQL.
    Retorna [{year, country, state, ia_sector, monto_inversion}, ...] o None.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT year, country, state, ia_sector, monto_inversion "
                "FROM anuncios_inversion_base ORDER BY year, ia_sector"
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {
                    "year": r[0],
                    "country": r[1] or "",
                    "state": r[2] or "",
                    "ia_sector": r[3] or "",
                    "monto_inversion": float(r[4]) if r[4] else 0,
                }
                for r in rows
            ]
    except Exception:
        return None


def save_anuncios_base_to_db(data: list[dict]) -> bool:
    """Guarda Anuncios de Inversión Base en PostgreSQL. Retorna True si tuvo éxito."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM anuncios_inversion_base")
            for r in data:
                cur.execute(
                    "INSERT INTO anuncios_inversion_base (year, country, state, ia_sector, monto_inversion) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (
                        r.get("year", 0),
                        r.get("country", ""),
                        r.get("state", ""),
                        r.get("ia_sector", ""),
                        r.get("monto_inversion", 0),
                    ),
                )
            return True
    except Exception:
        return False


def get_participacion_mercado_from_db() -> list[dict] | None:
    """Obtiene participación mercado aéreo nacional. [{aerolinea, participacion}, ...]"""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT aerolinea, participacion FROM participacion_mercado_aereo ORDER BY participacion DESC")
            rows = cur.fetchall()
            if not rows:
                return None
            return [{"aerolinea": r[0] or "", "participacion": float(r[1]) if r[1] else 0} for r in rows]
    except Exception:
        return None


def save_participacion_mercado_to_db(data: list[dict]) -> bool:
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM participacion_mercado_aereo")
            for r in data:
                cur.execute(
                    "INSERT INTO participacion_mercado_aereo (aerolinea, participacion) VALUES (%s, %s)",
                    (r.get("aerolinea", ""), r.get("participacion", 0)),
                )
            return True
    except Exception:
        return False


def get_producto_aeropuertos_nacional_from_db() -> list[dict] | None:
    """Obtiene operaciones por aeropuerto y año desde PostgreSQL. [{anio, aeropuerto, operaciones}, ...]."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT anio, aeropuerto, operaciones FROM producto_aeropuertos_nacional ORDER BY anio, operaciones DESC"
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {"anio": int(r[0]), "aeropuerto": r[1] or "", "operaciones": int(r[2]) if r[2] is not None else 0}
                for r in rows
            ]
    except Exception:
        return None


def save_producto_aeropuertos_nacional_to_db(data: list[dict]) -> bool:
    """Guarda producto aeropuertos nacional en PostgreSQL. data = [{anio, aeropuerto, operaciones}, ...]."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM producto_aeropuertos_nacional")
            agregados = {}
            for r in data:
                a = r.get("anio")
                aer = (r.get("aeropuerto") or "")[:250].strip()
                op = int(r.get("operaciones", 0) or 0)
                if not a or not aer: continue
                key = (a, aer)
                if key in agregados:
                    agregados[key]["operaciones"] += op
                else:
                    agregados[key] = {"anio": a, "aeropuerto": aer, "operaciones": op}
                    
            for key, val in agregados.items():
                cur.execute(
                    """
                    INSERT INTO producto_aeropuertos_nacional (anio, aeropuerto, operaciones) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT (anio, aeropuerto) 
                    DO UPDATE SET operaciones = EXCLUDED.operaciones
                    """,
                    (val["anio"], val["aeropuerto"], val["operaciones"]),
                )
            return True
    except Exception:
        return False


def get_participacion_internacional_from_db() -> list[dict] | None:
    """Obtiene participación internacional por región. [{region, pasajeros}, ...]"""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT region, pasajeros FROM participacion_internacional_region ORDER BY pasajeros DESC")
            rows = cur.fetchall()
            if not rows:
                return None
            return [{"region": r[0] or "", "pasajeros": float(r[1]) if r[1] else 0} for r in rows]
    except Exception:
        return None


def save_participacion_internacional_to_db(data: list[dict]) -> bool:
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM participacion_internacional_region")
            for r in data:
                cur.execute(
                    "INSERT INTO participacion_internacional_region (region, pasajeros) VALUES (%s, %s)",
                    (r.get("region", ""), r.get("pasajeros", 0)),
                )
            return True
    except Exception:
        return False


def get_ied_paises_from_db() -> list[dict] | None:
    """
    Obtiene IED por país de origen desde PostgreSQL.
    Retorna [{pais, monto_mdd, periodo}, ...] o None.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT pais, monto_mdd, periodo FROM ied_paises ORDER BY monto_mdd DESC"
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {
                    "pais": r[0],
                    "monto_mdd": float(r[1]) if r[1] else 0,
                    "periodo": r[2] or "",
                }
                for r in rows
            ]
    except Exception:
        return None


def save_ied_paises_to_db(data: list[dict]) -> bool:
    """Guarda IED por país en PostgreSQL. Retorna True si tuvo éxito."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM ied_paises")
            for r in data:
                cur.execute(
                    "INSERT INTO ied_paises (pais, monto_mdd, periodo) VALUES (%s, %s, %s)",
                    (r.get("pais", ""), r.get("monto_mdd", 0), r.get("periodo", "")),
                )
            return True
    except Exception:
        return False


def get_ied_sectores_from_db() -> list[dict] | None:
    """
    Obtiene IED por sector económico desde PostgreSQL.
    Retorna [{sector, monto_mdd, periodo}, ...] o None.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT sector, monto_mdd, periodo FROM ied_sectores ORDER BY monto_mdd DESC"
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {
                    "sector": r[0],
                    "monto_mdd": float(r[1]) if r[1] else 0,
                    "periodo": r[2] or "",
                }
                for r in rows
            ]
    except Exception:
        return None


def save_ied_sectores_to_db(data: list[dict]) -> bool:
    """Guarda IED por sector en PostgreSQL. Retorna True si tuvo éxito."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM ied_sectores")
            for r in data:
                cur.execute(
                    "INSERT INTO ied_sectores (sector, monto_mdd, periodo) VALUES (%s, %s, %s)",
                    (r.get("sector", ""), r.get("monto_mdd", 0), r.get("periodo", "")),
                )
            return True
    except Exception:
        return False


def save_proyeccion_pib_to_db(data: list[dict], tc_fix: float, tc_date: str) -> bool:
    """Guarda proyección PIB en PostgreSQL. Retorna True si tuvo éxito."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            for r in data:
                cur.execute(
                    """INSERT INTO pib_proyeccion_fmi
                       (anio, pib_total_mxn_billones, pib_total_usd_billones, pib_per_capita_mxn, pib_per_capita_usd)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (anio) DO UPDATE SET
                           pib_total_mxn_billones = EXCLUDED.pib_total_mxn_billones,
                           pib_total_usd_billones = EXCLUDED.pib_total_usd_billones,
                           pib_per_capita_mxn = EXCLUDED.pib_per_capita_mxn,
                           pib_per_capita_usd = EXCLUDED.pib_per_capita_usd""",
                    (
                        r["anio"],
                        r["pib_total_mxn_billones"],
                        r["pib_total_usd_billones"],
                        r["pib_per_capita_mxn"],
                        r["pib_per_capita_usd"],
                    ),
                )
            cur.execute(
                """
                INSERT INTO kpis_nacional (indicator, value, date, updated_at)
                VALUES ('proyeccion_tc_fix', %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (indicator) DO UPDATE SET
                    value = EXCLUDED.value,
                    date = EXCLUDED.date,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (tc_fix, tc_date),
            )
            return True
    except Exception:
        return False


def get_demografia_estatal_from_db(estado_codigo: str) -> dict | None:
    """
    Obtiene demografía estatal desde PostgreSQL (crecimiento, genero, edad).
    estado_codigo: '01'..'32'. Retorna { crecimiento: [...], genero: [...], edad: [...] } o None si no hay datos.
    """
    if not estado_codigo:
        return None
    codigo = str(estado_codigo).strip().zfill(2)
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            out = {"crecimiento": [], "genero": [], "edad": []}
            cur.execute(
                "SELECT anio, valor, crecimiento_pct FROM demografia_estatal_crecimiento WHERE estado_codigo = %s ORDER BY anio",
                (codigo,),
            )
            for r in cur.fetchall():
                out["crecimiento"].append({"anio": r[0], "valor": r[1] or 0, "crecimiento_pct": float(r[2]) if r[2] is not None else None})
            cur.execute(
                "SELECT anio, hombres, mujeres FROM demografia_estatal_genero WHERE estado_codigo = %s ORDER BY anio",
                (codigo,),
            )
            for r in cur.fetchall():
                out["genero"].append({"anio": r[0], "hombres": r[1] or 0, "mujeres": r[2] or 0})
            cur.execute(
                "SELECT anio, g_0_19, g_20_64, g_65_plus, no_especificado FROM demografia_estatal_edad WHERE estado_codigo = %s ORDER BY anio",
                (codigo,),
            )
            for r in cur.fetchall():
                out["edad"].append({
                    "anio": r[0],
                    "0-19": r[1] or 0,
                    "20-64": r[2] or 0,
                    "65+ años": r[3] or 0,
                    "No especificado": r[4] or 0,
                })
            if out["crecimiento"] or out["genero"] or out["edad"]:
                return out
            return None
    except Exception:
        return None


def save_demografia_estatal_to_db(estado_codigo: str, data: dict) -> bool:
    """Guarda demografía estatal en PostgreSQL. data = { crecimiento: [...], genero: [...], edad: [...] }. Retorna True si ok."""
    if not estado_codigo or not data:
        return False
    codigo = str(estado_codigo).strip().zfill(2)
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM demografia_estatal_crecimiento WHERE estado_codigo = %s", (codigo,))
            for r in data.get("crecimiento") or []:
                cur.execute(
                    "INSERT INTO demografia_estatal_crecimiento (estado_codigo, anio, valor, crecimiento_pct) VALUES (%s, %s, %s, %s)",
                    (codigo, r.get("anio"), r.get("valor"), r.get("crecimiento_pct")),
                )
            cur.execute("DELETE FROM demografia_estatal_genero WHERE estado_codigo = %s", (codigo,))
            for r in data.get("genero") or []:
                cur.execute(
                    "INSERT INTO demografia_estatal_genero (estado_codigo, anio, hombres, mujeres) VALUES (%s, %s, %s, %s)",
                    (codigo, r.get("anio"), r.get("hombres"), r.get("mujeres")),
                )
            cur.execute("DELETE FROM demografia_estatal_edad WHERE estado_codigo = %s", (codigo,))
            for r in data.get("edad") or []:
                cur.execute(
                    """INSERT INTO demografia_estatal_edad (estado_codigo, anio, g_0_19, g_20_64, g_65_plus, no_especificado)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (
                        codigo,
                        r.get("anio"),
                        r.get("0-19"),
                        r.get("20-64"),
                        r.get("65+ años"),
                        r.get("No especificado"),
                    ),
                )
            return True
    except Exception:
        return False


def get_proyecciones_conapo_from_db(estado_codigo: str) -> list[dict] | None:
    """
    Obtiene proyecciones CONAPO desde PostgreSQL (2025-2030).
    estado_codigo: '01'..'32'. Retorna [{anio, total, hombres, mujeres}, ...] o None si no hay datos.
    """
    if not estado_codigo:
        return None
    codigo = str(estado_codigo).strip().zfill(2)
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT anio, total, hombres, mujeres FROM proyecciones_conapo WHERE estado_codigo = %s ORDER BY anio",
                (codigo,),
            )
            rows = cur.fetchall()
            if not rows:
                return None
            return [
                {"anio": r[0], "total": r[1] or 0, "hombres": r[2] or 0, "mujeres": r[3] or 0}
                for r in rows
            ]
    except Exception:
        return None


def save_proyecciones_conapo_to_db(estado_codigo: str, data: list[dict]) -> bool:
    """Guarda proyecciones CONAPO en PostgreSQL. data = [{anio, total, hombres, mujeres}, ...]. Retorna True si ok."""
    if not estado_codigo or not data:
        return False
    codigo = str(estado_codigo).strip().zfill(2)
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM proyecciones_conapo WHERE estado_codigo = %s", (codigo,))
            for r in data:
                cur.execute(
                    "INSERT INTO proyecciones_conapo (estado_codigo, anio, total, hombres, mujeres) VALUES (%s, %s, %s, %s, %s)",
                    (codigo, r.get("anio"), r.get("total"), r.get("hombres"), r.get("mujeres")),
                )
            return True
    except Exception:
        return False


def get_itaee_estatal_from_db(estado_codigo: str) -> dict | None:
    """
    Obtiene ITAEE estatal desde PostgreSQL (último año disponible).
    estado_codigo: '01'..'32'. Retorna {anio, primario, secundario, terciario, total} o None si no hay datos.
    """
    if not estado_codigo:
        return None
    codigo = str(estado_codigo).strip().zfill(2)
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT anio, sector, valor FROM itaee_estatal WHERE estado_codigo = %s ORDER BY anio DESC, sector",
                (codigo,),
            )
            rows = cur.fetchall()
            if not rows:
                return None
            by_year = {}
            for r in rows:
                anio = str(r[0])
                sector = str(r[1])
                valor = float(r[2]) if r[2] else 0.0
                if anio not in by_year:
                    by_year[anio] = {}
                by_year[anio][sector] = valor
            if not by_year:
                return None
            # Buscar el año más reciente que tenga un Total mayor a cero
            for anio in sorted(by_year.keys(), reverse=True):
                datos = by_year[anio]
                prim_val = datos.get("Primario") or datos.get("Primarias") or datos.get("Actividades Primarias") or 0.0
                sec_val = datos.get("Secundario") or datos.get("Secundarias") or datos.get("Actividades Secundarias") or 0.0
                terc_val = datos.get("Terciario") or datos.get("Terciarias") or datos.get("Actividades Terciarias") or 0.0
                total_val = datos.get("Total") or datos.get("Total de la economía") or (prim_val + sec_val + terc_val)
                
                if total_val > 0:
                    return {
                        "anio": anio,
                        "primario": prim_val,
                        "secundario": sec_val,
                        "terciario": terc_val,
                        "total": total_val
                    }
            return None
    except Exception:
        return None


def get_itaee_estatal_timeline_from_db(estado_codigo: str) -> list[dict]:
    """
    Obtiene el histórico del ITAEE estatal desde PostgreSQL.
    Retorna lista de {anio, primario, secundario, terciario, total} ordenada por año.
    """
    if not estado_codigo:
        return []
    codigo = str(estado_codigo).strip().zfill(2)
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT anio, sector, valor FROM itaee_estatal WHERE estado_codigo = %s ORDER BY anio ASC, sector",
                (codigo,),
            )
            rows = cur.fetchall()
            if not rows:
                return []
            
            by_year = {}
            for r in rows:
                anio = str(r[0])
                sector = str(r[1])
                valor = float(r[2]) if r[2] else 0.0
                if anio not in by_year:
                    by_year[anio] = {"anio": anio, "primario": 0.0, "secundario": 0.0, "terciario": 0.0, "total": 0.0}
                
                # Normalizar sectores
                s_key = sector.lower()
                if "primari" in s_key: by_year[anio]["primario"] = valor
                elif "secundari" in s_key: by_year[anio]["secundario"] = valor
                elif "terciari" in s_key: by_year[anio]["terciario"] = valor
                elif "total" in s_key: by_year[anio]["total"] = valor

            # Calcular totales si faltan
            for anio in by_year:
                if by_year[anio]["total"] == 0:
                    by_year[anio]["total"] = by_year[anio]["primario"] + by_year[anio]["secundario"] + by_year[anio]["terciario"]

            return sorted(by_year.values(), key=lambda x: x["anio"])
    except Exception:
        return []


def save_itaee_estatal_to_db(estado_codigo: str, data: dict) -> bool:
    """Guarda ITAEE estatal en PostgreSQL. data = {anio, primario, secundario, terciario, total}. Retorna True si ok."""
    if not estado_codigo or not data:
        return False
    codigo = str(estado_codigo).strip().zfill(2)
    anio = str(data.get("anio", ""))
    if not anio:
        return False
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM itaee_estatal WHERE estado_codigo = %s AND anio = %s", (codigo, anio))
            for sector in ["Primario", "Secundario", "Terciario"]:
                valor = data.get(sector.lower(), 0.0)
                cur.execute(
                    "INSERT INTO itaee_estatal (estado_codigo, anio, sector, valor) VALUES (%s, %s, %s, %s)",
                    (codigo, anio, sector, valor),
                )
            return True
    except Exception:
        return False


def get_actividad_hotelera_estatal_from_db(
    estado_codigo: str, anio: int | None = None
) -> tuple[dict | None, list[int]]:
    """
    Obtiene actividad hotelera por estado (y opcionalmente año) desde PostgreSQL.
    estado_codigo: '01'..'32'. anio: si no se pasa, se usa el año más reciente disponible.
    Retorna (datos, lista_de_años). datos = {meses, disponibles, ocupados, porc_ocupacion} o None.
    """
    if not estado_codigo:
        return None, []
    codigo = str(estado_codigo).strip().zfill(2)
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT anio FROM actividad_hotelera_estatal WHERE estado_codigo = %s ORDER BY anio DESC",
                (codigo,),
            )
            years = [r[0] for r in cur.fetchall() if r[0] is not None]
            if not years:
                return None, []
            year_to_use = anio if anio is not None and anio in years else years[0]
            cur.execute(
                """SELECT mes_num, disponibles, ocupados, porc_ocupacion
                   FROM actividad_hotelera_estatal WHERE estado_codigo = %s AND anio = %s ORDER BY mes_num""",
                (codigo, year_to_use),
            )
            rows = cur.fetchall()
            if not rows or len(rows) < 12:
                return None, years
            meses = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
            import math
            disponibles = [float(r[1]) if r[1] is not None and not math.isnan(float(r[1])) else 0 for r in rows]
            ocupados = [float(r[2]) if r[2] is not None and not math.isnan(float(r[2])) else 0 for r in rows]
            porc_ocupacion = [float(r[3]) if r[3] is not None and not math.isnan(float(r[3])) else 0 for r in rows]
            data = {"meses": meses, "disponibles": disponibles, "ocupados": ocupados, "porc_ocupacion": porc_ocupacion}
            return data, years
    except Exception:
        return None, []


def get_exportaciones_estatal_from_db(slug: str = None) -> list[dict]:
    """
    Obtiene las exportaciones por estado desde PostgreSQL.
    Si se pasa slug, filtra por ese estado.
    Retorna [{year, state_slug, estado_codigo, trade_value}, ...].
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            if slug:
                cur.execute(
                    """SELECT estado_codigo, estado_slug, anio, trade_value
                       FROM exportaciones_estatal WHERE estado_slug = %s ORDER BY anio ASC""", (slug,)
                )
            else:
                cur.execute(
                    """SELECT estado_codigo, estado_slug, anio, trade_value
                       FROM exportaciones_estatal ORDER BY estado_codigo, anio"""
                )
            rows = cur.fetchall()
            return [
                {
                    "year": r[2],
                    "state_slug": r[1],
                    "estado_codigo": r[0],
                    "trade_value": float(r[3]) if r[3] is not None else 0,
                }
                for r in rows
            ]
    except Exception:
        return []

def get_exportaciones_estatal_ranking_from_db() -> list[dict]:
    """Obtiene el ranking de exportaciones por estado del último año."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT MAX(anio) FROM exportaciones_estatal")
            latest_year = cur.fetchone()[0]
            if not latest_year:
                return []
            
            cur.execute("""
                SELECT estado_slug, estado_codigo, trade_value 
                FROM exportaciones_estatal 
                WHERE anio = %s 
                ORDER BY trade_value DESC
            """, (latest_year,))
            
            rows = cur.fetchall()
            return [
                {
                    "state_slug": r[0],
                    "estado_codigo": r[1],
                    "trade_value": float(r[2]) if r[2] is not None else 0,
                    "year": latest_year
                } for r in rows
            ]
    except Exception:
        return []


def get_mapa_carretero_from_db(estado_nombre: str) -> bytes | None:
    """Obtiene la imagen del mapa carretero desde PostgreSQL."""
    if not estado_nombre:
        return None
    try:
        from services.data_sources import ESTADO_NOMBRE_TO_CODIGO
        codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado_nombre)
        if not codigo:
            # Reintentar con coincidencia parcial
            for k, v in ESTADO_NOMBRE_TO_CODIGO.items():
                if estado_nombre.lower() in k.lower() or k.lower() in estado_nombre.lower():
                    codigo = v
                    break
        
        if not codigo:
            return None

        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT imagen FROM mapas_carreteros WHERE estado_codigo = %s", (codigo,))
            row = cur.fetchone()
            if row:
                return bytes(row[0])
            return None
    except Exception:
        return None


def save_exportaciones_estatal_to_db(data: list[dict]) -> bool:
    """
    Guarda exportaciones por estado en PostgreSQL.
    data = [{year, state_slug, estado_codigo, trade_value}, ...].
    Retorna True si ok.
    """
    if not data:
        return False
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            # Limpiar datos existentes antes de insertar nuevos
            cur.execute("DELETE FROM exportaciones_estatal")
            for row in data:
                codigo = str(row.get("estado_codigo", "")).strip().zfill(2)
                slug = str(row.get("state_slug", "")).strip()
                anio = int(row.get("year", 0))
                valor = float(row.get("trade_value", 0))
                if codigo and slug and anio > 0:
                    cur.execute(
                        """INSERT INTO exportaciones_estatal (estado_codigo, estado_slug, anio, trade_value)
                           VALUES (%s, %s, %s, %s)
                           ON CONFLICT (estado_codigo, anio) DO UPDATE SET
                           estado_slug = EXCLUDED.estado_slug,
                           trade_value = EXCLUDED.trade_value""",
                        (codigo, slug, anio, valor),
                    )
            conn.commit()
            return True
    except Exception as e:
        print(f"Error guardando exportaciones: {e}")
        return False


def save_actividad_hotelera_estatal_to_db(estado_codigo: str, data: dict, anio: int = 2024) -> bool:
    """Guarda actividad hotelera estatal para un año. data = {meses, disponibles, ocupados, porc_ocupacion}. Retorna True si ok."""
    if not estado_codigo or not data:
        return False
    codigo = str(estado_codigo).strip().zfill(2)
    disp = data.get("disponibles") or []
    ocup = data.get("ocupados") or []
    porc = data.get("porc_ocupacion") or []
    if len(disp) < 12 or len(ocup) < 12 or len(porc) < 12:
        return False
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM actividad_hotelera_estatal WHERE estado_codigo = %s AND anio = %s", (codigo, anio))
            import math
            for mes in range(12):
                v_disp = disp[mes] if not (isinstance(disp[mes], float) and math.isnan(disp[mes])) else 0
                v_ocup = ocup[mes] if not (isinstance(ocup[mes], float) and math.isnan(ocup[mes])) else 0
                v_porc = porc[mes] if not (isinstance(porc[mes], float) and math.isnan(porc[mes])) else 0
                
                cur.execute(
                    """INSERT INTO actividad_hotelera_estatal (estado_codigo, anio, mes_num, disponibles, ocupados, porc_ocupacion)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (codigo, anio, mes + 1, v_disp, v_ocup, v_porc),
                )
            return True
    except Exception:
        return False


def save_llegada_turistas_estatal_to_db(estado_codigo: str, anio: int, total: int) -> bool:
    """Guarda o actualiza llegada de turistas para un estado y año."""
    try:
        codigo = str(estado_codigo).strip().zfill(2)
        with db_connection() as conn:
            cur = conn.cursor()
            # Asegurar que la tabla existe
            cur.execute("""
                CREATE TABLE IF NOT EXISTS llegada_turistas_estatal (
                    id SERIAL PRIMARY KEY,
                    estado_codigo CHAR(2) NOT NULL,
                    anio INTEGER NOT NULL,
                    turistas_total BIGINT NOT NULL,
                    UNIQUE(estado_codigo, anio)
                )
            """)
            cur.execute(
                """INSERT INTO llegada_turistas_estatal (estado_codigo, anio, turistas_total)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (estado_codigo, anio) DO UPDATE SET turistas_total = EXCLUDED.turistas_total""",
                (codigo, anio, total)
            )
            return True
    except Exception:
        return False


def get_aeropuertos_estatal_from_db(estado_codigo: str) -> list[dict]:
    """
    Obtiene operaciones aeroportuarias por estado desde PostgreSQL.
    estado_codigo: '01'..'32'.
    Retorna [{aeropuerto, grupo, anio, operaciones}, ...] ordenado por año descendente, luego aeropuerto.
    """
    if not estado_codigo:
        return []
    codigo = str(estado_codigo).strip().zfill(2)
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT aeropuerto, grupo, anio, operaciones
                   FROM aeropuertos_estatal WHERE estado_codigo = %s ORDER BY anio DESC, aeropuerto""",
                (codigo,),
            )
            rows = cur.fetchall()
            return [
                {
                    "aeropuerto": r[0],
                    "grupo": r[1] or "",
                    "anio": r[2],
                    "operaciones": int(r[3]) if r[3] is not None else 0,
                }
                for r in rows
            ]
    except Exception:
        return []


def get_municipios_from_db(estado_nombre: str) -> list[dict]:
    """
    Obtiene lista de municipios para un estado desde PostgreSQL.
    Retorna [{nombre, codigo, estado_codigo}, ...] ordenado por nombre.
    """
    try:
        import unicodedata
        import traceback
        
        # Normalizar nombre del estado para búsqueda
        def normalize_str(s: str) -> str:
            if not s:
                return ""
            return "".join(
                c for c in unicodedata.normalize("NFD", str(s).lower().strip())
                if unicodedata.category(c) != "Mn"
            )
        
        with db_connection() as conn:
            cur = conn.cursor()
            
            # Primero verificar si la tabla existe
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'municipios'
                )
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                print("Advertencia: La tabla 'municipios' no existe en la base de datos")
                return []
            
            # Buscar por estado_nombre directamente (comparación case-insensitive)
            cur.execute(
                """SELECT DISTINCT municipio_nombre, municipio_codigo, estado_codigo, estado_nombre
                   FROM municipios
                   WHERE estado_nombre ILIKE %s
                   ORDER BY municipio_nombre ASC""",
                (estado_nombre,)
            )
            rows = cur.fetchall()
            
            # Si no hay resultados exactos, intentar búsqueda más flexible
            if not rows:
                estado_norm = normalize_str(estado_nombre)
                cur.execute(
                    """SELECT DISTINCT municipio_nombre, municipio_codigo, estado_codigo, estado_nombre
                       FROM municipios
                       WHERE municipio_nombre_normalizado LIKE %s
                       ORDER BY municipio_nombre ASC""",
                    (f"%{estado_norm}%",)
                )
                rows = cur.fetchall()
            
            result = [
                {
                    "nombre": row[0],
                    "codigo": row[1],
                    "estado_codigo": row[2],
                    "estado_nombre": row[3],
                }
                for row in rows
            ]
            
            print(f"Municipios encontrados para '{estado_nombre}': {len(result)}")
            return result
    except Exception as e:
        import traceback
        print(f"Error obteniendo municipios: {e}")
        print(traceback.format_exc())
        return []


def save_municipios_to_db(municipios: list[dict]) -> bool:
    """
    Guarda lista de municipios en PostgreSQL.
    municipios = [{nombre, codigo, estado_codigo, estado_nombre}, ...]
    Retorna True si ok.
    """
    if not municipios:
        return False
    try:
        import unicodedata
        
        def normalize_str(s: str) -> str:
            if not s:
                return ""
            return "".join(
                c for c in unicodedata.normalize("NFD", str(s).lower().strip())
                if unicodedata.category(c) != "Mn"
            )
        
        with db_connection() as conn:
            cur = conn.cursor()
            for muni in municipios:
                nombre = str(muni.get("nombre", "")).strip()
                codigo = str(muni.get("codigo", "")).strip().zfill(3)
                estado_codigo = str(muni.get("estado_codigo", "")).strip().zfill(2)
                estado_nombre = str(muni.get("estado_nombre", "")).strip()
                nombre_norm = normalize_str(nombre)
                
                if nombre and codigo and estado_codigo:
                    cur.execute(
                        """INSERT INTO municipios (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, municipio_nombre_normalizado)
                           VALUES (%s, %s, %s, %s, %s)
                           ON CONFLICT (estado_codigo, municipio_codigo) DO UPDATE SET
                           estado_nombre = EXCLUDED.estado_nombre,
                           municipio_nombre = EXCLUDED.municipio_nombre,
                           municipio_nombre_normalizado = EXCLUDED.municipio_nombre_normalizado""",
                        (estado_codigo, estado_nombre, codigo, nombre, nombre_norm),
                    )
            conn.commit()
            return True
    except Exception as e:
        print(f"Error guardando municipios: {e}")
        import traceback
        traceback.print_exc()
        return False


def save_distribucion_poblacion_municipal_to_db(data: dict) -> bool:
    """
    Guarda datos de distribución de población municipal en PostgreSQL.
    data debe contener estado_codigo, municipio_codigo, POBTOT, POBFEM, POBMAS
    y opcionalmente campos de grupos de edad (P_0A4_F, P_0A4_M, etc.).
    Retorna True si ok.
    """
    if not data:
        return False
    try:
        import json
        
        estado_codigo = str(data.get("estado_codigo", "")).strip().zfill(2)
        municipio_codigo = str(data.get("municipio_codigo", "")).strip().zfill(3)
        pobtot = int(data.get("POBTOT", 0) or 0)
        pobfem = int(data.get("POBFEM", 0) or 0)
        pobmas = int(data.get("POBMAS", 0) or 0)
        
        if not estado_codigo or not municipio_codigo:
            return False
        
        # Extraer campos básicos y datos adicionales (grupos de edad)
        campos_basicos = ["estado_codigo", "municipio_codigo", "POBTOT", "POBFEM", "POBMAS"]
        data_adicional = {k: v for k, v in data.items() if k not in campos_basicos}
        data_json_str = json.dumps(data_adicional) if data_adicional else None
        
        # Obtener nombres de estado y municipio si están disponibles
        estado_nombre = data.get("estado_nombre", "")
        municipio_nombre = data.get("municipio_nombre", "")
        
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO distribucion_poblacion_municipal 
                   (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, pobtot, pobfem, pobmas, data_json)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (estado_codigo, municipio_codigo) DO UPDATE SET
                   estado_nombre = EXCLUDED.estado_nombre,
                   municipio_nombre = EXCLUDED.municipio_nombre,
                   pobtot = EXCLUDED.pobtot,
                   pobfem = EXCLUDED.pobfem,
                   pobmas = EXCLUDED.pobmas,
                   data_json = EXCLUDED.data_json""",
                (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, pobtot, pobfem, pobmas, data_json_str),
            )
            conn.commit()
            return True
    except Exception as e:
        print(f"Error guardando distribución población municipal: {e}")
        import traceback
        traceback.print_exc()
        return False


def save_distribucion_poblacion_municipal_bulk(lista_data: list[dict]) -> int:
    """
    Guarda en lote los registros de distribución poblacional municipal.
    Retorna el número de registros guardados correctamente.
    """
    if not lista_data:
        return 0
    import json
    saved = 0
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            for data in lista_data:
                estado_codigo = str(data.get("estado_codigo", "")).strip().zfill(2)
                municipio_codigo = str(data.get("municipio_codigo", "")).strip().zfill(3)
                if not estado_codigo or not municipio_codigo:
                    continue
                pobtot = int(data.get("POBTOT", 0) or 0)
                pobfem = int(data.get("POBFEM", 0) or 0)
                pobmas = int(data.get("POBMAS", 0) or 0)
                campos_basicos = ["estado_codigo", "municipio_codigo", "POBTOT", "POBFEM", "POBMAS"]
                data_adicional = {k: v for k, v in data.items() if k not in campos_basicos}
                data_json_str = json.dumps(data_adicional) if data_adicional else None
                estado_nombre = data.get("estado_nombre", "")
                municipio_nombre = data.get("municipio_nombre", "")
                cur.execute(
                    """INSERT INTO distribucion_poblacion_municipal 
                       (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, pobtot, pobfem, pobmas, data_json)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (estado_codigo, municipio_codigo) DO UPDATE SET
                       estado_nombre = EXCLUDED.estado_nombre,
                       municipio_nombre = EXCLUDED.municipio_nombre,
                       pobtot = EXCLUDED.pobtot,
                       pobfem = EXCLUDED.pobfem,
                       pobmas = EXCLUDED.pobmas,
                       data_json = EXCLUDED.data_json""",
                    (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, pobtot, pobfem, pobmas, data_json_str),
                )
                saved += 1
            conn.commit()
        return saved
    except Exception as e:
        print(f"Error guardando distribución población municipal (bulk): {e}")
        import traceback
        traceback.print_exc()
        return saved


def get_distribucion_poblacion_municipal_from_db(estado_nombre: str, municipio_nombre: str) -> dict | None:
    """
    Obtiene distribución de población por municipio desde PostgreSQL.
    Retorna dict con datos de población por edad y sexo o None si no hay datos.
    """
    try:
        import unicodedata
        
        def normalize_str(s: str) -> str:
            if not s:
                return ""
            return "".join(
                c for c in unicodedata.normalize("NFD", str(s).lower().strip())
                if unicodedata.category(c) != "Mn"
            )
        
        municipio_norm = normalize_str(municipio_nombre)
        
        with db_connection() as conn:
            cur = conn.cursor()
            # Verificar si existe la tabla municipios para hacer JOIN
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'municipios'
                )
            """)
            tiene_tabla_municipios = cur.fetchone()[0]
            
            if tiene_tabla_municipios:
                # Buscar usando JOIN con tabla municipios
                cur.execute(
                    """SELECT dpm.estado_codigo, dpm.municipio_codigo, dpm.pobtot, dpm.pobfem, dpm.pobmas, dpm.data_json
                       FROM distribucion_poblacion_municipal dpm
                       JOIN municipios m ON dpm.estado_codigo = m.estado_codigo 
                                          AND dpm.municipio_codigo = m.municipio_codigo
                       WHERE m.municipio_nombre_normalizado LIKE %s 
                         AND (m.estado_nombre = %s OR m.estado_nombre ILIKE %s)
                       LIMIT 1""",
                    (f"%{municipio_norm}%", estado_nombre, f"%{estado_nombre}%")
                )
            else:
                # Buscar directamente en distribucion_poblacion_municipal
                cur.execute(
                    """SELECT estado_codigo, municipio_codigo, pobtot, pobfem, pobmas, data_json
                       FROM distribucion_poblacion_municipal
                       WHERE municipio_nombre ILIKE %s 
                         AND (estado_nombre = %s OR estado_nombre ILIKE %s)
                       LIMIT 1""",
                    (f"%{municipio_nombre}%", estado_nombre, f"%{estado_nombre}%")
                )
            
            row = cur.fetchone()
            
            if not row:
                return None
            
            # Construir diccionario con campos básicos
            data = {
                "estado_codigo": row[0],
                "municipio_codigo": row[1],
                "POBTOT": int(row[2]) if row[2] else 0,
                "POBFEM": int(row[3]) if row[3] else 0,
                "POBMAS": int(row[4]) if row[4] else 0,
            }
            
            # Cargar datos adicionales desde JSON si existe
            if row[5]:  # data_json
                import json
                try:
                    data_json = json.loads(row[5])
                    if isinstance(data_json, dict):
                        data.update(data_json)
                except Exception:
                    pass
            
            return data
    except Exception as e:
        print(f"Error obteniendo distribución población municipal: {e}")
        return None


def get_proyeccion_poblacional_municipal_from_db(estado_nombre: str, municipio_nombre: str) -> list[dict]:
    """
    Obtiene proyección poblacional por municipio desde PostgreSQL.
    Retorna [{anio, sexo, pob}, ...] ordenado por año y sexo.
    """
    try:
        import unicodedata
        
        def normalize_str(s: str) -> str:
            if not s:
                return ""
            return "".join(
                c for c in unicodedata.normalize("NFD", str(s).lower().strip())
                if unicodedata.category(c) != "Mn"
            )
        
        municipio_norm = normalize_str(municipio_nombre)
        
        with db_connection() as conn:
            cur = conn.cursor()
            # Verificar si existe tabla municipios
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'municipios'
                )
            """)
            tiene_tabla_municipios = cur.fetchone()[0]
            
            if tiene_tabla_municipios:
                # Buscar con JOIN a municipios
                cur.execute(
                    """SELECT ppm.anio, ppm.sexo, ppm.poblacion
                       FROM proyeccion_poblacional_municipal ppm
                       JOIN municipios m ON ppm.estado_codigo = m.estado_codigo 
                                          AND ppm.municipio_codigo = m.municipio_codigo
                       WHERE m.municipio_nombre_normalizado LIKE %s 
                         AND (m.estado_nombre = %s OR m.estado_nombre ILIKE %s)
                       ORDER BY ppm.anio ASC, ppm.sexo ASC""",
                    (f"%{municipio_norm}%", estado_nombre, f"%{estado_nombre}%")
                )
            else:
                # Buscar directamente en proyeccion_poblacional_municipal
                cur.execute(
                    """SELECT anio, sexo, poblacion
                       FROM proyeccion_poblacional_municipal
                       WHERE municipio_nombre ILIKE %s 
                         AND (estado_nombre = %s OR estado_nombre ILIKE %s)
                       ORDER BY anio ASC, sexo ASC""",
                    (f"%{municipio_nombre}%", estado_nombre, f"%{estado_nombre}%")
                )
            rows = cur.fetchall()
            
            return [
                {
                    "anio": int(row[0]),
                    "sexo": str(row[1]),
                    "pob": int(row[2]) if row[2] else 0,
                    "poblacion": int(row[2]) if row[2] else 0,
                }
                for row in rows
            ]
    except Exception as e:
        print(f"Error obteniendo proyección poblacional municipal: {e}")
        return []


def save_proyeccion_poblacional_municipal_to_db(data: list[dict]) -> bool:
    """
    Guarda datos de proyección poblacional municipal en PostgreSQL.
    data = [{estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, anio, sexo, poblacion}, ...]
    Retorna True si ok.
    """
    if not data:
        return False
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            for record in data:
                estado_codigo = str(record.get("estado_codigo", "")).strip().zfill(2)
                municipio_codigo = str(record.get("municipio_codigo", "")).strip().zfill(3)
                estado_nombre = str(record.get("estado_nombre", "")).strip()
                municipio_nombre = str(record.get("municipio_nombre", "")).strip()
                anio = int(record.get("anio", 0))
                sexo = str(record.get("sexo", "")).strip().upper()
                poblacion = int(record.get("poblacion", 0) or 0)
                
                if not estado_codigo or not municipio_codigo or not anio or not sexo:
                    continue
                
                cur.execute(
                    """INSERT INTO proyeccion_poblacional_municipal 
                       (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, anio, sexo, poblacion)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (estado_codigo, municipio_codigo, anio, sexo) DO UPDATE SET
                       estado_nombre = EXCLUDED.estado_nombre,
                       municipio_nombre = EXCLUDED.municipio_nombre,
                       poblacion = EXCLUDED.poblacion""",
                    (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, anio, sexo, poblacion),
                )
            conn.commit()
            return True
    except Exception as e:
        print(f"Error guardando proyección poblacional municipal: {e}")
        import traceback
        traceback.print_exc()
        return False


# ——— Localidades (INEGI Censo 2020) ———

def get_localidades_from_db(estado_nombre: str, municipio_nombre: str | None = None) -> list[dict]:
    """
    Obtiene lista de localidades desde PostgreSQL.
    Si municipio_nombre es None, retorna localidades de todo el estado.
    Retorna [{nombre, codigo, estado_codigo, estado_nombre, municipio_codigo, municipio_nombre}, ...]
    """
    try:
        import unicodedata
        def normalize_str(s: str) -> str:
            if not s:
                return ""
            return "".join(c for c in unicodedata.normalize("NFD", str(s).lower().strip()) if unicodedata.category(c) != "Mn")
        estado_norm = normalize_str(estado_nombre)
        municipio_norm = normalize_str(municipio_nombre) if municipio_nombre else None
        with db_connection() as conn:
            cur = conn.cursor()
            if municipio_nombre:
                cur.execute(
                    """SELECT localidad_nombre, loc_codigo, estado_codigo, estado_nombre, municipio_codigo, municipio_nombre
                       FROM localidades
                       WHERE estado_nombre ILIKE %s AND municipio_nombre ILIKE %s
                       ORDER BY localidad_nombre""",
                    (f"%{estado_nombre}%", f"%{municipio_nombre}%")
                )
            else:
                cur.execute(
                    """SELECT localidad_nombre, loc_codigo, estado_codigo, estado_nombre, municipio_codigo, municipio_nombre
                       FROM localidades
                       WHERE estado_nombre ILIKE %s
                       ORDER BY municipio_nombre, localidad_nombre""",
                    (f"%{estado_nombre}%",)
                )
            rows = cur.fetchall()
            return [
                {
                    "nombre": str(row[0]) or "",
                    "codigo": str(row[1]) or "",
                    "estado_codigo": str(row[2]) or "",
                    "estado_nombre": str(row[3]) or "",
                    "municipio_codigo": str(row[4]) or "",
                    "municipio_nombre": str(row[5]) or "",
                }
                for row in rows
            ]
    except Exception as e:
        print(f"Error obteniendo localidades: {e}")
        return []


def save_localidades_to_db(lista: list[dict]) -> bool:
    """Guarda lista de localidades en PostgreSQL."""
    if not lista:
        return False
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            for r in lista:
                cur.execute(
                    """INSERT INTO localidades (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (estado_codigo, municipio_codigo, loc_codigo) DO UPDATE SET
                       estado_nombre = EXCLUDED.estado_nombre,
                       municipio_nombre = EXCLUDED.municipio_nombre,
                       localidad_nombre = EXCLUDED.localidad_nombre""",
                    (
                        str(r.get("estado_codigo", "")).strip().zfill(2),
                        str(r.get("estado_nombre", "")).strip(),
                        str(r.get("municipio_codigo", "")).strip().zfill(3),
                        str(r.get("municipio_nombre", "")).strip(),
                        str(r.get("loc_codigo", "")).strip(),
                        str(r.get("localidad_nombre", "")).strip(),
                    ),
                )
            conn.commit()
            return True
    except Exception as e:
        print(f"Error guardando localidades: {e}")
        return False


def get_distribucion_poblacion_localidad_from_db(estado_nombre: str, municipio_nombre: str, localidad_nombre: str) -> dict | None:
    """Obtiene distribución de población por localidad desde PostgreSQL."""
    try:
        import json
        import unicodedata
        def normalize_str(s: str) -> str:
            if not s:
                return ""
            return "".join(c for c in unicodedata.normalize("NFD", str(s).lower().strip()) if unicodedata.category(c) != "Mn")
        loc_norm = normalize_str(localidad_nombre)
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT estado_codigo, municipio_codigo, loc_codigo, estado_nombre, municipio_nombre, localidad_nombre, pobtot, pobfem, pobmas, data_json
                   FROM distribucion_poblacion_localidad
                   WHERE estado_nombre ILIKE %s AND municipio_nombre ILIKE %s
                     AND (localidad_nombre ILIKE %s OR localidad_nombre ILIKE %s)
                   LIMIT 1""",
                (f"%{estado_nombre}%", f"%{municipio_nombre}%", f"%{localidad_nombre}%", f"%{loc_norm}%")
            )
            row = cur.fetchone()
            if not row:
                return None
            data = {
                "estado_codigo": row[0], "municipio_codigo": row[1], "loc_codigo": row[2],
                "estado_nombre": row[3], "municipio_nombre": row[4], "localidad_nombre": row[5],
                "POBTOT": int(row[6]) if row[6] else 0,
                "POBFEM": int(row[7]) if row[7] else 0,
                "POBMAS": int(row[8]) if row[8] else 0,
            }
            if row[9]:
                try:
                    data.update(json.loads(row[9]))
                except Exception:
                    pass
            return data
    except Exception as e:
        print(f"Error obteniendo distribución localidad: {e}")
        return None


def save_distribucion_poblacion_localidad_to_db(data: dict) -> bool:
    """Guarda distribución de población de una localidad en PostgreSQL."""
    if not data:
        return False
    try:
        import json
        ec = str(data.get("estado_codigo", "")).strip().zfill(2)
        mc = str(data.get("municipio_codigo", "")).strip().zfill(3)
        lc = str(data.get("loc_codigo", "")).strip()
        if not ec or not mc or not lc:
            return False
        pt = int(data.get("POBTOT", 0) or 0)
        pf = int(data.get("POBFEM", 0) or 0)
        pm = int(data.get("POBMAS", 0) or 0)
        campos = ["estado_codigo", "municipio_codigo", "loc_codigo", "estado_nombre", "municipio_nombre", "localidad_nombre", "POBTOT", "POBFEM", "POBMAS"]
        extra = {k: v for k, v in data.items() if k not in campos}
        js = json.dumps(extra) if extra else None
        en = str(data.get("estado_nombre", "")).strip()
        mn = str(data.get("municipio_nombre", "")).strip()
        ln = str(data.get("localidad_nombre", "")).strip()
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO distribucion_poblacion_localidad
                   (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre, pobtot, pobfem, pobmas, data_json)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (estado_codigo, municipio_codigo, loc_codigo) DO UPDATE SET
                   estado_nombre = EXCLUDED.estado_nombre,
                   municipio_nombre = EXCLUDED.municipio_nombre,
                   localidad_nombre = EXCLUDED.localidad_nombre,
                   pobtot = EXCLUDED.pobtot,
                   pobfem = EXCLUDED.pobfem,
                   pobmas = EXCLUDED.pobmas,
                   data_json = EXCLUDED.data_json""",
                (ec, en, mc, mn, lc, ln, pt, pf, pm, js),
            )
            conn.commit()
        return True
    except Exception as e:
        print(f"Error guardando distribución localidad: {e}")
        return False


def save_distribucion_poblacion_localidad_bulk(lista_data: list[dict]) -> int:
    """Guarda en lote distribución de población por localidad. Retorna número de registros insertados."""
    if not lista_data:
        return 0
    import json
    n = 0
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            for data in lista_data:
                ec = str(data.get("estado_codigo", "")).strip().zfill(2)
                mc = str(data.get("municipio_codigo", "")).strip().zfill(3)
                lc = str(data.get("loc_codigo", "")).strip()
                if not ec or not mc or not lc:
                    continue
                pt = int(data.get("POBTOT", 0) or 0)
                pf = int(data.get("POBFEM", 0) or 0)
                pm = int(data.get("POBMAS", 0) or 0)
                campos = ["estado_codigo", "municipio_codigo", "loc_codigo", "estado_nombre", "municipio_nombre", "localidad_nombre", "POBTOT", "POBFEM", "POBMAS"]
                extra = {k: v for k, v in data.items() if k not in campos}
                js = json.dumps(extra) if extra else None
                en = str(data.get("estado_nombre", "")).strip()
                mn = str(data.get("municipio_nombre", "")).strip()
                ln = str(data.get("localidad_nombre", "")).strip()
                cur.execute(
                    """INSERT INTO distribucion_poblacion_localidad
                       (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre, pobtot, pobfem, pobmas, data_json)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (estado_codigo, municipio_codigo, loc_codigo) DO UPDATE SET
                       estado_nombre = EXCLUDED.estado_nombre, municipio_nombre = EXCLUDED.municipio_nombre,
                       localidad_nombre = EXCLUDED.localidad_nombre, pobtot = EXCLUDED.pobtot,
                       pobfem = EXCLUDED.pobfem, pobmas = EXCLUDED.pobmas, data_json = EXCLUDED.data_json""",
                    (ec, en, mc, mn, lc, ln, pt, pf, pm, js),
                )
                n += 1
            conn.commit()
        return n
    except Exception as e:
        print(f"Error guardando distribución localidad (bulk): {e}")
        return n


def get_all_distribucion_localidad_para_crecimiento(limit: int = 100000) -> list[dict]:
    """Lista (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre) para ETL de crecimiento histórico."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre
                   FROM distribucion_poblacion_localidad
                   ORDER BY estado_codigo, municipio_codigo, loc_codigo
                   LIMIT %s""",
                (limit,),
            )
            rows = cur.fetchall()
            return [
                {
                    "estado_codigo": r[0], "estado_nombre": r[1],
                    "municipio_codigo": r[2], "municipio_nombre": r[3],
                    "loc_codigo": r[4], "localidad_nombre": r[5],
                }
                for r in rows
            ]
    except Exception as e:
        print(f"Error get_all_distribucion_localidad_para_crecimiento: {e}")
        return []


def get_crecimiento_historico_localidad_from_db(estado_nombre: str, municipio_nombre: str, localidad_nombre: str) -> list[dict]:
    """Obtiene crecimiento histórico (2005, 2010, 2020) por localidad desde PostgreSQL. Retorna [] si no hay datos."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT anio, poblacion, hombres, mujeres
                   FROM crecimiento_historico_localidad
                   WHERE estado_nombre ILIKE %s AND municipio_nombre ILIKE %s
                     AND (localidad_nombre ILIKE %s OR localidad_nombre ILIKE %s)
                   ORDER BY anio""",
                (f"%{estado_nombre}%", f"%{municipio_nombre}%", f"%{localidad_nombre}%", f"%{localidad_nombre.strip().lower()}%"),
            )
            rows = cur.fetchall()
            return [
                {"anio": r[0], "poblacion": r[1], "hombres": r[2], "mujeres": r[3]}
                for r in rows
            ]
    except Exception as e:
        print(f"Error obteniendo crecimiento histórico localidad: {e}")
        return []


def save_crecimiento_historico_localidad_to_db(
    estado_codigo: str,
    estado_nombre: str,
    municipio_codigo: str,
    municipio_nombre: str,
    loc_codigo: str,
    localidad_nombre: str,
    registros: list[dict],
) -> bool:
    """Guarda crecimiento histórico de una localidad. registros = [{anio, poblacion, hombres, mujeres}, ...]."""
    if not registros:
        return False
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            for r in registros:
                cur.execute(
                    """INSERT INTO crecimiento_historico_localidad
                       (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre, anio, poblacion, hombres, mujeres)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (estado_codigo, municipio_codigo, loc_codigo, anio) DO UPDATE SET
                       poblacion = EXCLUDED.poblacion, hombres = EXCLUDED.hombres, mujeres = EXCLUDED.mujeres""",
                    (
                        estado_codigo, estado_nombre, municipio_codigo, municipio_nombre,
                        loc_codigo, localidad_nombre,
                        int(r.get("anio", 0)),
                        int(r.get("poblacion", 0)),
                        int(r.get("hombres", 0)),
                        int(r.get("mujeres", 0)),
                    ),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"Error guardando crecimiento histórico localidad: {e}")
        return False


def get_ciudades_from_db() -> list[dict]:
    """Lista de ciudades para el menú (slug, nombre)."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT slug, nombre FROM ciudades ORDER BY id")
            return [{"slug": r[0], "nombre": r[1]} for r in cur.fetchall()]
    except Exception as e:
        print(f"Error get_ciudades_from_db: {e}")
        return []


def get_ciudad_by_slug_from_db(slug: str) -> dict | None:
    """Devuelve {slug, nombre, estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, es_entidad_completa} o None."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT slug, nombre, estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, es_entidad_completa FROM ciudades WHERE slug = %s",
                (slug,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "slug": row[0], "nombre": row[1], "estado_codigo": row[2], "estado_nombre": row[3],
                "municipio_codigo": row[4], "municipio_nombre": row[5], "es_entidad_completa": row[6],
            }
    except Exception as e:
        print(f"Error get_ciudad_by_slug_from_db: {e}")
        return None


def get_crecimiento_historico_municipal_from_db(estado_codigo: str, municipio_codigo: str | None = None) -> list[dict]:
    """Crecimiento histórico por municipio (2005, 2010, 2020). Si municipio_codigo es None, agrega todo el estado."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            if municipio_codigo:
                cur.execute(
                    """SELECT anio, poblacion, hombres, mujeres FROM crecimiento_historico_municipal
                       WHERE estado_codigo = %s AND municipio_codigo = %s ORDER BY anio""",
                    (estado_codigo, municipio_codigo),
                )
            else:
                cur.execute(
                    """SELECT anio, SUM(poblacion) AS poblacion, SUM(hombres) AS hombres, SUM(mujeres) AS mujeres
                       FROM crecimiento_historico_municipal WHERE estado_codigo = %s GROUP BY anio ORDER BY anio""",
                    (estado_codigo,),
                )
            rows = cur.fetchall()
            return [{"anio": r[0], "poblacion": int(r[1]), "hombres": int(r[2]), "mujeres": int(r[3])} for r in rows]
    except Exception as e:
        print(f"Error get_crecimiento_historico_municipal_from_db: {e}")
        return []


def save_crecimiento_historico_municipal_bulk(lista: list[dict]) -> int:
    """Bulk insert crecimiento_historico_municipal. Cada item: estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, anio, poblacion, hombres, mujeres."""
    if not lista:
        return 0
    n = 0
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            for r in lista:
                cur.execute(
                    """INSERT INTO crecimiento_historico_municipal
                       (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, anio, poblacion, hombres, mujeres)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (estado_codigo, municipio_codigo, anio) DO UPDATE SET
                       poblacion = EXCLUDED.poblacion, hombres = EXCLUDED.hombres, mujeres = EXCLUDED.mujeres""",
                    (
                        str(r.get("estado_codigo", "")).strip().zfill(2),
                        str(r.get("estado_nombre", "")).strip(),
                        str(r.get("municipio_codigo", "")).strip().zfill(3),
                        str(r.get("municipio_nombre", "")).strip(),
                        int(r.get("anio", 0)),
                        int(r.get("poblacion", 0)),
                        int(r.get("hombres", 0)),
                        int(r.get("mujeres", 0)),
                    ),
                )
                n += 1
            conn.commit()
        return n
    except Exception as e:
        print(f"Error save_crecimiento_historico_municipal_bulk: {e}")
        return n


def save_crecimiento_historico_localidad_bulk(lista: list[dict]) -> int:
    """Bulk insert crecimiento histórico. Cada item: estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre, anio, poblacion, hombres, mujeres."""
    if not lista:
        return 0
    n = 0
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            for r in lista:
                cur.execute(
                    """INSERT INTO crecimiento_historico_localidad
                       (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre, anio, poblacion, hombres, mujeres)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (estado_codigo, municipio_codigo, loc_codigo, anio) DO UPDATE SET
                       poblacion = EXCLUDED.poblacion, hombres = EXCLUDED.hombres, mujeres = EXCLUDED.mujeres""",
                    (
                        str(r.get("estado_codigo", "")).strip().zfill(2),
                        str(r.get("estado_nombre", "")).strip(),
                        str(r.get("municipio_codigo", "")).strip().zfill(3),
                        str(r.get("municipio_nombre", "")).strip(),
                        str(r.get("loc_codigo", "")).strip(),
                        str(r.get("localidad_nombre", "")).strip(),
                        int(r.get("anio", 0)),
                        int(r.get("poblacion", 0)),
                        int(r.get("hombres", 0)),
                        int(r.get("mujeres", 0)),
                    ),
                )
                n += 1
            conn.commit()
        return n
    except Exception as e:
        print(f"Error guardando crecimiento histórico localidad (bulk): {e}")
        return n


def get_distribucion_poblacion_entidad_from_db(estado_codigo: str) -> dict | None:
    """Agrega distribución de todos los municipios de un estado (para CDMX). Retorna dict con POBTOT, POBFEM, POBMAS y data_json con grupos de edad sumados."""
    try:
        import json
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT pobtot, pobfem, pobmas, data_json FROM distribucion_poblacion_municipal WHERE estado_codigo = %s""",
                (estado_codigo.strip().zfill(2),),
            )
            rows = cur.fetchall()
            if not rows:
                return None
            pobtot = sum(int(r[0] or 0) for r in rows)
            pobfem = sum(int(r[1] or 0) for r in rows)
            pobmas = sum(int(r[2] or 0) for r in rows)
            merged_json = {}
            for r in rows:
                if r[3]:
                    try:
                        js = json.loads(r[3])
                        for k, v in js.items():
                            if isinstance(v, (int, float)):
                                merged_json[k] = merged_json.get(k, 0) + int(v)
                    except Exception:
                        pass
            cur.execute("SELECT estado_nombre FROM distribucion_poblacion_municipal WHERE estado_codigo = %s LIMIT 1", (estado_codigo.strip().zfill(2),))
            nom = cur.fetchone()
            estado_nombre = nom[0] if nom else ""
            return {
                "estado_codigo": estado_codigo.strip().zfill(2), "estado_nombre": estado_nombre,
                "POBTOT": pobtot, "POBFEM": pobfem, "POBMAS": pobmas,
                **merged_json,
            }
    except Exception as e:
        print(f"Error get_distribucion_poblacion_entidad_from_db: {e}")
        return None


def save_aeropuertos_estatal_to_db(data: list[dict]) -> bool:
    """
    Guarda operaciones aeroportuarias por estado en PostgreSQL.
    data = [{estado_codigo, aeropuerto, grupo, anio, operaciones}, ...].
    Retorna True si ok.
    """
    if not data:
        return False
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            # Limpiar datos existentes antes de insertar nuevos
            cur.execute("DELETE FROM aeropuertos_estatal")
            for row in data:
                codigo = str(row.get("estado_codigo", "")).strip().zfill(2)
                aeropuerto = str(row.get("aeropuerto", "")).strip()
                grupo = str(row.get("grupo", "")).strip() or None
                anio = int(row.get("anio", 0))
                operaciones = int(row.get("operaciones", 0))
                if codigo and aeropuerto and anio > 0:
                    cur.execute(
                        """INSERT INTO aeropuertos_estatal (estado_codigo, aeropuerto, grupo, anio, operaciones)
                           VALUES (%s, %s, %s, %s, %s)
                           ON CONFLICT (estado_codigo, aeropuerto, anio) DO UPDATE SET
                           grupo = EXCLUDED.grupo,
                           operaciones = EXCLUDED.operaciones""",
                        (codigo, aeropuerto, grupo, anio, operaciones),
                    )
            conn.commit()
            return True
    except Exception as e:
        print(f"Error guardando aeropuertos: {e}")
        return False


def get_llegada_turistas_estatal_from_db(estado_codigo: str) -> list[dict]:
    """
    Retorna histórico de llegada de turistas para un estado.
    [{anio, turistas_total}, ...] ordenado por año descendente.
    """
    try:
        codigo = str(estado_codigo).strip().zfill(2)
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT anio, turistas_total FROM llegada_turistas_estatal WHERE estado_codigo = %s ORDER BY anio DESC LIMIT 10",
                (codigo,)
            )
            rows = cur.fetchall()
            # Ordenar ascendente para la gráfica? El requerimiento dice "10 años más recientes".
            # Pero Plotly suele preferir orden cronológico.
            data = [{"anio": r[0], "total": r[1]} for r in rows]
            data.sort(key=lambda x: x["anio"])
            return data
    except Exception:
        return []


# ——— Funciones para Análisis Geo-Económico (PIB y Demografía General) ———

def save_estado_info_general_to_db(estado: str, poblacion: int, extension_km2: int) -> bool:
    """Guarda información estática (Censo 2020 y km2) de un estado."""
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO estado_info_general (estado, poblacion, extension_km2)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (estado) DO UPDATE SET
                   poblacion = EXCLUDED.poblacion,
                   extension_km2 = EXCLUDED.extension_km2""",
                (estado.strip(), poblacion, extension_km2)
            )
            conn.commit()
            return True
    except Exception as e:
        print(f"Error guardando estado_info_general para {estado}: {e}")
        return False

def save_pib_estatal_to_db(data: list[dict]) -> bool:
    """
    Guarda el PIB Histórico por Estado en PostgreSQL.
    data = [{estado, anio, pib_actual, pib_anterior, variacion_pct}, ...]
    """
    if not data:
        return False
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            for row in data:
                estado = (row.get("estado") or row.get("Estado") or "").strip()
                anio = row.get("anio") or row.get("Anio")
                p_act = row.get("pib_actual") or row.get("PIB_Actual") or 0.0
                p_ant = row.get("pib_anterior") or row.get("PIB_Anterior") or 0.0
                v_pct = row.get("variacion_pct") or row.get("Variacion_Pct") or 0.0
                if estado and anio:
                    cur.execute(
                        """INSERT INTO pib_estatal (estado, anio, pib_actual, pib_anterior, variacion_pct)
                           VALUES (%s, %s, %s, %s, %s)
                           ON CONFLICT (estado, anio) DO UPDATE SET
                           pib_actual = EXCLUDED.pib_actual,
                           pib_anterior = EXCLUDED.pib_anterior,
                           variacion_pct = EXCLUDED.variacion_pct""",
                        (estado, anio, p_act, p_ant, v_pct)
                    )
            conn.commit()
            return True
    except Exception as e:
        print(f"Error guardando pib_estatal: {e}")
        return False

def get_geo_economico_from_db(estado: str) -> dict | None:
    """
    Retorna datos de PIB e info general para el Análisis Geo-Económico.
    Retorna: { series: [{anio, pib_actual, pib_anterior, variacion_pct}], poblacion, extension_km2 }
    """
    try:
        from services.db import db_connection
        estado_clean = estado.strip().lower()
        with db_connection() as conn:
            cur = conn.cursor()
            
            # 1. Obtener Info General (Población, Extensión)
            cur.execute(
                "SELECT poblacion, extension_km2 FROM estado_info_general WHERE LOWER(estado) = %s LIMIT 1",
                (estado_clean,)
            )
            info_row = cur.fetchone()
            if not info_row:
                return None
            pob, ext = info_row

            # 2. Obtener Histórico de PIB
            cur.execute(
                "SELECT anio, pib_actual, pib_anterior, variacion_pct FROM pib_estatal WHERE LOWER(estado) = %s ORDER BY anio ASC",
                (estado_clean,)
            )
            rows = cur.fetchall()
            series = [
                {"anio": r[0], "pib_actual": float(r[1]), "pib_anterior": float(r[2]), "variacion_pct": float(r[3])} 
                for r in rows
            ]
            
            return {
                "series": series,
                "poblacion": pob,
                "extension_km2": ext
            }
    except Exception as e:
        print(f"Error get_geo_economico_from_db para {estado}: {e}")
        return None

# ——— Funciones para Municipios ———

def _normalizar_municipio(s: str) -> str:
    """Normaliza nombre de municipio para comparación (sin acentos, minúsculas)."""
    import unicodedata
    return "".join(
        c for c in unicodedata.normalize("NFD", str(s).lower().strip())
        if unicodedata.category(c) != "Mn"
    )


def _estado_nombre_to_codigo(estado_nombre: str) -> str | None:
    """Convierte nombre de estado a código INEGI (01-32)."""
    estado_map = {
        "aguascalientes": "01", "baja california": "02", "baja california sur": "03",
        "campeche": "04", "coahuila de zaragoza": "05", "colima": "06",
        "chiapas": "07", "chihuahua": "08", "ciudad de méxico": "09",
        "durango": "10", "guanajuato": "11", "guerrero": "12",
        "hidalgo": "13", "jalisco": "14", "méxico": "15",
        "michoacán de ocampo": "16", "morelos": "17", "nayarit": "18",
        "nuevo león": "19", "oaxaca": "20", "puebla": "21",
        "querétaro": "22", "quintana roo": "23", "san luis potosí": "24",
        "sinaloa": "25", "sonora": "26", "tabasco": "27",
        "tamaulipas": "28", "tlaxcala": "29", "veracruz de ignacio de la llave": "30",
        "yucatán": "31", "zacatecas": "32",
    }
    estado_norm = _normalizar_municipio(estado_nombre)
    return estado_map.get(estado_norm)


def get_municipios_from_db(estado_nombre: str) -> list[dict]:
    """
    Obtiene lista de municipios para un estado desde PostgreSQL.
    Prioridad: PostgreSQL.
    Retorna [{nombre, codigo}, ...] ordenado por nombre.
    """
    if not estado_nombre:
        return []
    estado_codigo = _estado_nombre_to_codigo(estado_nombre)
    if not estado_codigo:
        return []
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT municipio_nombre, municipio_codigo
                   FROM municipios 
                   WHERE estado_codigo = %s 
                   ORDER BY municipio_nombre""",
                (estado_codigo,),
            )
            rows = cur.fetchall()
            return [
                {"nombre": r[0], "codigo": r[1]}
                for r in rows
            ]
    except Exception:
        return []

def get_proyeccion_poblacional_sexo_from_db(estado_codigo: str, municipio_codigo: str | None = None) -> list[dict]:
    """
    Obtiene histórico/proyección de población por sexo desde proyeccion_poblacional_municipal (CONAPO).
    Retorna [{anio, hombres, mujeres}, ...] ordenado por año.
    """
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            if municipio_codigo:
                cur.execute(
                    """SELECT anio, 
                              SUM(CASE WHEN UPPER(sexo) IN ('HOMBRES', 'HOMBRE') THEN poblacion ELSE 0 END) as hombres,
                              SUM(CASE WHEN UPPER(sexo) IN ('MUJERES', 'MUJER') THEN poblacion ELSE 0 END) as mujeres
                       FROM proyeccion_poblacional_municipal
                       WHERE estado_codigo = %s AND municipio_codigo = %s
                       GROUP BY anio ORDER BY anio""",
                    (estado_codigo, municipio_codigo),
                )
            else:
                cur.execute(
                    """SELECT anio, 
                              SUM(CASE WHEN UPPER(sexo) IN ('HOMBRES', 'HOMBRE') THEN poblacion ELSE 0 END) as hombres,
                              SUM(CASE WHEN UPPER(sexo) IN ('MUJERES', 'MUJER') THEN poblacion ELSE 0 END) as mujeres
                       FROM proyeccion_poblacional_municipal
                       WHERE estado_codigo = %s
                       GROUP BY anio ORDER BY anio""",
                    (estado_codigo,),
                )
            rows = cur.fetchall()
            return [{"anio": r[0], "hombres": int(r[1]), "mujeres": int(r[2])} for r in rows]
    except Exception as e:
        print(f"Error get_proyeccion_poblacional_sexo_from_db: {e}")
        return []

def save_tourism_generic_bulk(table_name, data, value_key="valor"):
    """Guarda datos de turismo de forma genérica (ON CONFLICT)."""
    if not data: return 0
    try:
        from services.db import db_connection
        with db_connection() as conn:
            with conn.cursor() as cur:
                # Determinar si tiene mes o trimestre
                period_col = "mes" if "mes" in data[0] else "trimestre"
                period_lbl_col = f"{period_col}_lbl"
                
                tuples = []
                for r in data:
                    val = r.get(value_key) or r.get("valor")
                    tuples.append((
                        r["estado_codigo"], 
                        r.get("municipio_codigo"), 
                        r["anio"], 
                        r[period_col], 
                        r.get(period_lbl_col, ""),
                        val
                    ))
                
                # Construir consulta dinámica
                query = f"""
                    INSERT INTO {table_name} 
                    (estado_codigo, municipio_codigo, anio, {period_col}, {period_lbl_col}, valor)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (estado_codigo, municipio_codigo, anio, {period_col}) 
                    DO UPDATE SET valor = EXCLUDED.valor, {period_lbl_col} = EXCLUDED.{period_lbl_col}
                """
                if "municipio_codigo" in data[0] and data[0]["municipio_codigo"] is None:
                    # Caso estatal, el ON CONFLICT debe manejar el NULL si la tabla lo permite
                    # pero usualmente las tablas tienen UNIQUE (estado, municipio, anio, periodo)
                    # Si municipio es NULL, Postgres lo ve como diferente.
                    # Para simplificar, asumimos que municipio_codigo es "" o NULL según la tabla.
                    pass
                
                cur.executemany(query, tuples)
                return len(tuples)
    except Exception as e:
        print(f"Error save_tourism_generic_bulk ({table_name}): {e}")
        return 0

def get_tourism_generic_from_db(table_name, estado_codigo, municipio_codigo=None):
    """Obtiene datos de turismo de forma genérica desde PostgreSQL."""
    try:
        from services.db import db_connection
        with db_connection() as conn:
            with conn.cursor() as cur:
                # Verificar si la tabla usa mes o trimestre
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name = 'mes'")
                is_monthly = cur.fetchone() is not None
                period_col = "mes" if is_monthly else "trimestre"
                period_lbl_col = f"{period_col}_lbl"

                query = f"SELECT anio, {period_col}, {period_lbl_col}, valor FROM {table_name} WHERE estado_codigo = %s"
                params = [estado_codigo]
                if municipio_codigo:
                    query += " AND municipio_codigo = %s"
                    params.append(municipio_codigo)
                else:
                    query += " AND municipio_codigo IS NULL"
                query += f" ORDER BY anio, {period_col}"
                
                cur.execute(query, params)
                rows = cur.fetchall()
                
                results = []
                for r in rows:
                    y, p_num, p_lbl, val = r
                    # Limpiar label si existe, si no usar inicial
                    lbl = p_lbl if p_lbl else f"P{p_num}"
                    
                    # Opcional: Acortar meses largos
                    months_map = {"enero": "Ene", "febrero": "Feb", "marzo": "Mar", "abril": "Abr", "mayo": "May", "junio": "Jun", 
                                  "julio": "Jul", "agosto": "Ago", "septiembre": "Sep", "octubre": "Oct", "noviembre": "Nov", "diciembre": "Dic",
                                  "i trimestre": "1T", "ii trimestre": "2T", "iii trimestre": "3T", "iv trimestre": "4T"}
                    
                    short_lbl = lbl
                    for full, short in months_map.items():
                        if full in lbl.lower():
                            short_lbl = f"{short} {y}"
                            break
                    else:
                        short_lbl = f"{lbl} {y}"
                    
                    results.append({
                        "anio": y,
                        "periodo_num": p_num,
                        "periodo": short_lbl,
                        "valor": float(val)
                    })
                return results
    except Exception as e:
        print(f"Error get_tourism_generic_from_db ({table_name}): {e}")
        return []

def save_poblacion_ocupada_turismo_bulk(data):
    return save_tourism_generic_bulk("poblacion_ocupada_turismo_municipal", data, "poblacion_ocupada")

def get_poblacion_ocupada_turismo_from_db(estado_codigo, municipio_codigo=None):
    data = get_tourism_generic_from_db("poblacion_ocupada_turismo_municipal", estado_codigo, municipio_codigo)
    return data

def save_ocupacion_hotelera_bulk(data):
    return save_tourism_generic_bulk("ocupacion_hotelera_municipal", data)

def get_ocupacion_hotelera_from_db(estado_codigo, municipio_codigo=None):
    return get_tourism_generic_from_db("ocupacion_hotelera_municipal", estado_codigo, municipio_codigo)

def save_llegada_visitantes_bulk(data):
    return save_tourism_generic_bulk("llegada_visitantes_municipal", data)

def get_llegada_visitantes_from_db(estado_codigo, municipio_codigo=None):
    return get_tourism_generic_from_db("llegada_visitantes_municipal", estado_codigo, municipio_codigo)

def save_gasto_promedio_bulk(data):
    return save_tourism_generic_bulk("gasto_promedio_municipal", data)

def get_gasto_promedio_from_db(estado_codigo, municipio_codigo=None):
    return get_tourism_generic_from_db("gasto_promedio_municipal", estado_codigo, municipio_codigo)

def save_derrama_economica_bulk(data):
    return save_tourism_generic_bulk("derrama_economica_turismo", data)

def get_derrama_economica_from_db(estado_codigo, municipio_codigo=None):
    return get_tourism_generic_from_db("derrama_economica_turismo", estado_codigo, municipio_codigo)

def save_ingreso_hotelero_bulk(data):
    return save_tourism_generic_bulk("ingreso_hotelero_municipal", data)

def get_ingreso_hotelero_from_db(estado_codigo, municipio_codigo=None):
    return get_tourism_generic_from_db("ingreso_hotelero_municipal", estado_codigo, municipio_codigo)

def save_establecimientos_turismo_bulk(data):
    return save_tourism_generic_bulk("establecimientos_turismo_municipal", data)

def get_establecimientos_turismo_from_db(estado_codigo, municipio_codigo=None):
    return get_tourism_generic_from_db("establecimientos_turismo_municipal", estado_codigo, municipio_codigo)

def get_ventas_internacionales_from_db(estado_codigo, municipio_codigo):
    if not municipio_codigo: return []
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT anio, mes, flujo, valor_usd 
                FROM ventas_internacionales_municipal
                WHERE estado_codigo = %s AND municipio_codigo = %s
                ORDER BY anio, mes
            """, (estado_codigo, municipio_codigo))
            rows = cur.fetchall()
            
            results = []
            meses_map = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
            for r in rows:
                anio, mes, flujo, valor = r
                results.append({
                    "anio": anio,
                    "mes": mes,
                    "periodo": f"{meses_map[mes - 1]} {anio}",
                    "flujo": "Exportaciones" if "Export" in flujo else "Importaciones",
                    "valor_usd": float(valor)
                })
            return results
    except Exception as e:
        print(f"Error get_ventas_internacionales_from_db: {e}")
        return []


def get_oferta_servicios_turisticos_from_db(estado_codigo, municipio_codigo):
    """Lee de oferta_servicios_turisticos_municipal: ventas por categoría (Turissste)."""
    if not municipio_codigo: return []
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT anio, ventas_hospedaje, ventas_aereos, ventas_terrestre, ventas_excursiones, 
                       ventas_paquetes_propios, ventas_paquetes_no_propios, ventas_turismo_negocios
                FROM oferta_servicios_turisticos_municipal
                WHERE estado_codigo = %s AND municipio_codigo = %s
                ORDER BY anio
            """, (estado_codigo, municipio_codigo))
            rows = cur.fetchall()
            
            results = []
            for r in rows:
                anio, hospedaje, aereos, terrestre, excursiones, pq_propios, pq_no_propios, negocios = r
                results.append({
                    "anio": anio,
                    "ventas_hospedaje": float(hospedaje or 0),
                    "ventas_aereos": float(aereos or 0),
                    "ventas_terrestre": float(terrestre or 0),
                    "ventas_excursiones": float(excursiones or 0),
                    "ventas_paquetes": float((pq_propios or 0) + (pq_no_propios or 0)),
                    "ventas_turismo_negocios": float(negocios or 0)
                })
            return results
    except Exception as e:
        print(f"Error get_oferta_servicios_turisticos_from_db: {e}")
        return []

def get_vuelos_llegada_aicm_from_db(estado_codigo, municipio_codigo):
    if not municipio_codigo: return []
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT anio, llegadas_t1, llegadas_t2
                FROM vuelos_llegada_aicm_municipal
                WHERE estado_codigo = %s AND municipio_codigo = %s
                ORDER BY anio
            """, (estado_codigo, municipio_codigo))
            rows = cur.fetchall()
            
            results = []
            for r in rows:
                anio, t1, t2 = r
                results.append({
                    "anio": anio,
                    "llegadas_t1": int(t1),
                    "llegadas_t2": int(t2)
                })
            return results
    except Exception as e:
        print(f"Error get_vuelos_llegada_aicm_from_db: {e}")
        return []

def get_comercio_internacional_from_db(estado_nombre):
    """
    Obtiene los datos de Inversión Extranjera Directa (Comercio Internacional) para un estado,
    agrupados por año y mostrando los principales sectores (top 5 y 'Otros').
    """
    if not estado_nombre: return []
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            
            cur.execute("""
                SELECT anio, sector, SUM(monto_mdd) AS total_mdd
                FROM ied_historico_entidad_sector
                WHERE estado_nombre = %s AND monto_mdd > 0
                GROUP BY anio, sector
                ORDER BY anio, total_mdd DESC
            """, (estado_nombre,))
            rows = cur.fetchall()
            
            data_by_year = {}
            for anio, sector, monto_mdd in rows:
                if anio not in data_by_year:
                    data_by_year[anio] = {"anio": anio, "sectores": [], "otros": 0}
                
                # Si tenemos menos de 5 sectores, lo agregamos a la lista, sino a otros
                if len(data_by_year[anio]["sectores"]) < 5:
                    data_by_year[anio]["sectores"].append({
                        "sector": sector,
                        "monto_mdd": float(monto_mdd)
                    })
                else:
                    data_by_year[anio]["otros"] += float(monto_mdd)
            
            # Formatear la salida como una lista simple de valores por año para facilitar la gráfica
            return list(data_by_year.values())

    except Exception as e:
        print(f"Error get_comercio_internacional_from_db: {e}")
        return []

def get_llegada_pasajeros_from_db(ciudad_slug):
    """Obtiene datos de llegada de pasajeros al aeropuerto por ciudad."""
    if not ciudad_slug: return []
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT anio, pasajeros_nacionales, pasajeros_internacionales, pasajeros_total
                FROM llegada_pasajeros_aeropuerto
                WHERE ciudad_slug = %s
                ORDER BY anio
            """, (ciudad_slug,))
            rows = cur.fetchall()
            results = []
            for r in rows:
                anio, nac, intl, total = r
                results.append({
                    "anio": anio,
                    "nacionales": int(nac or 0),
                    "internacionales": int(intl or 0),
                    "total": int(total or 0)
                })
            return results
    except Exception as e:
        print(f"Error get_llegada_pasajeros_from_db: {e}")
        return []

def get_visitantes_nac_ext_from_db(ciudad_slug):
    """Obtiene visitantes nacionales y extranjeros por ciudad."""
    if not ciudad_slug: return []
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT anio, visitantes_nacionales, visitantes_extranjeros, pct_nacionales, pct_extranjeros
                FROM visitantes_nacionales_extranjeros
                WHERE ciudad_slug = %s
                ORDER BY anio
            """, (ciudad_slug,))
            rows = cur.fetchall()
            results = []
            for r in rows:
                anio, nac, ext, pct_nac, pct_ext = r
                results.append({
                    "anio": anio,
                    "nacionales": int(nac or 0),
                    "extranjeros": int(ext or 0),
                    "total": int(nac or 0) + int(ext or 0),
                    "pct_nacionales": round(float(pct_nac or 0) * 100, 1),
                    "pct_extranjeros": round(float(pct_ext or 0) * 100, 1)
                })
            return results
    except Exception as e:
        print(f"Error get_visitantes_nac_ext_from_db: {e}")
        return []
