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
    port = _get_db_port()
    # Si usamos localhost, el puerto expuesto suele ser 5433 (mapeo Docker)
    if host == "localhost" and port == "5432":
        port = os.getenv("POSTGRES_PORT", "5433")
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
