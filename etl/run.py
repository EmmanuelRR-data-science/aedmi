"""
ETL para KPIs nacionales.

PIPELINE DE DATOS:
  1. INGESTA  → services.data_sources: fetch crudo desde INEGI y Banxico
  2. PROCESAMIENTO → get_kpis_nacional: cálculos (PIB USD, inflación anual, etc.)
  3. CARGA → run_etl: INSERT/UPDATE en PostgreSQL (kpis_nacional)
  4. API → app.py: GET /api/kpis/nacional lee desde PostgreSQL
  5. FRONTEND → dashboard.html: fetch API y renderiza KPIs

Ejecutar: python -m etl.run
"""

import os
import sys

import psycopg2
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()


def _get_db_host():
    """Host de PostgreSQL. Usa localhost si 'db' no se resuelve (ejecución fuera de Docker)."""
    host = os.getenv("POSTGRES_HOST", "db")
    if host == "db":
        try:
            import socket
            socket.gethostbyname("db")
        except (socket.gaierror, OSError):
            return "localhost"
    return host


def get_db_conn():
    """Conexión a PostgreSQL."""
    host = _get_db_host()
    port = os.getenv("POSTGRES_PORT", "5432")
    if host == "localhost" and port == "5432":
        port = "5433"  # Puerto expuesto por Docker en el host
    return psycopg2.connect(
        host=host,
        port=int(port),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "dash_db"),
    )


def _ensure_schema(conn):
    """Aplica schema.sql para crear tablas faltantes en BD existente."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    if not os.path.isfile(schema_path):
        return
    try:
        with open(schema_path, encoding="utf-8") as f:
            schema_sql = f.read()
        cur = conn.cursor()
        try:
            conn.autocommit = True
            for stmt in schema_sql.split(";"):
                stmt = stmt.strip()
                lines = [l for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")]
                stmt = "\n".join(lines)
                if stmt.upper().startswith("CREATE TABLE"):
                    try:
                        cur.execute(stmt)
                    except Exception:
                        pass  # Tabla puede existir
        finally:
            conn.autocommit = False
        cur.close()
    except Exception as e:
        if conn:
            conn.rollback()
            conn.autocommit = False
        print(f"  [WARN] Aplicando schema: {e}")


def run_etl():
    """Ejecuta el ETL: obtiene KPIs y guarda en PostgreSQL."""
    from services.data_sources import _fetch_inegi_crecimiento_poblacional, get_kpis_nacional

    conn = None
    try:
        conn = get_db_conn()
        _ensure_schema(conn)
        cur = conn.cursor()

        kpis = get_kpis_nacional()
        rows = []
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
            rows.append((key, num_val, date_str))

        # Upsert cada indicador
        for row in rows:
            cur.execute(
                """
                INSERT INTO kpis_nacional (indicator, value, date, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (indicator) DO UPDATE SET
                    value = EXCLUDED.value,
                    date = EXCLUDED.date,
                    updated_at = CURRENT_TIMESTAMP
                """,
                row,
            )
        conn.commit()

        # Crecimiento poblacional nacional (desde INEGI)
        try:
            poblacion = _fetch_inegi_crecimiento_poblacional()
            if poblacion:
                cur.execute("DELETE FROM crecimiento_poblacional_nacional")
                for r in poblacion:
                    cur.execute(
                        "INSERT INTO crecimiento_poblacional_nacional (year, value) VALUES (%s, %s)",
                        (r["year"], r["value"]),
                    )
                conn.commit()
                print(f"  Crecimiento poblacional: {len(poblacion)} registros")
        except Exception as e:
            conn.rollback()
            cur = conn.cursor()
            print(f"  [WARN] Crecimiento poblacional: {e}")

        # Estructura poblacional por edad
        try:
            from services.data_sources import _fetch_inegi_estructura_poblacional

            estructura = _fetch_inegi_estructura_poblacional()
            if estructura:
                cur.execute("DELETE FROM estructura_poblacional_inegi")
                for r in estructura:
                    cur.execute(
                        "INSERT INTO estructura_poblacional_inegi (year, pob_0_14, pob_15_64, pob_65_plus) VALUES (%s, %s, %s, %s)",
                        (r["year"], r["pob_0_14"], r["pob_15_64"], r["pob_65_plus"]),
                    )
                conn.commit()
                print(f"  Estructura poblacional: {len(estructura)} registros")
        except Exception as e:
            conn.rollback()
            cur = conn.cursor()
            print(f"  [WARN] Estructura poblacional: {e}")

        # Distribución por sexo
        try:
            from services.data_sources import _fetch_inegi_distribucion_sexo

            sexo = _fetch_inegi_distribucion_sexo()
            if sexo:
                cur.execute("DELETE FROM distribucion_sexo_inegi")
                for r in sexo:
                    cur.execute(
                        "INSERT INTO distribucion_sexo_inegi (year, male, female) VALUES (%s, %s, %s)",
                        (r["year"], r["male"], r["female"]),
                    )
                conn.commit()
                print(f"  Distribución por sexo: {len(sexo)} registros")
        except Exception as e:
            conn.rollback()
            cur = conn.cursor()
            print(f"  [WARN] Distribución por sexo: {e}")

        # PEA (Población Económicamente Activa)
        try:
            from services.data_sources import _fetch_inegi_pea

            pea = _fetch_inegi_pea()
            if pea:
                cur.execute("DELETE FROM pea_inegi")
                for r in pea:
                    cur.execute(
                        "INSERT INTO pea_inegi (anio, trimestre, valor) VALUES (%s, %s, %s)",
                        (r["anio"], r["trimestre"], r["valor"]),
                    )
                conn.commit()
                print(f"  PEA: {len(pea)} registros")
        except Exception as e:
            conn.rollback()
            cur = conn.cursor()
            print(f"  [WARN] PEA: {e}")

        # Población por sector de actividad
        try:
            from services.data_sources import _fetch_inegi_pob_sector_actividad

            sector = _fetch_inegi_pob_sector_actividad()
            if sector:
                cur.execute("DELETE FROM pob_sector_actividad")
                for r in sector:
                    cur.execute(
                        "INSERT INTO pob_sector_actividad (sector, valor, pct, es_residual) VALUES (%s, %s, %s, %s)",
                        (r["sector"], r["valor"], r["pct"], r.get("es_residual", False)),
                    )
                conn.commit()
                print(f"  Población por sector: {len(sector)} registros")
        except Exception as e:
            conn.rollback()
            cur = conn.cursor()
            print(f"  [WARN] Población por sector: {e}")

        # Tipo de cambio (Banxico SF43718 - pq-estudios-mercado-vps)
        try:
            from services.data_sources import _fetch_tipo_cambio_banxico
            from services.db import save_tipo_cambio_to_db

            diario, mensual = _fetch_tipo_cambio_banxico()
            if diario and mensual:
                save_tipo_cambio_to_db(diario, mensual)
                print(f"  Tipo de cambio: {len(diario)} diarios, {len(mensual)} mensuales")
        except Exception as e:
            print(f"  [WARN] Tipo de cambio: {e}")

        # Inflación nacional (Banxico INPC - inflacion_nacional.ipynb)
        try:
            from services.data_sources import _fetch_inflacion_nacional_banxico
            from services.db import save_inflacion_nacional_to_db

            inflacion = _fetch_inflacion_nacional_banxico()
            if inflacion:
                save_inflacion_nacional_to_db(inflacion)
                print(f"  Inflación nacional: {len(inflacion)} registros")
        except Exception as e:
            print(f"  [WARN] Inflación nacional: {e}")

        # IED Flujo por Entidad (últimos 4 trimestres - inversion_extranjera_ied.ipynb)
        try:
            from services.data_sources import get_ied_flujo_entidad
            from services.db import save_ied_flujo_entidad_to_db

            ied_flujo = get_ied_flujo_entidad()
            if ied_flujo:
                save_ied_flujo_entidad_to_db(ied_flujo)
                print(f"  IED flujo entidad: {len(ied_flujo)} registros")
        except Exception as e:
            print(f"  [WARN] IED flujo entidad: {e}")

        # Ranking Turismo Mundial (Banco Mundial WDI)
        try:
            from services.data_sources import get_ranking_turismo_wb
            from services.db import save_ranking_turismo_wb_to_db

            ranking_turismo = get_ranking_turismo_wb()
            if ranking_turismo:
                save_ranking_turismo_wb_to_db(ranking_turismo)
                print(f"  Ranking turismo: {len(ranking_turismo)} registros")
        except Exception as e:
            print(f"  [WARN] Ranking turismo: {e}")

        # Balanza de Visitantes (INEGI BISE)
        try:
            from services.data_sources import get_balanza_visitantes
            from services.db import save_balanza_visitantes_to_db

            balanza = get_balanza_visitantes()
            if balanza:
                save_balanza_visitantes_to_db(balanza)
                print(f"  Balanza visitantes: {len(balanza)} registros")
        except Exception as e:
            print(f"  [WARN] Balanza visitantes: {e}")

        # Anuncios de Inversión Combinados (DataMéxico)
        try:
            from services.data_sources import _fetch_and_process_anuncios_combinados, _load_anuncios_combinados_from_csv
            from services.db import save_anuncios_combinados_to_db

            raw = _fetch_and_process_anuncios_combinados()
            if not raw:
                raw = _load_anuncios_combinados_from_csv()
            if raw:
                save_anuncios_combinados_to_db(raw)
                print(f"  Anuncios Inversión Combinados: {len(raw)} registros")
        except Exception as e:
            print(f"  [WARN] Anuncios Inversión Combinados: {e}")

        # Anuncios de Inversión Base (DataMéxico)
        try:
            from services.data_sources import _fetch_and_process_anuncios_base, _load_anuncios_base_from_csv
            from services.db import save_anuncios_base_to_db

            raw_base = _fetch_and_process_anuncios_base()
            if not raw_base:
                raw_base = _load_anuncios_base_from_csv()
            if raw_base:
                save_anuncios_base_to_db(raw_base)
                print(f"  Anuncios Inversión Base: {len(raw_base)} registros")
        except Exception as e:
            print(f"  [WARN] Anuncios Inversión Base: {e}")

        # Participación Mercado Aéreo (AFAC/DataTur)
        try:
            from services.data_sources import get_participacion_mercado_aereo

            pma = get_participacion_mercado_aereo()
            if pma.get("nacional") or pma.get("internacional"):
                print(f"  Participación Mercado Aéreo: {len(pma.get('nacional', []))} nacional, {len(pma.get('internacional', []))} internacional")
        except Exception as e:
            print(f"  [WARN] Participación Mercado Aéreo: {e}")

        # IED por País de Origen (Secretaría de Economía)
        try:
            from services.data_sources import get_ied_paises
            from services.db import save_ied_paises_to_db

            ied_paises = get_ied_paises()
            if ied_paises:
                save_ied_paises_to_db(ied_paises)
                print(f"  IED paises: {len(ied_paises)} registros")
        except Exception as e:
            print(f"  [WARN] IED paises: {e}")

        # IED por Sector Económico (Secretaría de Economía)
        try:
            from services.data_sources import get_ied_sectores
            from services.db import save_ied_sectores_to_db

            ied_sectores = get_ied_sectores()
            if ied_sectores:
                save_ied_sectores_to_db(ied_sectores)
                print(f"  IED sectores: {len(ied_sectores)} registros")
        except Exception as e:
            print(f"  [WARN] IED sectores: {e}")

        # Proyección PIB (FMI WEO)
        try:
            from services.data_sources import _fetch_proyeccion_pib_fmi

            data, tc_fix, tc_date = _fetch_proyeccion_pib_fmi()
            if data:
                cur.execute("DELETE FROM pib_proyeccion_fmi")
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
                conn.commit()
                print(f"  Proyección PIB: {len(data)} registros (TC: {tc_fix:.2f})")
        except Exception as e:
            conn.rollback()
            cur = conn.cursor()
            print(f"  [WARN] Proyección PIB: {e}")

        cur.execute(
            """
            INSERT INTO etl_log (status, indicators_updated)
            VALUES ('success', %s)
            """,
            (len(rows),),
        )
        conn.commit()
        print(f"ETL completado: {len(rows)} indicadores actualizados")
        return 0

    except Exception as e:
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO etl_log (status, error_message) VALUES ('error', %s)",
                    (str(e),),
                )
                conn.commit()
            except Exception:
                pass
        print(f"Error ETL: {e}", file=sys.stderr)
        return 1
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    sys.exit(run_etl())
