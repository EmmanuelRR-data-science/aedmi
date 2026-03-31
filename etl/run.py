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

        # Balanza Comercial por Producto (Economía/DataMéxico - inegi_foreign_trade_product)
        try:
            from services.data_sources import fetch_balanza_comercial_producto_from_api
            from services.db import save_balanza_comercial_producto_to_db

            bcp = fetch_balanza_comercial_producto_from_api()
            if bcp:
                save_balanza_comercial_producto_to_db(bcp)
                print(f"  Balanza comercial por producto: {len(bcp)} registros")
        except Exception as e:
            print(f"  [WARN] Balanza comercial por producto: {e}")

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

        # Producto Aeropuertos Nacional (Excel 2006-2025) → PostgreSQL
        try:
            import os
            from services.data_sources import load_producto_aeropuertos_from_excel
            from services.db import save_producto_aeropuertos_nacional_to_db

            xlsx_path = os.getenv("PRODUCTO_AEROPUERTOS_XLSX", "").strip()
            if not xlsx_path or not os.path.isfile(xlsx_path):
                downloads = os.path.join(os.environ.get("USERPROFILE", ""), "Downloads", "producto-aeropuertos-2006-2025-nov-29122025.xlsx")
                if os.path.isfile(downloads):
                    xlsx_path = downloads
            if xlsx_path and os.path.isfile(xlsx_path):
                data = load_producto_aeropuertos_from_excel(xlsx_path)
                if data:
                    save_producto_aeropuertos_nacional_to_db(data)
                    print(f"  Producto Aeropuertos Nacional: {len(data)} registros -> PostgreSQL")
                else:
                    print("  Producto Aeropuertos Nacional: 0 registros (revisar estructura del Excel)")
            else:
                print("  Producto Aeropuertos Nacional: archivo no encontrado. Defina PRODUCTO_AEROPUERTOS_XLSX o coloque el .xlsx en Downloads.")
        except Exception as e:
            print(f"  [WARN] Producto Aeropuertos Nacional: {e}")

        # Proyecciones CONAPO: descargar CSV si no existe y cargar por estado
        try:
            from services.data_sources import _download_conapo_proyecciones_csv, get_proyecciones_conapo, STATE_ID_TO_NAME
            from services.db import save_proyecciones_conapo_to_db

            csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "process", "proyecciones_conapo.csv")
            if not os.path.isfile(csv_path) or os.path.getsize(csv_path) < 500:
                if _download_conapo_proyecciones_csv():
                    print("  Proyecciones CONAPO: CSV descargado")
            proyecciones_ok = 0
            for _sid, nombre in STATE_ID_TO_NAME.items():
                try:
                    data = get_proyecciones_conapo(nombre)
                    if data and len(data) > 0:
                        codigo = str(_sid).zfill(2)
                        if save_proyecciones_conapo_to_db(codigo, data):
                            proyecciones_ok += 1
                except Exception:
                    pass
            if proyecciones_ok:
                print(f"  Proyecciones CONAPO: {proyecciones_ok} estados -> PostgreSQL")
        except Exception as e:
            print(f"  [WARN] Proyecciones CONAPO: {e}")

        # ITAEE estatal (INEGI BIE con token en .env)
        try:
            from services.data_sources import get_itaee_estatal, STATE_ID_TO_NAME
            from services.db import save_itaee_estatal_to_db

            itaee_ok = 0
            for _sid, nombre in STATE_ID_TO_NAME.items():
                try:
                    data = get_itaee_estatal(nombre)
                    if data:
                        codigo = str(_sid).zfill(2)
                        if save_itaee_estatal_to_db(codigo, data):
                            itaee_ok += 1
                except Exception:
                    pass
            if itaee_ok:
                print(f"  ITAEE estatal: {itaee_ok} estados -> PostgreSQL")
        except Exception as e:
            print(f"  [WARN] ITAEE estatal: {e}")

        # Exportaciones por Estado (DataMéxico API, sin token)
        try:
            from services.data_sources import _get_exportaciones_por_estado_from_api
            from services.db import save_exportaciones_estatal_to_db

            api_data = _get_exportaciones_por_estado_from_api()
            if api_data:
                if save_exportaciones_estatal_to_db(api_data):
                    print(f"  Exportaciones estatal: {len(api_data)} registros -> PostgreSQL")
        except Exception as e:
            print(f"  [WARN] Exportaciones estatal: {e}")

        # Aeropuertos por Estado (DGAC Excel desde CUADRO_DGAC_URL)
        try:
            from services.data_sources import _fetch_aeropuertos_estatal_from_dgac
            from services.db import save_aeropuertos_estatal_to_db

            por_estado = _fetch_aeropuertos_estatal_from_dgac()
            if por_estado:
                if save_aeropuertos_estatal_to_db(por_estado):
                    print(f"  Aeropuertos estatal: {len(por_estado)} registros -> PostgreSQL")
        except Exception as e:
            print(f"  [WARN] Aeropuertos estatal: {e}")

        # Actividad Hotelera estatal (CETM): archivo local CETM_LOCAL_XLSX o CETM_EXCEL_URL
        try:
            from services.data_sources import load_cetm_actividad_hotelera_todos_estados
            from services.db import save_actividad_hotelera_estatal_to_db

            all_data = load_cetm_actividad_hotelera_todos_estados()
            hotelera_ok = 0
            for codigo, data_by_year in (all_data or {}).items():
                for anio, data in (data_by_year or {}).items():
                    try:
                        if save_actividad_hotelera_estatal_to_db(codigo, data, anio=int(anio)):
                            hotelera_ok += 1
                    except Exception:
                        pass
            if hotelera_ok:
                print(f"  Actividad Hotelera estatal (CETM): {hotelera_ok} estados/años -> PostgreSQL")
        except Exception as e:
            print(f"  [WARN] Actividad Hotelera estatal: {e}")

        # Actividad Hotelera nacional (DataTur) - Base70centros.csv (agregado anual)
        try:
            import requests
            import csv
            import io

            def _safe_float(v):
                if v is None or v == "":
                    return 0.0
                try:
                    s = str(v).strip().replace(",", "").replace(" ", "")
                    return float(s) if s else 0.0
                except (TypeError, ValueError):
                    return 0.0

            url = "https://repodatos.atdt.gob.mx/s_turismo/ocupacion_hotelera/Base70centros.csv"
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()

            text = resp.content.decode("utf-8", errors="replace")
            reader = csv.DictReader(io.StringIO(text), delimiter="\t")

            by_year = {}
            by_year_cat = {}
            for row in reader:
                try:
                    anio = int(row.get("anio") or 0)
                except Exception:
                    continue
                if not anio:
                    continue
                disp = _safe_float(row.get("cuartos_disponibles"))
                occ_nr = _safe_float(row.get("cuartos_ocupados_no_residentes"))
                occ_r = _safe_float(row.get("cuartos_ocupados_residentes"))
                occ = occ_nr + occ_r
                if anio not in by_year:
                    by_year[anio] = {"disp": 0.0, "occ": 0.0}
                by_year[anio]["disp"] += disp
                by_year[anio]["occ"] += occ
                cat = (row.get("categoria") or "Sin categoría").strip() or "Sin categoría"
                key_cat = (anio, cat)
                if key_cat not in by_year_cat:
                    by_year_cat[key_cat] = {"disp": 0.0, "occ": 0.0}
                by_year_cat[key_cat]["disp"] += disp
                by_year_cat[key_cat]["occ"] += occ

            if by_year:
                cur.execute("DELETE FROM actividad_hotelera_nacional")
                for anio in sorted(by_year.keys()):
                    disp = by_year[anio]["disp"]
                    occ = by_year[anio]["occ"]
                    pct = (occ / disp * 100.0) if disp else 0.0
                    cur.execute(
                        """
                        INSERT INTO actividad_hotelera_nacional (anio, cuartos_disponibles_pd, cuartos_ocupados_pd, porc_ocupacion, updated_at)
                        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                        """,
                        (anio, disp, occ, pct),
                    )
                cur.execute("DELETE FROM actividad_hotelera_nacional_por_categoria")
                for (anio, cat) in sorted(by_year_cat.keys()):
                    d = by_year_cat[(anio, cat)]
                    disp, occ = d["disp"], d["occ"]
                    pct = (occ / disp * 100.0) if disp else 0.0
                    cur.execute(
                        """
                        INSERT INTO actividad_hotelera_nacional_por_categoria (anio, categoria, cuartos_disponibles_pd, cuartos_ocupados_pd, porc_ocupacion)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (anio, cat, disp, occ, pct),
                    )
                conn.commit()
                print(f"  Actividad Hotelera nacional (DataTur): {len(by_year)} años, {len(by_year_cat)} año/categoría -> PostgreSQL")
        except Exception as e:
            conn.rollback()
            cur = conn.cursor()
            print(f"  [WARN] Actividad Hotelera nacional (DataTur): {e}")

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

        # ── Cargar datos locales (CSV/JSON) para tablas estatales ──
        import csv as _csv
        import json as _json
        _PROCESS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "process")
        _ESTADO_NAMES = [
            "Aguascalientes","Baja California","Baja California Sur","Campeche",
            "Coahuila de Zaragoza","Colima","Chiapas","Chihuahua",
            "Ciudad de México","Durango","Guanajuato","Guerrero",
            "Hidalgo","Jalisco","México","Michoacán de Ocampo",
            "Morelos","Nayarit","Nuevo León","Oaxaca",
            "Puebla","Querétaro","Quintana Roo","San Luis Potosí",
            "Sinaloa","Sonora","Tabasco","Tamaulipas",
            "Tlaxcala","Veracruz de Ignacio de la Llave","Yucatán","Zacatecas",
        ]
        # estado_info_general
        try:
            cur = conn.cursor()
            for _nom in _ESTADO_NAMES:
                cur.execute("INSERT INTO estado_info_general (estado,poblacion,extension_km2) VALUES (%s,0,0) ON CONFLICT DO NOTHING", (_nom,))
            conn.commit()
        except Exception:
            conn.rollback()
        # pib_estatal
        _path = os.path.join(_PROCESS_DIR, "pib_estatal_consolidado.csv")
        if os.path.exists(_path):
            try:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM pib_estatal")
                if cur.fetchone()[0] == 0:
                    with open(_path, "r", encoding="utf-8") as _f:
                        _reader = _csv.DictReader(_f)
                        _n = 0
                        for _row in _reader:
                            try:
                                cur.execute("INSERT INTO pib_estatal (estado,anio,pib_actual,pib_anterior,variacion_pct) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                                    (_row["Estado"], int(_row["Anio"]), float(_row["PIB_Actual"]), float(_row["PIB_Anterior"]), float(_row["Variacion_Pct"])))
                                _n += 1
                            except Exception:
                                conn.rollback()
                    conn.commit()
                    if _n: print(f"  PIB estatal: {_n} registros cargados")
            except Exception as _e:
                print(f"  [WARN] PIB estatal: {_e}")
                conn.rollback()
        # pib_nacional
        _path = os.path.join(_PROCESS_DIR, "pib_nacional.csv")
        if os.path.exists(_path):
            try:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM pib_nacional")
                if cur.fetchone()[0] == 0:
                    with open(_path, "r", encoding="utf-8") as _f:
                        _reader = _csv.DictReader(_f)
                        _n = 0
                        for _row in _reader:
                            cur.execute("INSERT INTO pib_nacional (fecha,anio,trimestre,pib_total_millones,pib_per_capita) VALUES (%s,%s,%s,%s,%s)",
                                (_row["fecha"], int(_row["anio"]), int(_row["trimestre"]), float(_row["pib_total_millones"]), float(_row["pib_per_capita"])))
                            _n += 1
                    conn.commit()
                    if _n: print(f"  PIB nacional: {_n} registros cargados")
            except Exception as _e:
                print(f"  [WARN] PIB nacional: {_e}")
                conn.rollback()
        # pea_inegi
        _path = os.path.join(_PROCESS_DIR, "pea_inegi.csv")
        if os.path.exists(_path):
            try:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM pea_inegi")
                if cur.fetchone()[0] == 0:
                    with open(_path, "r", encoding="utf-8") as _f:
                        _reader = _csv.DictReader(_f)
                        _n = 0
                        for _row in _reader:
                            cur.execute("INSERT INTO pea_inegi (anio,trimestre,valor) VALUES (%s,%s,%s)",
                                (int(_row["anio"]), int(_row["trimestre"]), int(float(_row["valor"]))))
                            _n += 1
                    conn.commit()
                    if _n: print(f"  PEA INEGI: {_n} registros cargados")
            except Exception as _e:
                print(f"  [WARN] PEA INEGI: {_e}")
                conn.rollback()
        # estructura_poblacional_inegi
        _path = os.path.join(_PROCESS_DIR, "estructura_poblacional_inegi.csv")
        if os.path.exists(_path):
            try:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM estructura_poblacional_inegi")
                if cur.fetchone()[0] == 0:
                    with open(_path, "r", encoding="utf-8") as _f:
                        _reader = _csv.DictReader(_f)
                        _n = 0
                        for _row in _reader:
                            cur.execute("INSERT INTO estructura_poblacional_inegi (year,pob_0_14,pob_15_64,pob_65_plus) VALUES (%s,%s,%s,%s)",
                                (int(float(_row["year"])), int(float(_row["pob_0_14"])), int(float(_row["pob_15_64"])), int(float(_row["pob_65_plus"]))))
                            _n += 1
                    conn.commit()
                    if _n: print(f"  Estructura poblacional: {_n} registros cargados")
            except Exception as _e:
                print(f"  [WARN] Estructura poblacional: {_e}")
                conn.rollback()
        # Demografía estatal (32 JSONs)
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM demografia_estatal_crecimiento")
            if cur.fetchone()[0] == 0:
                _dtotal = 0
                for _i in range(1, 33):
                    _cod = str(_i).zfill(2)
                    _path = os.path.join(_PROCESS_DIR, f"demografia_estatal_{_cod}.json")
                    if not os.path.exists(_path): continue
                    with open(_path, "r", encoding="utf-8") as _f:
                        _data = _json.load(_f)
                    for _row in _data.get("crecimiento", []):
                        try:
                            cur.execute("INSERT INTO demografia_estatal_crecimiento (estado_codigo,anio,valor,crecimiento_pct) VALUES (%s,%s,%s,%s) ON CONFLICT (estado_codigo, anio) DO UPDATE SET valor=EXCLUDED.valor, crecimiento_pct=EXCLUDED.crecimiento_pct",
                                (_cod, _row["anio"], _row["valor"], _row.get("crecimiento_pct")))
                            _dtotal += 1
                        except Exception:
                            conn.rollback()
                    for _row in _data.get("genero", []):
                        try:
                            cur.execute("INSERT INTO demografia_estatal_genero (estado_codigo,anio,hombres,mujeres) VALUES (%s,%s,%s,%s) ON CONFLICT (estado_codigo, anio) DO NOTHING",
                                (_cod, _row["anio"], _row["hombres"], _row["mujeres"]))
                            _dtotal += 1
                        except Exception:
                            conn.rollback()
                    for _row in _data.get("edad", []):
                        try:
                            cur.execute("INSERT INTO demografia_estatal_edad (estado_codigo,anio,g_0_19,g_20_64,g_65_plus,no_especificado) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (estado_codigo, anio) DO NOTHING",
                                (_cod, _row.get("anio",2020), _row.get("g_0_19",0), _row.get("g_20_64",0), _row.get("g_65_plus",0), _row.get("no_especificado",0)))
                            _dtotal += 1
                        except Exception:
                            conn.rollback()
                conn.commit()
                if _dtotal: print(f"  Demografía estatal: {_dtotal} registros cargados")
        except Exception as _e:
            print(f"  [WARN] Demografía estatal: {_e}")
            conn.rollback()

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
