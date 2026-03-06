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

    """Aplica schema.sql y migraciones para crear/actualizar tablas."""

    etl_dir = os.path.dirname(__file__)

    schema_path = os.path.join(etl_dir, "schema.sql")

    if os.path.isfile(schema_path):

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

                    if stmt.upper().startswith("CREATE TABLE") or stmt.upper().startswith("CREATE INDEX"):

                        try:

                            cur.execute(stmt)

                        except Exception:

                            pass

            finally:

                conn.autocommit = False

            cur.close()

        except Exception as e:

            if conn:

                conn.rollback()

                conn.autocommit = False

            print(f"  [WARN] Aplicando schema: {e}")



    migrations_dir = os.path.join(etl_dir, "migrations")

    if os.path.isdir(migrations_dir):

        for fname in sorted(os.listdir(migrations_dir)):

            if fname.endswith(".sql"):

                path = os.path.join(migrations_dir, fname)

                try:

                    with open(path, encoding="utf-8") as f:

                        sql = f.read()

                    cur = conn.cursor()

                    try:

                        conn.autocommit = True

                        for stmt in sql.split(";"):

                            stmt = stmt.strip()

                            if not stmt or stmt.startswith("--"):

                                continue

                            try:

                                cur.execute(stmt)

                            except Exception as ex:

                                if "already exists" not in str(ex).lower() and "duplicate" not in str(ex).lower():

                                    print(f"  [WARN] Migración {fname}: {ex}")

                    finally:

                        conn.autocommit = False

                    cur.close()

                except Exception as e:

                    print(f"  [WARN] Migración {fname}: {e}")





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



        # PIB Estatal (INEGI PIBE) e Info General (estado_poblacion_pib.ipynb)

        try:

            from services.data_sources import (

                _fetch_and_process_pib_estatal_inegi,

                POBLACION_ESTADO_2020,

                SURFACE_AREA_KM2

            )

            from services.db import save_pib_estatal_to_db, save_estado_info_general_to_db



            # Info Geográfica y Poblacional

            estados_info_upserted = 0

            for est, pob in POBLACION_ESTADO_2020.items():

                ext = SURFACE_AREA_KM2.get(est, 0)

                if save_estado_info_general_to_db(est, pob, ext):

                    estados_info_upserted += 1

            print(f"  Info general (Poblacion, km2): {estados_info_upserted} estados guardados")



            pib_estatal = _fetch_and_process_pib_estatal_inegi()

            if pib_estatal:

                if save_pib_estatal_to_db(pib_estatal):

                    print(f"  PIB estatal (INEGI): {len(pib_estatal)} registros guardados en BD")

                else:

                    print("  [WARN] Falló el guardado de PIB estatal en BD")

        except Exception as e:

            print(f"  [WARN] PIB estatal INEGI e Info general: {e}")



        # Demografía estatal (INEGI - estado_crecimiento_hist.ipynb): precarga cache JSON y escribe en PostgreSQL

        try:

            from services.data_sources import get_demografia_estatal, STATE_ID_TO_NAME

            from services.db import save_demografia_estatal_to_db



            demografia_ok = 0

            for _sid, nombre in STATE_ID_TO_NAME.items():

                try:

                    d = get_demografia_estatal(nombre)

                    if d and (d.get("crecimiento") or d.get("genero") or d.get("edad")):

                        codigo = str(_sid).zfill(2)

                        if save_demografia_estatal_to_db(codigo, d):

                            demografia_ok += 1

                except Exception:

                    pass

            if demografia_ok:

                print(f"  Demografía estatal (INEGI): {demografia_ok} estados -> PostgreSQL")

        except Exception as e:

            print(f"  [WARN] Demografía estatal INEGI: {e}")



        # Proyecciones CONAPO (estado_proyeccion.ipynb): descarga CSV si no existe y escribe en PostgreSQL

        try:

            from services.data_sources import _download_conapo_proyecciones_csv, get_proyecciones_conapo, STATE_ID_TO_NAME

            from services.db import save_proyecciones_conapo_to_db

            csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "process", "proyecciones_conapo.csv")
            if not os.path.isfile(csv_path) or os.path.getsize(csv_path) < 500:
                _download_conapo_proyecciones_csv()

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



        # ITAEE estatal (estado_pib_sectores.ipynb): API INEGI y escribe en PostgreSQL

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



        # Actividad Hotelera estatal (CETM SECTUR - 6_2.xlsx, estado_turismo_llegadas.ipynb)

        # Fuente: 1) Si CETM_LOCAL_XLSX está definido y el archivo existe, se usa ese Excel.

        #        2) Si no, se descarga el ZIP desde la página del Compendio (CETM2024/CETM2025).

        # Ejemplo local: CETM_LOCAL_XLSX=C:\...\CETM2024\6_2.xlsx

        try:

            from services.data_sources import (

                load_cetm_actividad_hotelera_todos_estados,

                process_actividad_hotelera_from_upload,

            )

            from services.db import save_actividad_hotelera_estatal_to_db



            all_data = None

            local_path = os.getenv("CETM_LOCAL_XLSX", "").strip()

            if local_path and os.path.isfile(local_path):

                data_by_estado, err = process_actividad_hotelera_from_upload(local_path)

                if not err and data_by_estado:

                    all_data = data_by_estado

                    print(f"  Actividad Hotelera: usando archivo local {local_path}")

            if all_data is None:

                all_data = load_cetm_actividad_hotelera_todos_estados()



            hotelera_ok = 0

            for codigo, data_by_year in (all_data or {}).items():

                for anio, data in (data_by_year or {}).items():

                    try:

                        if save_actividad_hotelera_estatal_to_db(codigo, data, anio=anio):

                            hotelera_ok += 1

                    except Exception:

                        pass

            if hotelera_ok:

                print(f"  Actividad Hotelera estatal (CETM): {hotelera_ok} estados/años -> PostgreSQL")

        except Exception as e:

            print(f"  [WARN] Actividad Hotelera estatal CETM: {e}")



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



        # Exportaciones por Estado (DataMéxico API, sin token)

        try:

            from services.data_sources import _get_exportaciones_por_estado_from_api

            from services.db import get_exportaciones_estatal_from_db, save_exportaciones_estatal_to_db



            # Verificar si ya hay datos en BD

            db_data = get_exportaciones_estatal_from_db()

            if not db_data:

                # Si no hay datos, obtener desde API y guardar

                api_data = _get_exportaciones_por_estado_from_api()

                if api_data:

                    if save_exportaciones_estatal_to_db(api_data):

                        print(f"  Exportaciones por Estado: {len(api_data)} registros -> PostgreSQL")

                    else:

                        print(f"  [WARN] Exportaciones por Estado: error al guardar")

                else:

                    print(f"  [WARN] Exportaciones por Estado: no se pudieron obtener datos de la API")

            else:

                print(f"  Exportaciones por Estado: {len(db_data)} registros ya en PostgreSQL")

        except Exception as e:

            print(f"  [WARN] Exportaciones por Estado: {e}")



        # Aeropuertos por Estado (DGAC Excel desde CUADRO_DGAC_URL)

        try:

            from services.data_sources import _fetch_aeropuertos_estatal_from_dgac

            from services.db import get_aeropuertos_estatal_from_db, save_aeropuertos_estatal_to_db

            db_data = get_aeropuertos_estatal_from_db("01")

            if not db_data:

                por_estado = _fetch_aeropuertos_estatal_from_dgac()

                if por_estado:

                    if save_aeropuertos_estatal_to_db(por_estado):

                        print(f"  Aeropuertos por Estado: {len(por_estado)} registros -> PostgreSQL")

                    else:

                        print(f"  [WARN] Aeropuertos por Estado: error al guardar")

                else:

                    print(f"  [WARN] Aeropuertos por Estado: no se pudieron obtener datos del Excel DGAC")

            else:

                total_regs = sum(len(get_aeropuertos_estatal_from_db(str(c).zfill(2))) for c in range(1, 33))

                print(f"  Aeropuertos por Estado: {total_regs} registros ya en PostgreSQL")

        except Exception as e:

            print(f"  [WARN] Aeropuertos por Estado: {e}")



        # Municipios, Distribución Municipal, Localidades y Distribución por Localidad (INEGI Censo 2020) — una sola descarga

        try:


            from services.db import (

                get_municipios_from_db,

                get_distribucion_poblacion_municipal_from_db,

                get_localidades_from_db,

                save_municipios_to_db,

                save_distribucion_poblacion_municipal_bulk,

                save_localidades_to_db,

                save_distribucion_poblacion_localidad_bulk,

            )



            municipios_en_bd = get_municipios_from_db("Aguascalientes")

            tiene_distribucion = get_distribucion_poblacion_municipal_from_db("Aguascalientes", "Aguascalientes") is not None

            tiene_localidades = get_localidades_from_db("Aguascalientes")



            if not municipios_en_bd or len(municipios_en_bd) == 0 or not tiene_distribucion or not tiene_localidades:


                if list_municipios:

                    if not municipios_en_bd or len(municipios_en_bd) == 0:

                        if save_municipios_to_db(list_municipios):

                            print(f"  Municipios: {len(list_municipios)} registros -> PostgreSQL")

                        else:

                            print(f"  [WARN] Municipios: error al guardar")

                    else:

                        print(f"  Municipios: ya en BD")

                if list_distribucion and not tiene_distribucion:

                    n = save_distribucion_poblacion_municipal_bulk(list_distribucion)

                    print(f"  Distribución poblacional municipal: {n} registros -> PostgreSQL")

                if list_localidades:

                    if save_localidades_to_db(list_localidades):

                        print(f"  Localidades: {len(list_localidades)} registros -> PostgreSQL")

                if list_distribucion_localidad:

                    n_loc = save_distribucion_poblacion_localidad_bulk(list_distribucion_localidad)

                    print(f"  Distribución poblacional localidad: {n_loc} registros -> PostgreSQL")

            else:

                total_municipios = 0

                for estado_nom in ["Aguascalientes", "Jalisco", "México", "Veracruz"]:

                    munis = get_municipios_from_db(estado_nom)

                    total_municipios += len(munis) if munis else 0

                print(f"  Municipios y distribución: ya en PostgreSQL ({total_municipios}+ municipios)")

        except Exception as e:

            print(f"  [WARN] Municipios / Distribución / Localidades: {e}")

            import traceback

            traceback.print_exc()



        # Proyección Poblacional Municipal (CONAPO) — una descarga, todos los municipios en BD

        try:


            from services.db import get_municipios_from_db, save_proyeccion_poblacional_municipal_to_db, get_proyeccion_poblacional_municipal_from_db



            test_data = get_proyeccion_poblacional_municipal_from_db("Hidalgo", "Pachuca")

            if test_data:

                print(f"  Proyección poblacional municipal: datos ya en PostgreSQL")

            else:

                list_munis = []

                for _sid, estado_nombre in STATE_ID_TO_NAME.items():

                    munis = get_municipios_from_db(estado_nombre)

                    if munis:

                        ec = str(_sid).zfill(2)

                        for m in munis:

                            list_munis.append({

                                "estado_codigo": ec,

                                "estado_nombre": estado_nombre,

                                "municipio_codigo": str(m.get("codigo", "")).strip().zfill(3),

                                "municipio_nombre": str(m.get("nombre", "")).strip(),

                            })

                if list_munis:


                    if proy:

                        if save_proyeccion_poblacional_municipal_to_db(proy):

                            print(f"  Proyección poblacional municipal: {len(proy)} registros -> PostgreSQL")

                        else:

                            print(f"  [WARN] Proyección poblacional municipal: error al guardar")

                    else:

                        print(f"  [WARN] Proyección poblacional municipal: sin datos CONAPO")

                else:

                    print(f"  Proyección poblacional municipal: no hay municipios en BD, ejecute antes el paso de municipios")

        except Exception as e:

            print(f"  [WARN] Proyección poblacional municipal: {e}")

            import traceback

            traceback.print_exc()



        # Crecimiento histórico por localidad (2005, 2010, 2020) — INEGI, bulk

        try:


            from services.db import get_all_distribucion_localidad_para_crecimiento, save_crecimiento_historico_localidad_bulk, get_crecimiento_historico_localidad_from_db



            if get_crecimiento_historico_localidad_from_db("Aguascalientes", "Aguascalientes", "Aguascalientes"):

                print(f"  Crecimiento histórico localidad: datos ya en PostgreSQL")

            else:

                lista_loc = get_all_distribucion_localidad_para_crecimiento(limit=50000)

                if lista_loc:

                    print(f"  Crecimiento histórico localidad: procesando {len(lista_loc)} localidades...")


                    if registros:

                        n_crec = save_crecimiento_historico_localidad_bulk(registros)

                        print(f"  Crecimiento histórico localidad: {n_crec} registros -> PostgreSQL")

                    else:

                        print(f"  [WARN] Crecimiento histórico localidad: sin datos INEGI")

                else:

                    print(f"  Crecimiento histórico localidad: no hay localidades en BD")

        except Exception as e:

            print(f"  [WARN] Crecimiento histórico localidad: {e}")

            import traceback

            traceback.print_exc()



        # Ciudades (menú fijo): Mérida, Querétaro, CDMX, Monterrey, Guadalajara, Tijuana, Cancún

        try:

            cur.execute("SELECT COUNT(*) FROM ciudades")

            n_ciudades = cur.fetchone()[0]

            if n_ciudades == 0:

                ciudades_seed = [

                    ("merida", "Mérida", "31", "Yucatán", "050", "Mérida", False),

                    ("queretaro", "Querétaro", "22", "Querétaro", "014", "Querétaro", False),

                    ("cdmx", "CDMX", "09", "Ciudad de México", None, None, True),

                    ("monterrey", "Monterrey", "19", "Nuevo León", "039", "Monterrey", False),

                    ("guadalajara", "Guadalajara", "14", "Jalisco", "039", "Guadalajara", False),

                    ("tijuana", "Tijuana", "02", "Baja California", "004", "Tijuana", False),

                    ("cancun", "Cancún", "23", "Quintana Roo", "005", "Benito Juárez", False),

                ]

                for slug, nombre, ec, en, mc, mn, ent_completa in ciudades_seed:

                    cur.execute(

                        """INSERT INTO ciudades (slug, nombre, estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, es_entidad_completa)

                           VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (slug) DO NOTHING""",

                        (slug, nombre, ec, en, mc, mn, ent_completa),

                    )

                conn.commit()

                print(f"  Ciudades: {len(ciudades_seed)} registros -> PostgreSQL")

            else:

                print(f"  Ciudades: ya en PostgreSQL ({n_ciudades} registros)")

        except Exception as e:

            conn.rollback()

            cur = conn.cursor()

            print(f"  [WARN] Ciudades: {e}")



        # Crecimiento histórico MUNICIPAL (2005, 2010, 2020) — INEGI ITER LOC=0

        try:


            from services.db import save_crecimiento_historico_municipal_bulk



            cur.execute("SELECT COUNT(*) FROM crecimiento_historico_municipal")

            n_crec_mun = cur.fetchone()[0]

            if n_crec_mun < 1000:

                print(f"  Crecimiento histórico municipal: cargando desde INEGI...")


                if registros_mun:

                    n_m = save_crecimiento_historico_municipal_bulk(registros_mun)

                    print(f"  Crecimiento histórico municipal: {n_m} registros -> PostgreSQL")

                else:

                    print(f"  [WARN] Crecimiento histórico municipal: sin datos INEGI")

            else:

                print(f"  Crecimiento histórico municipal: ya en PostgreSQL ({n_crec_mun} registros)")

        except Exception as e:

            print(f"  [WARN] Crecimiento histórico municipal: {e}")

            import traceback

            traceback.print_exc()



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





        # Poblacion Ocupada Turismo - Observatur (Mérida/Yuc)

        try:

            from services.data_sources import _scrape_poblacion_ocupada_observatur

            from services.db import save_poblacion_ocupada_turismo_bulk

            cur.execute("SELECT COUNT(*) FROM poblacion_ocupada_turismo_merida")

            n_poocup = cur.fetchone()[0]

            if n_poocup < 10:

                print("  Poblacion Ocupada Observatur: extrayendo datos web...")

                registros_pooc = _scrape_poblacion_ocupada_observatur()

                if registros_pooc:

                    n_inserted = save_poblacion_ocupada_turismo_bulk(registros_pooc)

                    print(f"  Poblacion Ocupada Observatur: {n_inserted} registros -> PostgreSQL")

                else:

                    print(f"  [WARN] Poblacion Ocupada Observatur: sin datos")

            else:

                print(f"  Poblacion Ocupada Observatur: ya en PostgreSQL ({n_poocup} registros)")

        except Exception as e:

            print(f"  [WARN] Poblacion Ocupada Observatur: {e}")

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

