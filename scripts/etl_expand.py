"""
ETL Expansion: Loads all local CSV/JSON data files into the 19 empty PostgreSQL tables.
Run once: python scripts/etl_expand.py
"""
import os
import sys
import csv
import json

import psycopg2
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()


def _get_db_host():
    import socket
    try:
        socket.getaddrinfo("db", 5432)
        return "db"
    except socket.gaierror:
        return "localhost"


def get_conn():
    return psycopg2.connect(
        host=_get_db_host(),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "dash_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


ESTADO_NAMES = [
    "Aguascalientes", "Baja California", "Baja California Sur", "Campeche",
    "Coahuila de Zaragoza", "Colima", "Chiapas", "Chihuahua",
    "Ciudad de México", "Durango", "Guanajuato", "Guerrero",
    "Hidalgo", "Jalisco", "México", "Michoacán de Ocampo",
    "Morelos", "Nayarit", "Nuevo León", "Oaxaca",
    "Puebla", "Querétaro", "Quintana Roo", "San Luis Potosí",
    "Sinaloa", "Sonora", "Tabasco", "Tamaulipas",
    "Tlaxcala", "Veracruz de Ignacio de la Llave", "Yucatán", "Zacatecas",
]

PROCESS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "process")

def run():
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()
    total = 0

    # ── 1. estado_info_general (needed for PIB estatal FK) ──
    print("  Cargando estado_info_general...")
    for nombre in ESTADO_NAMES:
        cur.execute(
            """INSERT INTO estado_info_general (estado, poblacion, extension_km2)
               VALUES (%s, 0, 0) ON CONFLICT (estado) DO NOTHING""",
            (nombre,),
        )
    conn.commit()
    print(f"    32 estados cargados")
    total += 32

    # ── 2. PIB Estatal ──
    path = os.path.join(PROCESS_DIR, "pib_estatal_consolidado.csv")
    if os.path.exists(path):
        print("  Cargando pib_estatal...")
        n = 0
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                estado = row.get("Estado", "").strip()
                anio = row.get("Anio", "0")
                pib_actual = row.get("PIB_Actual", "0")
                pib_anterior = row.get("PIB_Anterior", "0")
                variacion = row.get("Variacion_Pct", "0")
                try:
                    cur.execute(
                        """INSERT INTO pib_estatal (estado, anio, pib_actual, pib_anterior, variacion_pct)
                           VALUES (%s, %s, %s, %s, %s)
                           ON CONFLICT (estado, anio) DO UPDATE SET
                               pib_actual = EXCLUDED.pib_actual,
                               pib_anterior = EXCLUDED.pib_anterior,
                               variacion_pct = EXCLUDED.variacion_pct""",
                        (estado, int(anio), float(pib_actual), float(pib_anterior), float(variacion)),
                    )
                    n += 1
                except Exception as e:
                    print(f"    [WARN] Fila PIB estatal omitida ({estado}/{anio}): {e}")
                    conn.rollback()
                    continue
        conn.commit()
        print(f"    {n} registros cargados")
        total += n

    # ── 3. PIB Nacional ──
    path = os.path.join(PROCESS_DIR, "pib_nacional.csv")
    if os.path.exists(path):
        print("  Cargando pib_nacional...")
        n = 0
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    cur.execute(
                        """INSERT INTO pib_nacional (fecha, anio, trimestre, pib_total_millones, pib_per_capita)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (row["fecha"], int(row["anio"]), int(row["trimestre"]),
                         float(row["pib_total_millones"]), float(row["pib_per_capita"])),
                    )
                    n += 1
                except Exception:
                    conn.rollback()
        conn.commit()
        print(f"    {n} registros cargados")
        total += n

    # ── 4. PEA INEGI ──
    path = os.path.join(PROCESS_DIR, "pea_inegi.csv")
    if os.path.exists(path):
        print("  Cargando pea_inegi...")
        n = 0
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    cur.execute(
                        """INSERT INTO pea_inegi (anio, trimestre, valor) VALUES (%s, %s, %s)""",
                        (int(row["anio"]), int(row["trimestre"]), int(float(row["valor"]))),
                    )
                    n += 1
                except Exception:
                    conn.rollback()
        conn.commit()
        print(f"    {n} registros cargados")
        total += n

    # ── 5. Estructura Poblacional INEGI ──
    path = os.path.join(PROCESS_DIR, "estructura_poblacional_inegi.csv")
    if os.path.exists(path):
        print("  Cargando estructura_poblacional_inegi...")
        n = 0
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    cur.execute(
                        """INSERT INTO estructura_poblacional_inegi (year, pob_0_14, pob_15_64, pob_65_plus)
                           VALUES (%s, %s, %s, %s)""",
                        (int(float(row["year"])), int(float(row["pob_0_14"])),
                         int(float(row["pob_15_64"])), int(float(row["pob_65_plus"]))),
                    )
                    n += 1
                except Exception:
                    conn.rollback()
        conn.commit()
        print(f"    {n} registros cargados")
        total += n

    # ── 6. Demografía Estatal (32 archivos JSON) ──
    print("  Cargando demografía estatal (32 estados)...")
    demo_total = 0
    for i in range(1, 33):
        codigo = str(i).zfill(2)
        path = os.path.join(PROCESS_DIR, f"demografia_estatal_{codigo}.json")
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Crecimiento
            for row in data.get("crecimiento", []):
                try:
                    cur.execute(
                        """INSERT INTO demografia_estatal_crecimiento (estado_codigo, anio, valor, crecimiento_pct)
                           VALUES (%s, %s, %s, %s)
                           ON CONFLICT (estado_codigo, anio) DO UPDATE SET
                               valor = EXCLUDED.valor, crecimiento_pct = EXCLUDED.crecimiento_pct""",
                        (codigo, row["anio"], row["valor"], row.get("crecimiento_pct")),
                    )
                    demo_total += 1
                except Exception:
                    conn.rollback()

            # Género
            for row in data.get("genero", []):
                try:
                    cur.execute(
                        """INSERT INTO demografia_estatal_genero (estado_codigo, anio, hombres, mujeres)
                           VALUES (%s, %s, %s, %s)
                           ON CONFLICT DO NOTHING""",
                        (codigo, row["anio"], row["hombres"], row["mujeres"]),
                    )
                    demo_total += 1
                except Exception:
                    conn.rollback()

            # Edad
            for row in data.get("edad", []):
                try:
                    cur.execute(
                        """INSERT INTO demografia_estatal_edad (estado_codigo, anio, grupo_edad, valor)
                           VALUES (%s, %s, %s, %s)
                           ON CONFLICT DO NOTHING""",
                        (codigo, row.get("anio", 2020), row.get("grupo", row.get("grupo_edad", "")),
                         row.get("valor", 0)),
                    )
                    demo_total += 1
                except Exception:
                    conn.rollback()
        except Exception as e:
            print(f"    [WARN] Error demografía {codigo}: {e}")

    conn.commit()
    print(f"    {demo_total} registros demográficos cargados")
    total += demo_total

    cur.close()
    conn.close()
    print(f"\nETL Expansión completado: {total} registros totales cargados")


if __name__ == "__main__":
    run()
