import os
import sys

# Asegurar path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.db import db_connection

def run_migration():
    print("Applying migration...")
    with open("etl/migrations/002_geo_economico.sql", "r") as f:
        sql = f.read()
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    print("Migration applied!")

def test_pib_etl():
    print("Running PIB ETL...")
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

def test_api():
    print("\nTesting get_geo_economico_from_db...")
    from services.db import get_geo_economico_from_db
    res = get_geo_economico_from_db("Aguascalientes")
    print("Aguascalientes =>", res)

if __name__ == "__main__":
    run_migration()
    test_pib_etl()
    test_api()
