import os

code_db = '''

def save_poblacion_ocupada_turismo_bulk(lista_data: list[dict]) -> int:
    if not lista_data:
        return 0
    saved = 0
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            for row in lista_data:
                anio = row.get("anio")
                trimestre = row.get("trimestre")
                poblacion_ocupada = row.get("poblacion_ocupada")
                if not anio or not trimestre or poblacion_ocupada is None:
                    continue
                cur.execute(
                    """INSERT INTO poblacion_ocupada_turismo_merida (anio, trimestre, poblacion_ocupada)
                       VALUES (%s, %s, %s)
                       ON CONFLICT (anio, trimestre) DO UPDATE SET
                       poblacion_ocupada = EXCLUDED.poblacion_ocupada""",
                    (anio, trimestre, poblacion_ocupada)
                )
                saved += 1
            conn.commit()
            return saved
    except Exception as e:
        print(f"Error guardando poblacion_ocupada_turismo_merida: {e}")
        import traceback
        traceback.print_exc()
        return 0

def get_poblacion_ocupada_turismo_merida():
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT anio, trimestre, poblacion_ocupada FROM poblacion_ocupada_turismo_merida ORDER BY anio ASC, trimestre ASC")
            res = cur.fetchall()
            return [{"anio": r[0], "trimestre": r[1], "poblacion_ocupada": r[2]} for r in res]
    except Exception as e:
        print(f"Error extrayendo poblacion_ocupada_turismo_merida: {e}")
        return []
'''

with open('services/db.py', 'a', encoding='utf-8') as f:
    f.write('\\n' + code_db)


code_run = '''
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
'''

# We need to insert code_run into etl/run.py before the conn.close() / finally block.
with open('etl/run.py', 'r', encoding='utf-8') as f:
    text = f.read()

# find "except Exception as e:\n        logging.error(f"Error en script de carga de Excel a DB: {e}")"
insert_pos = text.rfind("finally:")
if insert_pos == -1:
    print("Could not find finally block in run.py")
else:
    # insert before the exception catch of the main block
    try_end_pos = text.rfind("    except Exception as e:", 0, text.rfind("finally:"))
    if try_end_pos != -1:
        new_text = text[:try_end_pos] + code_run + '\\n' + text[try_end_pos:]
        with open('etl/run.py', 'w', encoding='utf-8') as f:
            f.write(new_text)
        print("Run script updated successfully.")
    else:
        print("Could not find try block end.")
