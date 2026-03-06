import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_db_conn():
    # Intenta localhost 5433 (puerto expuesto por Docker)
    return psycopg2.connect(
        host="localhost",
        port=5433,
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "dash_db"),
    )

try:
    conn = get_db_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT count(*) FROM localidades")
    count_loc = cur.fetchone()[0]
    print(f"Total en 'localidades': {count_loc}")
    
    cur.execute("SELECT count(*) FROM distribucion_poblacion_localidad")
    count_dist = cur.fetchone()[0]
    print(f"Total en 'distribucion_poblacion_localidad': {count_dist}")
    
    cur.execute("SELECT count(*) FROM crecimiento_historico_localidad")
    count_crec = cur.fetchone()[0]
    print(f"Total en 'crecimiento_historico_localidad': {count_crec}")
    
    if count_dist > 0:
        cur.execute("SELECT estado_nombre, municipio_nombre, localidad_nombre FROM distribucion_poblacion_localidad LIMIT 5")
        print("\nEjemplos en 'distribucion_poblacion_localidad':")
        for r in cur.fetchall():
            print(f" - {r[0]} | {r[1]} | {r[2]}")
            
except Exception as e:
    print(f"Error: {e}")
finally:
    if 'conn' in locals():
        conn.close()
