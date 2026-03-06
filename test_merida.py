import sys
sys.path.append('.')
from services.db import db_connection

with db_connection() as conn:
    cur = conn.cursor()
    cur.execute("SELECT anio, poblacion, hombres, mujeres FROM crecimiento_historico_municipal WHERE estado_codigo='31' AND municipio_codigo='050' ORDER BY anio")
    res = cur.fetchall()
    print("Mérida (31-050):")
    for r in res:
        print(r)
