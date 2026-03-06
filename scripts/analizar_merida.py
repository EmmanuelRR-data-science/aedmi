# -*- coding: utf-8 -*-
"""
Analiza en PostgreSQL que datos existen para la ciudad de Merida (Yucatan)
y si permiten obtener: poblacion total, crecimiento poblacional anual,
distribucion por sexo y distribucion por edad.
Ejecutar desde la raiz del proyecto: python scripts/analizar_merida.py
"""
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    try:
        from services.db import db_connection
    except Exception as e:
        print(f"No se pudo importar services.db: {e}")
        return 1

    print("=" * 70)
    print("ANALISIS DE DATOS EN POSTGRESQL PARA MERIDA (YUCATAN)")
    print("=" * 70)

    with db_connection() as conn:
        cur = conn.cursor()

        # Contar tablas
        cur.execute("SELECT COUNT(*) FROM municipios")
        n_mun = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM distribucion_poblacion_municipal")
        n_dpm = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM localidades")
        n_loc = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM distribucion_poblacion_localidad")
        n_dpl = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM crecimiento_historico_localidad")
        n_crec = cur.fetchone()[0]
        print("\nRegistros en tablas: municipios={}, dist_municipal={}, localidades={}, dist_localidad={}, crecimiento_hist={}".format(
            n_mun, n_dpm, n_loc, n_dpl, n_crec))

        # Estados que contengan 'yucatan' o codigo 31 (Yucatan INEGI = 31)
        cur.execute("SELECT DISTINCT estado_codigo, estado_nombre FROM municipios WHERE estado_codigo = '31' OR estado_nombre ILIKE '%yucatan%' OR estado_nombre ILIKE '%yucatán%' LIMIT 5")
        estados = cur.fetchall()
        print("\nEstados Yucatan (cod 31 o nombre):", estados if estados else "ninguno")

        # Municipios en estado 31
        cur.execute("SELECT municipio_nombre FROM municipios WHERE estado_codigo = '31' ORDER BY municipio_nombre LIMIT 20")
        muns_31 = cur.fetchall()
        print("Municipios en estado_codigo=31:", [r[0] for r in muns_31] if muns_31 else "ninguno")

        # Distribucion municipal estado 31, municipio Merida (nombre puede variar)
        cur.execute("""
            SELECT estado_nombre, municipio_nombre, pobtot, pobfem, pobmas,
                   CASE WHEN data_json IS NOT NULL AND length(data_json) > 10 THEN 'SI' ELSE 'NO' END as tiene_edad
            FROM distribucion_poblacion_municipal
            WHERE estado_codigo = '31' AND (municipio_nombre ILIKE '%merida%' OR municipio_nombre ILIKE '%mérida%')
        """)
        row = cur.fetchone()
        print("\nDistribucion poblacion MUNICIPAL (estado 31, municipio Merida):", row if row else "no hay")

        # Proyeccion municipal Merida
        cur.execute("""
            SELECT COUNT(*), MIN(anio), MAX(anio)
            FROM proyeccion_poblacional_municipal
            WHERE estado_codigo = '31' AND municipio_nombre ILIKE '%merida%'
        """)
        proy = cur.fetchone()
        print("Proyeccion municipal Merida (count, min_anio, max_anio):", proy if proy else "no hay")

        # Localidades Merida en estado 31
        cur.execute("""
            SELECT localidad_nombre, loc_codigo FROM localidades
            WHERE estado_codigo = '31' AND municipio_nombre ILIKE '%merida%' AND localidad_nombre ILIKE '%merida%'
            LIMIT 5
        """)
        locs = cur.fetchall()
        print("Localidades 'Merida' en mun Merida:", locs if locs else "ninguna")

        # Distribucion localidad
        cur.execute("""
            SELECT localidad_nombre, pobtot, pobfem, pobmas,
                   CASE WHEN data_json IS NOT NULL AND length(data_json) > 10 THEN 'SI' ELSE 'NO' END
            FROM distribucion_poblacion_localidad
            WHERE estado_codigo = '31' AND municipio_nombre ILIKE '%merida%' AND localidad_nombre ILIKE '%merida%'
            LIMIT 1
        """)
        dloc = cur.fetchone()
        print("Distribucion localidad Merida:", dloc if dloc else "no hay")

        # Crecimiento historico localidad
        cur.execute("""
            SELECT anio, poblacion, hombres, mujeres
            FROM crecimiento_historico_localidad
            WHERE estado_codigo = '31' AND municipio_nombre ILIKE '%merida%' AND localidad_nombre ILIKE '%merida%'
            ORDER BY anio
        """)
        hist = cur.fetchall()
        print("Crecimiento historico localidad Merida:", hist if hist else "no hay")

    print("\n" + "=" * 70)
    print("CONCLUSION")
    print("=" * 70)
    if n_mun == 0 and n_dpm == 0:
        print("La BD no tiene datos de municipios ni distribucion municipal.")
        print("Ejecuta: python etl/run.py  para poblar con INEGI/CONAPO.")
    else:
        tiene_mun = row is not None
        tiene_proy = proy and proy[0] and proy[0] > 0
        tiene_dist_loc = dloc is not None
        tiene_hist = len(hist) > 0
        print("""
Para la ciudad de Merida (Yucatan):

 a) MUNICIPIO MERIDA:
   - Poblacion total: {}  (distribucion_poblacion_municipal)
   - Distribucion por sexo (hombres/mujeres): {}
   - Distribucion por edad (grupos en data_json): {}
   - Crecimiento historico 2005/2010/2020: No existe tabla a nivel municipio.
   - Proyeccion por ano y sexo: {}  (proyeccion_poblacional_municipal)

 b) LOCALIDAD MERIDA (cabecera):
   - Poblacion total: {}  (distribucion_poblacion_localidad)
   - Distribucion por sexo: {}
   - Distribucion por edad (data_json): {}
   - Crecimiento poblacional anual (2005, 2010, 2020): {}  (crecimiento_historico_localidad)
""".format(
            "SI" if tiene_mun else "NO",
            "SI" if tiene_mun else "NO",
            "SI" if (tiene_mun and row and row[5] == 'SI') else "NO",
            "SI" if tiene_proy else "NO",
            "SI" if tiene_dist_loc else "NO",
            "SI" if tiene_dist_loc else "NO",
            "SI" if (tiene_dist_loc and dloc and dloc[4] == 'SI') else "NO",
            "SI" if tiene_hist else "NO",
        ))
    return 0

if __name__ == "__main__":
    sys.exit(main())
