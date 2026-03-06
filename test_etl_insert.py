import os
import sys
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from services.db import save_distribucion_poblacion_localidad_bulk, save_localidades_to_db

def test_insert():
    print("Iniciando prueba de inserción...")
    test_loc = [
        {
            "estado_codigo": "01",
            "estado_nombre": "Aguascalientes",
            "municipio_codigo": "001",
            "municipio_nombre": "Aguascalientes",
            "loc_codigo": "0001",
            "localidad_nombre": "Aguascalientes"
        }
    ]
    test_dist = [
        {
            "estado_codigo": "01",
            "estado_nombre": "Aguascalientes",
            "municipio_codigo": "001",
            "municipio_nombre": "Aguascalientes",
            "loc_codigo": "0001",
            "localidad_nombre": "Aguascalientes",
            "POBTOT": 1000,
            "POBFEM": 510,
            "POBMAS": 490,
            "P_0A4_F": 50,
            "P_0A4_M": 48
            # ... otros campos opcionales
        }
    ]
    
    n1 = save_localidades_to_db(test_loc)
    print(f"Localidades insertadas: {n1}")
    
    n2 = save_distribucion_poblacion_localidad_bulk(test_dist)
    print(f"Distribución insertada: {n2}")

if __name__ == "__main__":
    test_insert()
