import os
import sys
import requests
import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from services.db import db_connection

def test_db():
    print("Probando conexión a DB...")
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            print("OK: Conectado a DB")
    except Exception as e:
        print(f"Error DB: {e}")

def test_inegi_header():
    url = "https://www.inegi.org.mx/contenidos/programas/ccpv/2020/datosabiertos/iter/iter_00_cpv2020_csv.zip"
    print(f"Probando conexión a INEGI: {url}")
    try:
        r = requests.head(url, timeout=10)
        print(f"Status: {r.status_code}")
        print(f"Size: {r.headers.get('Content-Length')} bytes")
    except Exception as e:
        print(f"Error INEGI: {e}")

if __name__ == "__main__":
    test_db()
    test_inegi_header()
