"""
Inspecciona el Excel de producto aeropuertos para ver hojas, columnas y primeras filas.
Uso: python scripts/inspect_producto_aeropuertos.py "C:\...\producto-aeropuertos-2006-2025-nov-29122025.xlsx"
"""
import sys
import os

def main():
    if len(sys.argv) < 2:
        path = os.path.join(os.environ.get("USERPROFILE", ""), "Downloads", "producto-aeropuertos-2006-2025-nov-29122025.xlsx")
        print(f"Uso: python scripts/inspect_producto_aeropuertos.py <ruta.xlsx>")
        print(f"Intentando: {path}")
    else:
        path = sys.argv[1]
    if not os.path.isfile(path):
        print(f"Archivo no encontrado: {path}")
        return
    try:
        import pandas as pd
        xls = pd.ExcelFile(path, engine="openpyxl")
        print(f"Hojas: {xls.sheet_names}")
        for sheet in xls.sheet_names:
            print(f"\n--- Hoja: {sheet} ---")
            for header in (0, 1, 2):
                df = pd.read_excel(xls, sheet_name=sheet, header=header)
                print(f"  header={header} -> columnas: {list(df.columns)[:15]}")
                print(f"  filas: {len(df)}, primeras 3:")
                print(df.head(3).to_string())
                if len(df) > 0:
                    break
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
