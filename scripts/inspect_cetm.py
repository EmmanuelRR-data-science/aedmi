"""
Script para inspeccionar un Excel CETM (6_2_Sectur.xlsx).
Uso: python scripts/inspect_cetm.py [ruta/al/archivo.xlsx]
Si no se pasa ruta, busca 6_2_Sectur.xlsx en el directorio del proyecto y en data/raw.
"""
import os
import sys

def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if len(sys.argv) >= 2:
        path = sys.argv[1]
    else:
        for name in ("6_2_Sectur.xlsx", "6_2_Sectur.xls", "data/raw/6_2_Sectur.xlsx", "data/raw/CETM_6_2.xlsx"):
            p = os.path.join(base, name)
            if os.path.isfile(p):
                path = p
                break
        else:
            print("No se encontró 6_2_Sectur.xlsx. Uso: python scripts/inspect_cetm.py <ruta>")
            return
    print("Archivo:", path)
    try:
        import pandas as pd
    except ImportError:
        print("Necesita pandas: pip install pandas openpyxl")
        return
    engine = "openpyxl" if path.lower().endswith(".xlsx") else None
    xls = pd.ExcelFile(path, engine=engine)
    print("Hojas:", xls.sheet_names)
    # Inspeccionar las 3 hojas que usa actividad hotelera (CETM)
    for sh in ("Vista05", "Vista06a", "Vista09a"):
        if sh not in xls.sheet_names:
            print("\n--- Hoja no encontrada:", sh)
            continue
        print("\n--- Hoja:", sh, "---")
        for header_row in (12, 11, 10, 9, 8, 0):
            try:
                df = pd.read_excel(xls, sh, header=header_row)
                print(f"  header={header_row}: cols={len(df.columns)}, filas={len(df)}")
                if len(df.columns) > 0:
                    first_col = df.iloc[:, 0].dropna().astype(str)
                    sample = list(first_col.head(8))
                    print("  Primera columna (muestra):", sample)
                cols_bracket = [c for c in df.columns if str(c).strip().startswith("[")]
                if cols_bracket:
                    print("  Columnas que empiezan con '[':", len(cols_bracket), "-> últimas 12:", list(cols_bracket[-12:]) if len(cols_bracket) >= 12 else cols_bracket[-12:])
                if len(df.columns) >= 13:
                    print("  Últimas 12 columnas (nombres):", list(df.columns[-12:]))
                break
            except Exception as e:
                print(f"  header={header_row}: error {e}")
    print("\nListo.")

if __name__ == "__main__":
    main()
