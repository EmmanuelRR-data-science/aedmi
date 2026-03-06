"""
Script para diagnosticar qué datos se están leyendo para Hidalgo.
Uso: python scripts/debug_hidalgo.py "6_2_Sectur.xlsx"
"""
import os
import sys

def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if len(sys.argv) >= 2:
        path = sys.argv[1]
    else:
        path = os.path.join(base, "6_2_Sectur.xlsx")
    if not os.path.isfile(path):
        print(f"No se encontró: {path}")
        return
    
    try:
        import pandas as pd
    except ImportError:
        print("Necesita pandas: pip install pandas openpyxl")
        return
    
    # Cargar como lo hace el código
    engine = "openpyxl" if path.lower().endswith(".xlsx") else None
    xls = pd.ExcelFile(path, engine=engine)
    
    v05 = pd.read_excel(xls, "Vista06a", header=12)  # Vista06a = ocupados
    print(f"Vista06a (Ocupados): {len(v05)} filas, {len(v05.columns)} columnas")
    
    # Detectar columna de estados (igual que el código)
    def normalizar(s):
        import unicodedata
        return ''.join(c for c in unicodedata.normalize('NFD', str(s).lower()) 
                       if unicodedata.category(c) != 'Mn')
    
    STATE_ID_TO_NAME = {
        1: "Aguascalientes", 2: "Baja California", 3: "Baja California Sur", 4: "Campeche",
        5: "Coahuila de Zaragoza", 6: "Colima", 7: "Chiapas", 8: "Chihuahua",
        9: "Ciudad de México", 10: "Durango", 11: "Guanajuato", 12: "Guerrero",
        13: "Hidalgo", 14: "Jalisco", 15: "México", 16: "Michoacán de Ocampo",
        17: "Morelos", 18: "Nayarit", 19: "Nuevo León", 20: "Oaxaca",
        21: "Puebla", 22: "Querétaro", 23: "Quintana Roo", 24: "San Luis Potosí",
        25: "Sinaloa", 26: "Sonora", 27: "Tabasco", 28: "Tamaulipas",
        29: "Tlaxcala", 30: "Veracruz de Ignacio de la Llave", 31: "Yucatán", 32: "Zacatecas",
    }
    
    estado_norms = {normalizar(n) for n in STATE_ID_TO_NAME.values()}
    best_col = v05.columns[0]
    best_count = 0
    for col in list(v05.columns)[:15]:
        try:
            vals = v05[col].dropna().astype(str).str.strip()
            if vals.empty:
                continue
            normalized = vals.apply(normalizar)
            def matches_any(n):
                if n in estado_norms:
                    return True
                for e in estado_norms:
                    if e.startswith(n) or n.startswith(e) or n in e or e in n:
                        return True
                return False
            matches = sum(1 for n in normalized if len(n) >= 3 and matches_any(n))
            if matches > best_count:
                best_count = matches
                best_col = col
        except Exception:
            continue
    
    print(f"\nColumna de estados detectada: '{best_col}' (índice {list(v05.columns).index(best_col)})")
    print(f"Coincidencias encontradas: {best_count}")
    
    # Buscar Hidalgo
    hidalgo_norm = normalizar("Hidalgo")
    normalized = v05[best_col].astype(str).apply(normalizar)
    hidalgo_rows = v05[normalized == hidalgo_norm]
    if hidalgo_rows.empty:
        print("\n[!] Hidalgo no encontrado con coincidencia exacta. Buscando parcial...")
        for idx, row_norm in normalized.items():
            if hidalgo_norm == row_norm or hidalgo_norm.startswith(row_norm) or row_norm.startswith(hidalgo_norm):
                hidalgo_rows = v05.loc[[idx]]
                print(f"Encontrado en fila {idx}: '{v05[best_col].iloc[idx]}'")
                break
    
    if hidalgo_rows.empty:
        print("\n[ERROR] No se encontro Hidalgo")
        print("Primeras 10 filas de la columna detectada:")
        print(v05[best_col].head(10))
        return
    
    hidalgo_row = hidalgo_rows.iloc[0]
    print(f"\n[OK] Hidalgo encontrado en fila {hidalgo_rows.index[0]}")
    
    # Obtener columnas de meses (igual que el código)
    cols_bracket = [c for c in v05.columns if str(c).strip().startswith("[")]
    print(f"\nColumnas que empiezan con '[': {len(cols_bracket)}")
    print(f"Primeras 5: {cols_bracket[:5]}")
    print(f"Últimas 5: {cols_bracket[-5:]}")
    
    if len(cols_bracket) >= 12:
        month_cols = list(cols_bracket)[-12:]
        # Ordenar por número de mes (como hace el código ahora)
        import re
        def get_month_num(col_name):
            match = re.search(r'\[(\d+)\]', str(col_name))
            if match:
                return int(match.group(1))
            return 0
        month_cols_sorted = sorted(month_cols, key=get_month_num)
        print(f"\nÚltimas 12 columnas (sin ordenar):")
        for i, col in enumerate(month_cols):
            val = hidalgo_row[col]
            mes_nombre = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"][i]
            print(f"  {mes_nombre} ({col}): {val}")
        print(f"\nÚltimas 12 columnas (ORDENADAS por número de mes):")
        for i, col in enumerate(month_cols_sorted):
            val = hidalgo_row[col]
            mes_nombre = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"][i]
            print(f"  {mes_nombre} ({col}): {val}")
        
        print(f"\n[INFO] Valor de Enero (ocupados): {hidalgo_row[month_cols[0]]}")
        print(f"       (Esperado segun usuario: 113,801)")
        
        # Buscar si hay otras columnas de Enero que tengan el valor esperado
        print(f"\nBuscando columnas de Enero con valor cercano a 113,801...")
        enero_cols = [c for c in cols_bracket if '[01]' in str(c) or 'Ene' in str(c)]
        for col in enero_cols[-10:]:  # Últimas 10 columnas de Enero
            val = hidalgo_row[col]
            if abs(val - 113801) < 1000:  # Dentro de 1000 unidades
                print(f"  Encontrado: {col} = {val}")
    else:
        print("[!] No hay suficientes columnas con '['")

if __name__ == "__main__":
    main()
