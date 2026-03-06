from services.data_sources import _load_cetm_excel_sheets
import pandas as pd

# Mock CETM_SHEETS to include Vista07a for testing logic if needed, 
# but _load_cetm_excel_sheets currently hardcodes CETM_SHEETS.
# We need to manually load it to inspect structure first.

file_path = r"C:\Users\EmmanuelRamírez\Downloads\CETM2024\CETM2024\6_2.xlsx"
print(f"Inspecting Vista07a in: {file_path}")

try:
    xls = pd.ExcelFile(file_path, engine="openpyxl")
    print("Sheets found:", xls.sheet_names)
    
    # Find Vista07a
    target = None
    for s in xls.sheet_names:
        if "07a" in s.lower() or "vista07a" in s.lower().replace(" ", ""):
            target = s
            break
            
    if target:
        print(f"Found target sheet: {target}")
        # Read without header to inspect rows by index
        df = pd.read_excel(xls, target, header=None, nrows=20)
        print("--- Rows 0-19 ---")
        for i in range(20):
            row_vals = df.iloc[i].tolist()
            # print only first 15 cols to avoid huge output
            print(f"Row {i}: {row_vals[:15]}")
    else:
        print("Vista07a not found.")

except Exception as e:
    import traceback
    traceback.print_exc()
