import tempfile
import os
import sys
import pandas as pd

# Add project root to path
sys.path.append(r"c:\Users\EmmanuelRamírez\OneDrive - PhiQus\Escritorio\EDMI-APP-VPS")

from services.data_sources import _load_cetm_excel_sheets

source_file = r"C:\Users\EmmanuelRamírez\Downloads\CETM2024\CETM2024\6_2.xlsx"

print("--- Reproduction Test: Open FD with mkstemp ---")
fd, path = tempfile.mkstemp(suffix=".xlsx")
try:
    print(f"Temp file created at: {path}")
    print(f"FD: {fd}")
    
    # Write content to it so it's a valid excel file
    with open(source_file, "rb") as src:
        data = src.read()
        os.write(fd, data)
        # Note: We do NOT close fd here, simulating the app.py state where fd is open
        # during process_actividad_hotelera_from_upload calling _load_cetm_excel_sheets

    print("Attempting to load via _load_cetm_excel_sheets while FD is open...")
    try:
        # This calls pd.read_excel(path) internally
        dfs = _load_cetm_excel_sheets(path)
        if dfs:
            print("SUCCESS: Loaded despite open FD.")
        else:
            print("FAILURE: Returned None (likely caught exception).")
    except Exception as e:
        print(f"FAILURE: Exception raised: {e}")

finally:
    try:
        os.close(fd)
    except OSError:
        pass
    try:
        os.unlink(path)
    except OSError:
        pass
