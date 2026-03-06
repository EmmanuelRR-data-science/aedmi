import sys
import pandas as pd
import os

# Add project root to path
sys.path.append(r"c:\Users\EmmanuelRamírez\OneDrive - PhiQus\Escritorio\EDMI-APP-VPS")

from services.data_sources import _load_cetm_excel_sheets, _process_actividad_hotelera_dfs, _parse_year_month_headers_multilabel

file_path = r"C:\Users\EmmanuelRamírez\Downloads\CETM2024\CETM2024\6_2.xlsx"
print(f"Analyzing file: {file_path}")

try:
    dfs = _load_cetm_excel_sheets(file_path)
    v05 = dfs["Vista05"]
    
    # 1. Inspect the Header Rows raw
    print("\n--- Raw Header Rows (10-15) ---")
    raw_df = pd.read_excel(file_path, "Vista05", header=None, nrows=20)
    for i in range(10, 15):
        row_vals = raw_df.iloc[i].tolist()
        # Print first 20 cols to see structure
        print(f"Row {i}: {row_vals[:20]}...")

    # 2. Test the parsing logic specifically
    print("\n--- Testing _parse_year_month_headers_multilabel ---")
    if hasattr(v05, "attrs") and "cols_by_year" in v05.attrs:
        mapping = v05.attrs["cols_by_year"]
        print(f"Detected Years: {sorted(list(mapping.keys()))}")
        for y in sorted(list(mapping.keys())):
            cols = mapping[y]
            print(f"Year {y}: Cols {cols[0]}-{cols[-1]} (Count: {len(cols)})")
            # Print value at first column of data for Hidalgo to confirm
            # Find Hidalgo row index
            # We need to find the row for Hidalgo in the raw_df or v05
            # v05 has headers set, so checks might be offset. Use raw_df for absolute check
            
            # Find "Hidalgo" row in raw_df
            for r_idx in range(len(raw_df)):
                if "Hidalgo" in str(raw_df.iloc[r_idx, 0]):
                    val = raw_df.iloc[r_idx, cols[0]] # Jan value
                    print(f"  -> Hidalgo Jan {y} Value (at Row {r_idx}, Col {cols[0]}): {val}")

    # 3. Process specifically for Hidalgo
    print("\n--- Processing for Hidalgo (13) ---")
    processed = _process_actividad_hotelera_dfs(dfs)
    if '13' in processed:
        data = processed['13']
        print(f"Years found for Hidalgo: {list(data.keys())}")
        for y, d in data.items():
            print(f"Year {y}: Jan Occupied: {d.get('ocupados', [])[0] if 'ocupados' in d else 'N/A'}")
    else:
        print("Hidalgo (13) not found in processed data")

except Exception as e:
    import traceback
    traceback.print_exc()
