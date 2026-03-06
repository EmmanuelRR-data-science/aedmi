import sys
import pandas as pd
import numpy as np

file_path = r"C:\Users\EmmanuelRamírez\Downloads\CETM2024\CETM2024\6_2.xlsx"
print(f"Deep inspecting: {file_path}")

try:
    # Read Row 11 only, all columns
    # Pandas 1-indexed header? No, 0-indexed. Row 11 is index 11.
    df = pd.read_excel(file_path, "Vista05", header=None)
    
    # 1. Scan Row 11 for "2032"
    row_11 = df.iloc[11].tolist()
    print(f"Row 11 length: {len(row_11)}")
    
    found_2032 = False
    for i, val in enumerate(row_11):
        try:
            val_flt = float(val)
            if int(val_flt) == 2032:
                print(f"FOUND 2032 at Column index {i}! Value: {val}")
                found_2032 = True
                
                # Check surrounding columns in Row 11
                print(f"  Surrounding Row 11 (indices {i-2}-{i+2}): {row_11[i-2:i+3]}")
                
                # Check Row 12 (Months) below it
                row_12 = df.iloc[12].tolist()
                print(f"  Below in Row 12 (indices {i-2}-{i+2}): {row_12[i-2:i+3]}")
                
        except (ValueError, TypeError):
            continue

    if not found_2032:
        print("DID NOT find 2032 in Row 11.")

    # 2. Check for any year > 2024
    print("\nScanning for ANY year > 2024:")
    for i, val in enumerate(row_11):
        try:
            val_flt = float(val)
            if val_flt > 2024 and val_flt < 2100:
                print(f"  Found {val_flt} at Column {i}")
        except:
            pass

except Exception as e:
    print(f"Error: {e}")
