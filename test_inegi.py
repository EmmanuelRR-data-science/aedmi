import pandas as pd
import io
import requests
import zipfile
import csv

fuentes = [
    {"anio": 2005, "url": "https://www.inegi.org.mx/contenidos/programas/ccpv/2005/datosabiertos/cpv2005_iter_00_csv.zip",
     "csv_pattern": "cpv2005_iter_00.csv", "encoding": "latin-1", "cols": ("p_total", "p_mas", "p_fem")},
    {"anio": 2010, "url": "https://www.inegi.org.mx/contenidos/programas/ccpv/2010/datosabiertos/iter_nal_2010_csv.zip",
     "csv_pattern": "iter_00_cpv2010.csv", "encoding": "latin-1", "cols": ("pobtot", "pobmas", "pobfem")},
]

for fuente in fuentes:
    print(f"--- ANIO {fuente['anio']} ---")
    response = requests.get(fuente["url"], timeout=120)
    raw = response.content
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        target = next((n for n in z.namelist() if fuente["csv_pattern"] in n and n.endswith(".csv")), None)
        with z.open(target) as f:
            raw = f.read()
    
    try:
        text = raw.decode(fuente["encoding"])
    except Exception:
        text = raw.decode("utf-8", errors="replace")
    
    text = text.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
    df = pd.read_csv(io.StringIO(text), skipinitialspace=True, dtype=str, engine="python", on_bad_lines="skip", quoting=csv.QUOTE_NONE)
    df.columns = [c.replace("\ufeff", "").replace("ï»¿", "").replace("\"", "").strip() for c in df.columns]

    print("Initial rows:", len(df))
    
    if "LOC" in df.columns:
        df["LOC"] = pd.to_numeric(df["LOC"], errors="coerce").fillna(-1).astype(int)
        df = df[df["LOC"] == 0]
        print("Rows after LOC=0:", len(df))
    else:
        print("Missed LOC col. Columns available:", df.columns.tolist()[:10])
        
    for col in ["CVE_ENT", "CVE_MUN", "NOM_ENT", "NOM_MUN"]:
        if col not in df.columns and col.lower() in [c.lower() for c in df.columns]:
            for c in df.columns:
                if c.upper() == col:
                    df[col] = df[c]
                    break

    if "CVE_ENT" not in df.columns and "entidad" in [c.lower() for c in df.columns]:
        df["CVE_ENT"] = df[[c for c in df.columns if c.lower() == "entidad"][0]]
    if "CVE_MUN" not in df.columns and "mun" in [c.lower() for c in df.columns]:
        df["CVE_MUN"] = df[[c for c in df.columns if c.lower() == "mun"][0]]

    has_ent = "CVE_ENT" in df.columns
    has_mun = "CVE_MUN" in df.columns
    print(f"Has CVE_ENT? {has_ent}, Has CVE_MUN? {has_mun}")
    
    if not (has_ent and has_mun):
        print("Missing CVE_ENT/CVE_MUN. Available columns:", df.columns.tolist())
    else:
        # Check first row values
        print("Sample CVE_ENT:", df["CVE_ENT"].head().tolist())
        print("Sample CVE_MUN:", df["CVE_MUN"].head().tolist())
    
    col_total, col_h, col_m = fuente["cols"]
    col_map = {c.lower(): c for c in df.columns}
    
    c_tot = col_map.get(col_total.lower())
    c_h = col_map.get(col_h.lower())
    c_m = col_map.get(col_m.lower())
    
    print(f"Col mappings: {col_total}->{c_tot}, {col_h}->{c_h}, {col_m}->{c_m}")
