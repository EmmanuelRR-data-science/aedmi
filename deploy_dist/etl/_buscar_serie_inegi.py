"""Busca la serie correcta del PIB trimestral en el BIE de INEGI."""

import os
import sys

import httpx

sys.path.insert(0, ".")
token = os.environ["INEGI_TOKEN"]

# Probar con la URL de búsqueda del BIE
search_url = (
    f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/"
    f"jsonxml/INDICATOR/628194,628195,628196/es/00/false/BIE/{token}?type=json"
)

# Primero verificar que el token funciona con una serie conocida (poblacion total)
test_url = (
    f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/"
    f"jsonxml/INDICATOR/1002000001/es/00/false/BISE/{token}?type=json"
)
r = httpx.get(test_url, timeout=15)
print(f"Test poblacion: HTTP {r.status_code}, len={len(r.content)}")
if r.status_code == 200 and r.content:
    data = r.json()
    series = data.get("Series", [])
    if series:
        obs = series[0].get("OBSERVATIONS", [])
        print(f"  Nombre: {series[0].get('INDICADOR', '?')}")
        print(f"  Obs: {len(obs)}, ultimo: {obs[-1] if obs else None}")

# Ahora buscar PIB en BIE con claves del SCNM (Sistema de Cuentas Nacionales)
# La clave correcta del PIB trimestral nominal en BIE es conocida como 493911 en BISE
# Probar con BISE en lugar de BIE
for clave in ["493911", "493912", "493913"]:
    url = (
        f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/"
        f"jsonxml/INDICATOR/{clave}/es/00/false/BISE/{token}?type=json"
    )
    r = httpx.get(url, timeout=15)
    print(f"BISE {clave}: HTTP {r.status_code}, len={len(r.content)}")
    if r.status_code == 200 and r.content:
        try:
            data = r.json()
            series = data.get("Series", [])
            if series:
                obs = series[0].get("OBSERVATIONS", [])
                print(f"  Nombre: {series[0].get('INDICADOR', '?')[:80]}")
                print(f"  Obs: {len(obs)}, ultimos: {obs[-3:]}")
        except Exception as e:
            print(f"  JSON error: {e}")
