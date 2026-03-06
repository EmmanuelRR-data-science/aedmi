import os

code = '''
def _scrape_poblacion_ocupada_observatur():
    import requests
    import logging
    from bs4 import BeautifulSoup
    url = "https://www.observaturyucatan.org.mx/indicadores"
    try:
        response = requests.get(url, timeout=60, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.find_all("tr", class_="rw_mid_poocupada")
        data = []
        for r in rows:
            yr = r.get("data-yr")
            mnth = r.get("data-mnth")
            vl = r.get("data-vl")
            if yr and mnth and vl:
                try:
                    poblacion = int(float(vl.replace(",", "")))
                    data.append({
                        "anio": int(yr),
                        "trimestre": int(mnth),
                        "poblacion_ocupada": poblacion
                    })
                except ValueError:
                    continue
        return data
    except Exception as e:
        print(f"Error scraping Observatur: {e}")
        return []
'''

with open('services/data_sources.py', 'a', encoding='utf-8') as f:
    f.write('\\n' + code)
print("Appended successfully.")
