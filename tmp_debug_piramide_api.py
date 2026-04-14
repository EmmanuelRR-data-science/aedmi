import requests

base = "http://localhost:8080"
token = requests.post(
    f"{base}/auth/login",
    json={"username": "PhiQus", "password": "357UD105D3M3RC4D0"},
    timeout=30,
).json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

inds = requests.get(
    f"{base}/indicadores?nivel_geografico=municipal&categoria=demografia",
    headers=headers,
    timeout=30,
).json()
idx = {x["clave"]: x["id"] for x in inds}

for clave in ("conapo.municipios_poblacion", "conapo.municipios_piramide_edad"):
    data = requests.get(
        f"{base}/indicadores/{idx[clave]}/datos",
        headers=headers,
        timeout=120,
    ).json()["datos"]
    print(clave, "rows", len(data))
    for row in data[:5]:
        print(" ", row["entidad_clave"], row["periodo"], row["valor"])

