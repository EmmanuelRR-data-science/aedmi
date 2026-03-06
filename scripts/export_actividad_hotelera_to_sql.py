"""
Genera etl/seeds/actividad_hotelera_estatal.sql a partir del Excel CETM (6_2.xlsx).
Ejecutar una vez con la ruta al archivo para poblar la base al levantar contenedores.

Uso:
  python scripts/export_actividad_hotelera_to_sql.py "C:\\...\\CETM2024\\6_2.xlsx"
"""
import os
import sys

def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if len(sys.argv) < 2:
        print("Uso: python scripts/export_actividad_hotelera_to_sql.py <ruta a 6_2.xlsx>")
        sys.exit(1)
    xlsx_path = os.path.abspath(sys.argv[1])
    if not os.path.isfile(xlsx_path):
        print(f"No se encontró el archivo: {xlsx_path}")
        sys.exit(1)

    sys.path.insert(0, base)
    from services.data_sources import process_actividad_hotelera_from_upload

    print(f"Procesando {xlsx_path}...")
    data_by_estado, err = process_actividad_hotelera_from_upload(xlsx_path)
    if err:
        print(f"Error: {err}")
        sys.exit(1)
    if not data_by_estado:
        print("No se extrajeron datos de estados.")
        sys.exit(1)

    seeds_dir = os.path.join(base, "etl", "seeds")
    os.makedirs(seeds_dir, exist_ok=True)
    out_path = os.path.join(seeds_dir, "actividad_hotelera_estatal.sql")

    lines = [
        "-- Seed: Actividad Hotelera por estado y año (CETM SECTUR). Generado por scripts/export_actividad_hotelera_to_sql.py",
        "-- Se carga en docker-entrypoint-initdb.d al crear la base por primera vez.",
        "",
        "DELETE FROM actividad_hotelera_estatal;",
        "",
    ]
    count = 0
    for codigo in sorted(data_by_estado.keys()):
        data_by_year = data_by_estado[codigo] or {}
        for anio, data in data_by_year.items():
            disp = data.get("disponibles") or [0] * 12
            ocup = data.get("ocupados") or [0] * 12
            porc = data.get("porc_ocupacion") or [0] * 12
            for mes in range(12):
                d = disp[mes] if mes < len(disp) else 0
                o = ocup[mes] if mes < len(ocup) else 0
                p = porc[mes] if mes < len(porc) else 0
                if isinstance(d, float) and (d != d):
                    d = 0
                if isinstance(o, float) and (o != o):
                    o = 0
                if isinstance(p, float) and (p != p):
                    p = 0
                lines.append(
                    f"INSERT INTO actividad_hotelera_estatal (estado_codigo, anio, mes_num, disponibles, ocupados, porc_ocupacion) "
                    f"VALUES ('{codigo}', {int(anio)}, {mes + 1}, {float(d)}, {float(o)}, {float(p)}) "
                    f"ON CONFLICT (estado_codigo, anio, mes_num) DO UPDATE SET "
                    f"disponibles = EXCLUDED.disponibles, ocupados = EXCLUDED.ocupados, porc_ocupacion = EXCLUDED.porc_ocupacion;"
                )
                count += 1

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Generado: {out_path} ({len(data_by_estado)} estados, {count} filas)")
    print("Reinicia los contenedores (o borra el volumen postgres_data y vuelve a levantar) para cargar el seed.")

if __name__ == "__main__":
    main()
