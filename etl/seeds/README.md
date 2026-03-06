# Seeds para PostgreSQL

Los archivos `.sql` en esta carpeta se cargan **solo la primera vez** que se crea la base de datos (al levantar los contenedores con un volumen `postgres_data` nuevo).

## Actividad Hotelera estatal (CETM)

Para que la gráfica "Actividad Hotelera (DataTur)" tenga datos al levantar contenedores:

1. Genera el archivo de seed con tu Excel del Compendio (6_2.xlsx):
   ```bash
   python scripts/export_actividad_hotelera_to_sql.py "C:\Users\EmmanuelRamírez\Downloads\CETM2024\CETM2024\6_2.xlsx"
   ```
   Eso crea/sobrescribe `etl/seeds/actividad_hotelera_estatal.sql` con los INSERTs de los 32 estados.

2. Para que el seed se cargue en la base:
   - **Si es la primera vez** (o quieres recrear la BD):  
     `docker-compose down -v` y luego `docker-compose up -d`. El script `02-actividad-hotelera-estatal.sql` se ejecuta en el arranque.
   - **Si ya tienes la BD creada**: ejecuta el SQL a mano o importa el archivo en tu cliente PostgreSQL contra la base `dash_db`.

Tras eso, la API leerá de `actividad_hotelera_estatal` y las gráficas se pintarán sin subir el Excel.
