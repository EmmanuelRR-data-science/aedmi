# Respaldo de base de datos para VPS

Aquí vive el **artefacto versionado** que permite levantar producción con datos ya cargados (sin esperar un ETL completo al arranque).

## Formato recomendado (`aedmi-data.sql.gz`)

- Es un `pg_dump` **schema+datos** (sin owner/ACL), comprimido con gzip.
- Se genera con `--clean --if-exists`, por lo que al restaurar reemplaza objetos existentes sin depender de coincidencia exacta con el `init.sql` del repo.
- Generación (desde la raíz del repo, con el stack de desarrollo levantado):

  ```bash
  bash scripts/create_vps_db_snapshot.sh
  ```

  En Windows (PowerShell), con Docker Desktop:

  ```powershell
  .\scripts\create_vps_db_snapshot.ps1
  ```

## Formato alternativo (`aedmi_db_dump.zip`)

- ZIP que contiene un único `.sql` en texto plano (p. ej. el que ya se usa en `db/backup/`).
- El script de restauración lo acepta si no hay `.sql.gz`.

## Restaurar en el VPS (producción)

1. Copia el repo y crea `.env.prod` a partir de `.env.prod.example`.
2. **Importante:** indica al servicio ETL que no dispare la carga masiva al arrancar si ya restauraste datos:

   ```env
   ETL_RUN_ON_START=0
   ```

   (El scheduler interno sigue activo para ejecuciones programadas.)

3. Arranca solo la base para aplicar `init.sql` en volumen vacío:

   ```bash
   docker compose --env-file .env.prod -f docker-compose.prod.yml up -d db
   ```

   Espera a que `pg_isready` esté OK (healthcheck).

4. Ejecuta:

   ```bash
   bash scripts/restore_vps_db_snapshot.sh
   ```

   El script importa el dump comprimido (o el ZIP legacy) en la base configurada en `.env.prod`.

5. Levanta el resto del stack:

   ```bash
   docker compose --env-file .env.prod -f docker-compose.prod.yml up -d
   ```

## Actualizar el respaldo en Git

Tras generar un nuevo `aedmi-data.sql.gz` (o tras actualizar el ZIP), añade y comite solo `db/vps-snapshot/*` y los scripts relacionados; **no** subas `.env`, `.env.prod` ni secretos.
