# RFC: Respaldo versionado de base de datos y restauración reproducible en VPS

- **Author(s):** Emmanuel Ramírez Romero
- **Status:** Propuesta
- **Última actualización:** 2026-05-06

## Contenido

- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Background](#background)
- [Overview](#overview)
- [Detailed Design](#detailed-design)
- [Consideraciones](#consideraciones)
- [Métricas](#métricas)

## Links

- `db/vps-snapshot/aedmi-data.sql.gz`
- `scripts/create_vps_db_snapshot.sh`
- `scripts/create_vps_db_snapshot.ps1`
- `scripts/restore_vps_db_snapshot.sh`
- `docker-compose.prod.yml`
- `.env.prod.example`

## Objetivo

Versionar un **backup actualizado** de PostgreSQL y asegurar una **restauración reproducible** durante el despliegue en un VPS, evitando esperas por un ETL completo al arranque y reduciendo riesgos de inconsistencias entre entornos.

## Goals

- Tener un artefacto versionado `db/vps-snapshot/aedmi-data.sql.gz` que incluya **schema + datos**.
- Proveer scripts de generación y restauración para:
  - Desarrollo (Windows y Linux/macOS) para generar snapshot.
  - Producción (VPS) para restaurar snapshot usando `docker compose` y `.env.prod`.
- Asegurar que el dump sea portable entre entornos:
  - Sin ownership/ACL (`--no-owner --no-acl`).
  - Reemplazo idempotente (`--clean --if-exists`).
- Mantener el repositorio limpio y consistente (evitar artefactos de tooling que rompan Git o ensucien el status).

## Non-Goals

- No reemplaza una estrategia formal de migraciones (p. ej. Alembic) ni la define.
- No resuelve la orquestación completa de despliegue (CI/CD, rollback de aplicación).
- No garantiza compatibilidad entre **versiones mayores** de PostgreSQL distintas a la usada por el stack (`postgres:16`).
- No cubre políticas de retención ni cifrado del dump en Git; es un artefacto versionado dentro del repo (ver consideraciones de seguridad).

## Background

El proyecto usa PostgreSQL dentro de Docker Compose (`postgres:16`) para servir API/ETL/Frontend. Para producción en VPS se requiere:

- Levantar la base con `init.sql` en un volumen vacío.
- Cargar una base ya poblada para que el sistema esté operativo sin ejecutar todo el ETL al arranque.

En paralelo, se detectó que el repo podía ensuciarse con metadatos locales de herramientas (p. ej. directorios internos), afectando la confiabilidad de los commits.

## Overview

Se establece una convención:

- **Artefacto canónico:** `db/vps-snapshot/aedmi-data.sql.gz`
  - Dump SQL (`pg_dump`) con schema+datos.
  - Generado desde el contenedor `db` del compose de desarrollo.
  - Restore mediante `psql` hacia el contenedor `db` del compose de producción.

Flujo recomendado:

1. En desarrollo, levantar `db` y generar snapshot.
2. Comitear el snapshot actualizado.
3. En VPS, levantar solo `db` para inicializar y luego restaurar el snapshot.
4. Levantar API/ETL/Frontend.
5. Configurar `ETL_RUN_ON_START=0` si ya existe data restaurada.

## Detailed Design

### Artefacto: `aedmi-data.sql.gz`

Se genera con:

- `pg_dump -U $POSTGRES_USER $POSTGRES_DB --no-owner --no-acl --clean --if-exists | gzip`

Justificación de flags:

- `--no-owner --no-acl`: evita problemas por roles/usuarios distintos entre dev y VPS.
- `--clean --if-exists`: hace que la restauración sea tolerante a preexistencia de objetos.

### Generación del snapshot

Opciones soportadas:

- Linux/macOS: `scripts/create_vps_db_snapshot.sh`
- Windows/PowerShell: `scripts/create_vps_db_snapshot.ps1`

Contrato de entrada:

- Requiere `.env` con `POSTGRES_USER` y `POSTGRES_DB` (y el servicio `db` disponible).

Salida:

- `db/vps-snapshot/aedmi-data.sql.gz`

### Restauración del snapshot en VPS (producción)

Script: `scripts/restore_vps_db_snapshot.sh`

Contrato de entrada:

- `.env.prod` presente (basado en `.env.prod.example`).
- Stack de producción levantado al menos con `db`:
  - `docker compose --env-file .env.prod -f docker-compose.prod.yml up -d db`
- El script espera healthcheck vía `pg_isready`.

Restore:

- Preferente: `db/vps-snapshot/aedmi-data.sql.gz`
- Fallback (legacy): `db/vps-snapshot/aedmi_db_dump.zip` con un `.sql` interno.

### Interacción con ETL al arranque

Para evitar carga masiva después de restaurar snapshot:

- Setear `ETL_RUN_ON_START=0` en `.env.prod`

## Consideraciones

### Seguridad

- El dump contiene **datos**; validar que no incluya secretos ni información sensible antes de versionarlo.
- No versionar `.env` ni `.env.prod` (solo usar `.env*.example`).

### Tamaño del repositorio

- El archivo `aedmi-data.sql.gz` crece con el volumen de datos.
- Se recomienda monitorear el tamaño y considerar alternativas si supera límites razonables de Git remoto (p. ej. LFS o storage externo).

### Consistencia y limpieza del repo

- Directorios/tooling locales no deben entrar en commits para evitar ruido y errores de Git.

## Métricas

### Métricas técnicas

- Tamaño del snapshot (`db/vps-snapshot/aedmi-data.sql.gz`) en bytes.
- Tiempo de generación del snapshot en desarrollo.
- Tiempo de restore en VPS (desde “db healthy” hasta fin del `psql`).
- Éxito de healthchecks post-restore:
  - `db` (pg_isready)
  - `api` (`/health`)
  - `frontend` (HTTP 200)

### Métricas operativas

- Tiempo total de despliegue (sin ETL masivo).
- Incidencias por incompatibilidad de roles/ACL (esperado: 0 con `--no-owner --no-acl`).
