#!/usr/bin/env bash
# Restaura db/vps-snapshot/aedmi-data.sql.gz o aedmi_db_dump.zip en el contenedor db (compose produccion).
# El snapshot recomendado incluye schema+datos y sentencias --clean --if-exists.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f ".env.prod" ]]; then
  echo "ERROR: Falta .env.prod"
  exit 1
fi

# shellcheck disable=SC1091
set -a
source .env.prod
set +a

COMPOSE=(docker compose --env-file .env.prod -f docker-compose.prod.yml)
SNAP_DIR="${ROOT_DIR}/db/vps-snapshot"
GZIP_FILE="${SNAP_DIR}/aedmi-data.sql.gz"
ZIP_FILE="${SNAP_DIR}/aedmi_db_dump.zip"

echo "Esperando salud del servicio db..."
for _ in $(seq 1 30); do
  if "${COMPOSE[@]}" exec -T db pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" &>/dev/null; then
    break
  fi
  sleep 2
done

if [[ -f "${GZIP_FILE}" ]]; then
  echo "Restaurando desde ${GZIP_FILE} (schema+datos, --clean --if-exists)..."
  gunzip -c "${GZIP_FILE}" | "${COMPOSE[@]}" exec -T db \
    psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -v ON_ERROR_STOP=1
elif [[ -f "${ZIP_FILE}" ]]; then
  echo "Restaurando desde ${ZIP_FILE}..."
  SQL_NAME="$(unzip -Z1 "${ZIP_FILE}" | grep -E '\.sql$' | head -1)"
  if [[ -z "${SQL_NAME}" ]]; then
    echo "ERROR: El ZIP no contiene un .sql"
    exit 1
  fi
  unzip -p "${ZIP_FILE}" "${SQL_NAME}" | "${COMPOSE[@]}" exec -T db \
    psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -v ON_ERROR_STOP=1
else
  echo "ERROR: No se encontro ${GZIP_FILE} ni ${ZIP_FILE}"
  exit 1
fi

echo "Restore completado."
