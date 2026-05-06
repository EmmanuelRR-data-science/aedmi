#!/usr/bin/env bash
# Genera db/vps-snapshot/aedmi-data.sql.gz (schema+datos) usando el contenedor db del compose de desarrollo.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f ".env" ]]; then
  echo "ERROR: Falta .env en la raíz (POSTGRES_USER, POSTGRES_DB)."
  exit 1
fi

# shellcheck disable=SC1091
set -a
source .env
set +a

OUT_DIR="${ROOT_DIR}/db/vps-snapshot"
mkdir -p "${OUT_DIR}"
OUT_FILE="${OUT_DIR}/aedmi-data.sql.gz"

echo "Volcando schema+datos a ${OUT_FILE} (docker compose, servicio db)..."
docker compose exec -T db \
  pg_dump -U "${POSTGRES_USER}" "${POSTGRES_DB}" \
  --no-owner --no-acl --clean --if-exists | gzip > "${OUT_FILE}"

echo "Listo. Revisa tamaño: $(du -h "${OUT_FILE}" | cut -f1)"
echo "Añade el archivo a git: git add db/vps-snapshot/aedmi-data.sql.gz"
