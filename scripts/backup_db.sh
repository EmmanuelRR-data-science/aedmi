#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f ".env.prod" ]]; then
  echo "ERROR: Falta .env.prod."
  exit 1
fi

# shellcheck disable=SC1091
source .env.prod

BACKUP_DIR="${BACKUP_DIR:-./backups}"
mkdir -p "$BACKUP_DIR"

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_FILE="${BACKUP_DIR}/aedmi-${STAMP}.sql.gz"

echo "Generando backup en ${OUT_FILE}..."
docker compose --env-file .env.prod -f docker-compose.prod.yml exec -T db \
  pg_dump -U "${POSTGRES_USER}" "${POSTGRES_DB}" | gzip > "${OUT_FILE}"

echo "Backup completado: ${OUT_FILE}"
