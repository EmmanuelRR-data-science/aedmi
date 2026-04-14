#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f ".env.prod" ]]; then
  echo "ERROR: Falta .env.prod. Copia .env.prod.example y completa variables."
  exit 1
fi

echo "[1/4] Construyendo imagenes de produccion..."
docker compose --env-file .env.prod -f docker-compose.prod.yml build

echo "[2/4] Levantando stack de produccion..."
docker compose --env-file .env.prod -f docker-compose.prod.yml up -d

echo "[3/4] Esperando salud de servicios..."
sleep 5
docker compose --env-file .env.prod -f docker-compose.prod.yml ps

echo "[4/4] Smoke test local..."
bash scripts/smoke_test_prod.sh

echo "Deploy completado."
