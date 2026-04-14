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

API_PORT="${API_PORT:-8080}"
FRONTEND_PORT="${FRONTEND_PORT:-3000}"

echo "Verificando API health..."
curl -fsS "http://127.0.0.1:${API_PORT}/health" >/dev/null

echo "Verificando frontend..."
curl -fsS "http://127.0.0.1:${FRONTEND_PORT}" >/dev/null

echo "Verificando endpoint autenticacion..."
curl -fsS -X POST "http://127.0.0.1:${API_PORT}/auth/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${ADMIN_USER}\",\"password\":\"${ADMIN_PASSWORD}\"}" >/dev/null

echo "Smoke test OK."
