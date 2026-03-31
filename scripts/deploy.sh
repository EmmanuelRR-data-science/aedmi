#!/usr/bin/env bash
# Deploy script para VPS Hetzner
# Uso: bash scripts/deploy.sh [--with-etl]
#
# Desde local:
#   ssh vps-phiqus "cd /opt/indicadores-demografia-turismo-economia && bash scripts/deploy.sh"
# Con ETL:
#   ssh vps-phiqus "cd /opt/indicadores-demografia-turismo-economia && bash scripts/deploy.sh --with-etl"

set -euo pipefail

DEPLOY_DIR="/opt/indicadores-demografia-turismo-economia"
HEALTH_URL="http://localhost:${API_PORT:-8080}/api/health"
WITH_ETL=false

for arg in "$@"; do
  case $arg in
    --with-etl) WITH_ETL=true ;;
  esac
done

cd "$DEPLOY_DIR"

echo "==> [1/5] Verificando salud de db..."
for i in $(seq 1 10); do
  if docker compose exec db pg_isready -U "${POSTGRES_USER:-postgres}" -q 2>/dev/null; then
    echo "    db healthy"
    break
  fi
  if [ "$i" -eq 10 ]; then
    echo "ERROR: db no esta healthy despues de 30s. Abortando."
    exit 1
  fi
  sleep 3
done

echo "==> [2/5] git pull origin master..."
git pull origin master

echo "==> [3/5] docker compose build --no-cache web..."
docker compose build --no-cache web

echo "==> [4/5] docker compose up -d --no-deps web..."
docker compose up -d --no-deps web

echo "==> [5/5] Health check en $HEALTH_URL (30s timeout)..."
for i in $(seq 1 10); do
  if curl -sf "$HEALTH_URL" | grep -q '"status".*"ok"'; then
    echo "    Deploy exitoso."
    break
  fi
  if [ "$i" -eq 10 ]; then
    echo "ERROR: Health check fallo. Ultimos logs:"
    docker compose logs --tail=50 web
    exit 1
  fi
  sleep 3
done

if [ "$WITH_ETL" = true ]; then
  echo "==> [ETL] Ejecutando ETL..."
  docker compose run --rm etl
fi

echo "==> Deploy completado."
