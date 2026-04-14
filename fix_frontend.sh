#!/bin/bash
set -e
cd /opt/aedmi-sdd/frontend

echo "Corrigiendo archivos de código fuente..."
# 1. Limpiar cualquier basura de intentos previos (.http://... o duplicados)
find src -type f -exec sed -i "s/\.http:\/\/135.181.30.179:8080\./'http:\/\/135.181.30.179:8080'/g" {} + || true
find src -type f -exec sed -i "s/?? 'http:\/\/135.181.30.179:8080'//g" {} + || true

# 2. Reemplazo definitivo de variables por la IP
find src -type f -exec sed -i "s/process.env.NEXT_PUBLIC_API_URL/'http:\/\/135.181.30.179:8080'/g" {} + || true

# 3. Limpiar comillas dobles residuales si las hay
find src -type f -exec sed -i "s/''http/'http/g" {} + || true
find src -type f -exec sed -i "s/8080''/8080'/g" {} + || true

echo "Verificando cambio en src/lib/api.ts:"
grep "135.181.30.179" src/lib/api.ts

echo "Iniciando reconstrucción de Docker..."
cd /opt/aedmi-sdd
docker compose -f docker-compose.prod.yml build --no-cache frontend
docker compose -f docker-compose.prod.yml up -d frontend

echo "¡Listo!"
