# Indicadores Demografía, Turismo y Economía

Dashboard Flask de indicadores económicos, demográficos y turísticos de México. Incluye KPIs nacionales, análisis por estado, proyecciones del FMI/CONAPO, y análisis con IA (Groq).

## Requisitos

- [Docker](https://docs.docker.com/get-docker/) + Docker Compose v2
- Git
- Acceso SSH al VPS (alias `vps-phiqus` configurado en `~/.ssh/config`)
- Python 3.11 + [UV](https://docs.astral.sh/uv/) (solo para desarrollo local sin Docker)

## Configuración local

```bash
# 1. Clonar el repositorio
git clone https://github.com/EmmanuelRR-data-science/indicadores-demografia-turismo-economia.git
cd indicadores-demografia-turismo-economia

# 2. Configurar variables de entorno
cp .env.example .env
# Editar .env con los valores reales

# 3. Levantar base de datos y app
docker compose up -d db web

# 4. Ejecutar ETL inicial (poblar la BD)
docker compose run --rm etl

# 5. Abrir el dashboard
# http://localhost:8080
```

## Deploy al VPS

```bash
# Deploy solo del servicio web
ssh vps-phiqus "cd /opt/indicadores-demografia-turismo-economia && bash scripts/deploy.sh"

# Deploy + ejecutar ETL
ssh vps-phiqus "cd /opt/indicadores-demografia-turismo-economia && bash scripts/deploy.sh --with-etl"
```

El script realiza en orden: verifica salud de la BD, `git pull`, rebuild de la imagen, reinicio del contenedor web, y health check en `/api/health`.

## Túnel SSH para acceso a PostgreSQL de producción

```bash
# Abrir túnel (mantener la terminal abierta)
ssh -L 5433:127.0.0.1:5433 vps-phiqus

# Conectar con cualquier cliente a localhost:5433
# Credenciales: ver .env del VPS
```

## Comandos útiles

```bash
# Pre-commit: linting y formato
ruff check . --fix && ruff format .

# Ver logs del servicio web en tiempo real
docker compose logs -f web

# Reiniciar solo el web (sin rebuild)
docker compose up -d --no-deps web

# Ejecutar ETL manualmente
docker compose run --rm etl

# Ver estado de los servicios
docker compose ps

# Verificar salud de la app
curl http://localhost:8080/api/health
```

## Variables de entorno

Ver `.env.example` para la lista completa con descripciones y valores de ejemplo. Nunca commitear el `.env` real.

## Arquitectura

```
Local (dev) ──git push──> GitHub (master)
                                │
                         ssh + git pull
                                │
                           VPS Hetzner
                         docker compose
                        ┌──────┴──────┐
                       db           web
                   postgres:16    Flask app
                        │              │
                       cron ──────────┘
                    (ETL periódico)
```
