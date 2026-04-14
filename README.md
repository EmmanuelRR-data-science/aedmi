# AEDMI — Aplicación para Estudios de Mercado

Plataforma web full-stack que centraliza indicadores de demografía, economía, turismo y conectividad aérea para estudios de mercado. Integra un pipeline ETL modular, una API REST en FastAPI, y un frontend interactivo en Next.js con visualizaciones por nivel geográfico y análisis asistido por IA (Groq / llama-3.3-70b-versatile).

---

## Requisitos Previos

- [Docker](https://docs.docker.com/get-docker/) y [Docker Compose](https://docs.docker.com/compose/) v2+
- [UV](https://docs.astral.sh/uv/) — gestor de paquetes Python (para desarrollo local sin Docker)
- [Node.js](https://nodejs.org/) 20+ (para desarrollo local del frontend sin Docker)
- Git

---

## Setup Rápido

### 1. Clonar el repositorio

```bash
git clone <url-del-repositorio>
cd AEDMI-SDD
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env
```

Editar `.env` y ajustar al menos:
- `POSTGRES_PASSWORD` — contraseña segura para la base de datos
- `JWT_SECRET` — string aleatorio largo para firmar tokens JWT
- `ADMIN_PASSWORD` — contraseña de acceso a la aplicación
- `GROQ_API_KEY` — clave de la API de Groq (obtener en [console.groq.com](https://console.groq.com))

### 3. Levantar todos los servicios

```bash
docker compose up
```

La primera vez construirá las imágenes e inicializará la base de datos automáticamente.

| Servicio | URL |
|---------|-----|
| Frontend | http://localhost:3000 |
| API | http://localhost:8080 |
| PostgreSQL | localhost:5432 |

---

## Despliegue en VPS (producción)

### 1. Preparar variables de entorno

```bash
cp .env.prod.example .env.prod
```

Editar `.env.prod` y ajustar como mínimo:
- `POSTGRES_PASSWORD`
- `JWT_SECRET`
- `ADMIN_PASSWORD`
- `GROQ_API_KEY`
- `API_URL` (recomendado `https://tu-dominio.com/api`)
- `CORS_ORIGINS` (por ejemplo `https://tu-dominio.com`)

### 2. Desplegar stack productivo

```bash
chmod +x scripts/deploy_prod.sh scripts/smoke_test_prod.sh scripts/backup_db.sh
./scripts/deploy_prod.sh
```

Esto construye imágenes, levanta contenedores y ejecuta smoke tests básicos.

### 3. Operación diaria

```bash
# Estado de servicios
docker compose --env-file .env.prod -f docker-compose.prod.yml ps

# Logs API
docker compose --env-file .env.prod -f docker-compose.prod.yml logs -f api

# Actualizar versión (después de git pull)
docker compose --env-file .env.prod -f docker-compose.prod.yml build
docker compose --env-file .env.prod -f docker-compose.prod.yml up -d
```

### 4. Reverse proxy (Nginx en host)

Se incluye plantilla en `deploy/nginx/aedmi.conf` para servir:
- Frontend: `/` -> `127.0.0.1:3000`
- API: `/api` -> `127.0.0.1:8080`

Después de copiar el archivo a Nginx y ajustar `server_name`, puedes emitir TLS con Certbot.

### 5. Backup de base de datos

```bash
./scripts/backup_db.sh
```

Genera respaldo comprimido en `./backups`.

---

## Estructura del Proyecto

```
AEDMI-SDD/
├── etl/                    # Pipeline ETL (Python + UV)
│   ├── pyproject.toml
│   ├── main.py             # Punto de entrada
│   ├── scheduler.py        # APScheduler (19:00 MX)
│   ├── core/               # Módulos base: db, logger, extractor
│   ├── sources/            # Módulos por fuente de datos
│   │   └── manual/         # Carga manual XLSX/CSV
│   ├── migrations/         # Scripts SQL adicionales
│   └── tests/              # Tests con pytest + hypothesis
│
├── api/                    # API REST (FastAPI + UV)
│   ├── pyproject.toml
│   ├── main.py             # FastAPI app
│   ├── core/               # Config, DB, auth
│   ├── routers/            # Endpoints por dominio
│   ├── schemas/            # Pydantic schemas
│   └── tests/              # Tests con pytest + hypothesis
│
├── frontend/               # Interfaz web (Next.js 14 + TypeScript)
│   ├── package.json
│   ├── src/
│   │   ├── app/            # App Router de Next.js
│   │   ├── components/     # Componentes React
│   │   ├── hooks/          # Custom hooks (Zustand, react-query)
│   │   ├── lib/            # Utilidades (API client, auth)
│   │   └── types/          # Tipos TypeScript
│   └── public/             # Assets estáticos (fuentes, logos)
│
├── db/
│   ├── init.sql            # DDL inicial (schemas, tablas, índices)
│   └── schema.md           # Documentación del esquema
│
├── assets/                 # Assets del proyecto (fuentes, logos)
├── deploy/nginx/           # Plantilla de Nginx para VPS
├── docker-compose.yml      # Entorno de desarrollo
├── docker-compose.prod.yml # Entorno de producción
├── scripts/                # Scripts de deploy/smoke/backup
├── .env.example            # Plantilla de variables de entorno
├── .env.prod.example       # Plantilla para VPS/producción
└── README.md
```

---

## Comandos Útiles

### Docker

```bash
# Levantar todos los servicios (desarrollo)
docker compose up

# Levantar en background
docker compose up -d

# Ver logs de un servicio
docker compose logs -f api

# Reconstruir imágenes
docker compose build

# Detener y eliminar contenedores
docker compose down

# Producción
docker compose -f docker-compose.prod.yml up -d
```

Nota de persistencia PostgreSQL:
- El volumen de datos se declara con nombre fijo (`aedmi-sdd-cursor_pgdata`) para conservar la BD entre reinicios/redeploy.
- `docker compose down` conserva los datos.
- No usar `docker compose down -v` si quieres mantener la información actual.
- Para agregar nuevos seeds sin borrar información existente, ejecútalos manualmente:
  - `Get-Content .\db\seeds\002_llegada_turistas_estatal.sql | docker compose exec -T db psql -U <POSTGRES_USER> -d <POSTGRES_DB>`

### ETL (Python / UV)

```bash
# Instalar dependencias
cd etl && uv sync --extra dev

# Ejecutar tests
uv run pytest tests/ -v

# Linting y formato
uv run ruff check .
uv run ruff format --check .

# Corregir automáticamente
uv run ruff check --fix .
uv run ruff format .

# Ejecutar ETL manualmente (dentro del contenedor)
docker compose exec etl python main.py
```

### API (Python / UV)

```bash
# Instalar dependencias
cd api && uv sync --extra dev

# Ejecutar tests
uv run pytest tests/ -v

# Linting
uv run ruff check . && uv run ruff format --check .
```

### Frontend (Node.js)

```bash
cd frontend

# Instalar dependencias
npm install

# Desarrollo local
npm run dev

# Tests
npx vitest --run

# Build de producción
npm run build
```

---

## Ciclo de Desarrollo por Indicador

Cada nuevo indicador sigue este ciclo obligatorio antes de hacer commit:

1. **Fuente** — registrar en `fuentes_datos` con estado `pendiente`
2. **ETL** — implementar módulo en `etl/sources/<fuente>/` → estado `etl_listo`
3. **Base de datos** — crear/verificar tabla en schema de periodicidad → estado `api_lista`
4. **API** — exponer endpoint en `api/routers/indicadores.py` → estado `grafica_lista`
5. **Gráfica** — implementar visualización en frontend → estado `completo`
6. **Admin ETL** — verificar que el módulo aparece en `/admin/etl`

Ver `db/schema.md` para la documentación completa del esquema de base de datos.

---

## Credenciales por Defecto (Desarrollo)

- **Usuario:** `PhiQus`
- **Contraseña:** valor de `ADMIN_PASSWORD` en `.env`

> Nunca usar credenciales de desarrollo en producción. Configurar `.env.prod` con valores seguros.
