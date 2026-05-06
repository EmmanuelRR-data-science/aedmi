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

**Probar en local (Mapa).** Inicia sesión, abre el dashboard y la pestaña **Mapa**. La API expone `GET /mapa/fuentes`, `GET /mapa/alcance`, búsqueda, capas, `POST /mapa/export/pdf` (botón *Descargar informe PDF*) y descarga HTML generada en cliente. Ajusta variables opcionales `MAPA_*` en `.env` (timeouts, caché) si trabajas con redes lentas. Tras levantar la base, el ETL de pueblos mágicos se registra solo si existe la fila en `public.fuentes_datos` (`init.sql` la inserta).

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
- `API_URL` (**obligatorio** para que el frontend apunte a la API correcta)
  - Con puertos (sin Nginx): `http://TU_IP:8080`
  - Con reverse proxy: `https://tu-dominio.com/api`
- `CORS_ORIGINS` (origen permitido por la API)
  - Con puertos (sin Nginx): `http://TU_IP:3000`
  - Con reverse proxy: `https://tu-dominio.com`
  - Si no se define, el API permite todos los orígenes (sin credentials).

### 2. Desplegar stack productivo

```bash
chmod +x scripts/deploy_prod.sh scripts/smoke_test_prod.sh scripts/backup_db.sh
./scripts/deploy_prod.sh
```

Esto construye imágenes, levanta contenedores y ejecuta smoke tests básicos.

### 3. Restaurar snapshot de base de datos (recomendado para VPS)

El repo versiona un snapshot listo para producción en `db/vps-snapshot/aedmi-data.sql.gz`. Para restaurarlo en un VPS:

```bash
# Levanta solo la base primero (para inicializar init.sql en volumen vacío)
docker compose --env-file .env.prod -f docker-compose.prod.yml up -d db

# Restaura schema+datos desde db/vps-snapshot/aedmi-data.sql.gz
bash scripts/restore_vps_db_snapshot.sh
```

Si ya restauraste datos, desactiva el ETL masivo al arranque:

```env
ETL_RUN_ON_START=0
```

### 4. Operación diaria

```bash
# Estado de servicios
docker compose --env-file .env.prod -f docker-compose.prod.yml ps

# Logs API
docker compose --env-file .env.prod -f docker-compose.prod.yml logs -f api

# Actualizar versión (después de git pull)
docker compose --env-file .env.prod -f docker-compose.prod.yml build
docker compose --env-file .env.prod -f docker-compose.prod.yml up -d
```

### 5. Reverse proxy (Nginx en host)

Se incluye plantilla en `deploy/nginx/aedmi.conf` para servir:
- Frontend: `/` -> `127.0.0.1:3000`
- API: `/api` -> `127.0.0.1:8080`

Después de copiar el archivo a Nginx y ajustar `server_name`, puedes emitir TLS con Certbot.

### 6. Backup de base de datos

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

**Exportación a PowerPoint (Requisito 25):** con sesión iniciada, en el dashboard usa el botón **Exportar a PowerPoint** en el bloque de la gráfica. El backend genera un PPTX vía `POST /export/presentacion` (JWT). El texto del análisis prioriza el **análisis revisado**; si no hay, el **análisis IA**. Si la captura de la gráfica falla, aún se genera la presentación con título y texto.

**Plantilla corporativa (PPTX):** opcionalmente configura en el `.env` de la API `PPTX_TEMPLATE_PATH` apuntando a tu `.pptx` corporativo (la app **no** ofrece subida desde el navegador). En el repo, **`api/assets/plantilla-aedmi-export.pptx`** ya cumple la convención `AEDMI_*` (deriva del estudio de mercado de ejemplo). Guía: **[docs/guias/plantilla-pptx-corporativa.md](docs/guias/plantilla-pptx-corporativa.md)**. Ejemplo Docker: `PPTX_TEMPLATE_PATH=/app/assets/plantilla-aedmi-export.pptx`. Contrato: `SPEC_DRIVEN_CONTRACT.md` §12 y §12.5.

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
