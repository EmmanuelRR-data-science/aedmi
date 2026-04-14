FROM python:3.12-slim AS base
WORKDIR /app

# Instalar uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copiar dependencias e instalar (sin virtualenv, directo al sistema)
COPY pyproject.toml .
RUN uv pip install --system -e ".[dev]" 2>/dev/null || uv pip install --system .

# Copiar código fuente
COPY . .

# ── Desarrollo (default) ──────────────────────────────────────
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]

# ── Producción ────────────────────────────────────────────────
FROM base AS production
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "2"]
