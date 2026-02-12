FROM python:3.11-slim AS base

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    cron \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.10.2 /uv /uvx /bin/

COPY pyproject.toml .python-version uv.lock ./
ENV UV_NO_DEV=1
RUN uv sync --no-install-project

COPY . .
COPY etl/crontab /etc/cron.d/etl-cron
RUN chmod 0644 /etc/cron.d/etl-cron

ENV FLASK_APP=app.py
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 5000

CMD ["sh", "-c", "python -m etl.run & exec python app.py"]
