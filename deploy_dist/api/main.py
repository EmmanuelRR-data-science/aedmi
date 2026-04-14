# api/main.py
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.config import get_settings, parse_cors_origins
from routers import analisis, auth, etl_admin, indicadores, infraestructura, mapa

settings = get_settings()

app = FastAPI(
    title="AEDMI API",
    description="API para la Aplicación de Estudios de Mercado",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=parse_cors_origins(settings.cors_origins),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(indicadores.router)
app.include_router(analisis.router)
app.include_router(etl_admin.router)
app.include_router(infraestructura.router)
app.include_router(mapa.router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
