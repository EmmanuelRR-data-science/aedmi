# api/core/config.py
from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str
    postgres_password: str
    postgres_db: str

    # JWT
    jwt_secret: str
    jwt_expire_hours: int = 8

    # Auth
    admin_user: str = "PhiQus"
    admin_password: str

    # Groq
    groq_api_key: str

    # CORS (lista separada por comas)
    cors_origins: str = "http://localhost:3000"

    # Módulo mapa (tolerancia, caché y resiliencia; sobreescribir por .env)
    mapa_provider_timeout_ms: int = 4000
    mapa_max_retries: int = 2
    mapa_circuit_breaker_failures: int = 5
    mapa_cooldown_seconds: int = 120
    mapa_fallback_max_age_seconds: int = 86400
    mapa_query_cache_ttl_seconds: int = 300

    # Gamma (exportación de presentación vía API pública; opcional)
    gamma_api_key: str = ""
    gamma_id: str = ""  # legado; ya no se usa (solo theme_id + generación desde texto)
    theme_id: str = ""

    # PPTX corporativo (plantilla con shapes AEDMI_*; vacío = generación legacy)
    pptx_template_path: str = ""

    class Config:
        env_file = ".env"
        case_sensitive = False

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def database_url_sync(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()


def parse_cors_origins(raw: str) -> list[str]:
    return [origin.strip() for origin in raw.split(",") if origin.strip()]
