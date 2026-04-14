import os
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from core.logger import get_logger

logger = get_logger(__name__)

_engine: Engine | None = None
_SessionLocal: sessionmaker | None = None

MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds, doubles each retry

_DOTENV_LOADED = False


def _effective_postgres_host() -> str:
    """
    `POSTGRES_HOST=db` es correcto dentro de la red Docker Compose; en la máquina
    local ese nombre no resuelve. Si no estamos en contenedor, usar localhost.
    """
    host = os.environ.get("POSTGRES_HOST", "127.0.0.1")
    if host != "db":
        return host
    in_container = os.path.exists("/.dockerenv") or os.environ.get("RUNNING_IN_DOCKER") == "1"
    if in_container:
        return "db"
    logger.info(
        "POSTGRES_HOST=db solo aplica dentro de Docker; usando 127.0.0.1 para conexión local."
    )
    return "127.0.0.1"


def _load_dotenv_from_repo() -> None:
    """Carga `.env` de la raíz del repositorio (funciona al ejecutar ETL desde `etl/` o la raíz)."""
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    _DOTENV_LOADED = True
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    # etl/core/db.py -> raíz del repo = parents[2]
    repo_root = Path(__file__).resolve().parents[2]
    env_path = repo_root / ".env"
    if env_path.is_file():
        load_dotenv(env_path)
        logger.debug("Variables cargadas desde %s", env_path)
    else:
        load_dotenv()


def build_db_url() -> str:
    _load_dotenv_from_repo()
    host = _effective_postgres_host()
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    db = os.environ.get("POSTGRES_DB")
    missing = [
        name
        for name, val in (
            ("POSTGRES_USER", user),
            ("POSTGRES_PASSWORD", password),
            ("POSTGRES_DB", db),
        )
        if not val
    ]
    if missing:
        raise RuntimeError(
            "Faltan variables de entorno para PostgreSQL: "
            + ", ".join(missing)
            + (
                ". Copia `.env.example` a `.env` en la raíz del proyecto "
                "y configura la base de datos."
            )
        )
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        url = build_db_url()
        _engine = create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10)
    return _engine


def get_session_factory() -> sessionmaker:
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=get_engine(), autocommit=False, autoflush=False)
    return _SessionLocal


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    factory = get_session_factory()
    session: Session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def connect_with_retry(max_retries: int = MAX_RETRIES) -> Engine:
    engine = get_engine()
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Conexión a PostgreSQL establecida.")
            return engine
        except Exception as exc:
            wait = RETRY_BACKOFF**attempt
            logger.warning(
                f"Intento {attempt}/{max_retries} fallido: {exc}. Reintentando en {wait}s..."
            )
            if attempt == max_retries:
                logger.error(
                    "No se pudo conectar a PostgreSQL después de %d intentos.", max_retries
                )
                raise
            time.sleep(wait)
    return engine  # unreachable, satisfies type checker
