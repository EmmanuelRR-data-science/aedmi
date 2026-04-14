# api/core/auth.py
from datetime import datetime, timedelta, timezone

import jwt

from core.config import get_settings

settings = get_settings()

# Blacklist en memoria para tokens invalidados (logout)
_token_blacklist: set[str] = set()


def create_token(username: str) -> str:
    expire = datetime.now(tz=timezone.utc) + timedelta(hours=settings.jwt_expire_hours)
    payload = {"sub": username, "exp": expire}
    return jwt.encode(payload, settings.jwt_secret, algorithm="HS256")


def verify_token(token: str) -> str | None:
    """Verifica el token JWT. Retorna el username o None si es inválido."""
    if token in _token_blacklist:
        return None
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
        return payload.get("sub")
    except jwt.PyJWTError:
        return None


def invalidate_token(token: str) -> None:
    """Agrega el token a la blacklist (logout)."""
    _token_blacklist.add(token)


def validate_credentials(username: str, password: str) -> bool:
    """Valida credenciales contra las configuradas en variables de entorno."""
    return username == settings.admin_user and password == settings.admin_password
