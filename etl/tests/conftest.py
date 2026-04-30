# etl/tests/conftest.py
# Fixtures compartidos ETL (extensible vía pytest + hypothesis)
from __future__ import annotations

import os

# Asegurar variables mínimas si se cargan módulos que leen .env
os.environ.setdefault("POSTGRES_HOST", "127.0.0.1")
