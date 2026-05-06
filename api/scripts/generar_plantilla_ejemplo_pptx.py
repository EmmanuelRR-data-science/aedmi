#!/usr/bin/env python3
"""Genera un .pptx mínimo compatible con SPEC §12 (nombres AEDMI_*).

Uso (desde el directorio api/):
  uv run python scripts/generar_plantilla_ejemplo_pptx.py
  uv run python scripts/generar_plantilla_ejemplo_pptx.py ./mi-plantilla-base.pptx
"""

from __future__ import annotations

import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parent.parent
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from tests.fixtures_pptx_plantilla import escribir_plantilla_corporativa_minima  # noqa: E402


def main() -> None:
    dest = (
        Path(sys.argv[1]).resolve()
        if len(sys.argv) > 1
        else (API_ROOT / "aedmi-plantilla-ejemplo.pptx")
    )
    escribir_plantilla_corporativa_minima(dest)
    print(f"Plantilla de ejemplo escrita en: {dest}")


if __name__ == "__main__":
    main()
