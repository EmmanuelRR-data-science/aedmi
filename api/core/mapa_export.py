# api/core/mapa_export.py
"""Generación de export HTML/PDF del módulo mapa (informe de viabilidad básico)."""

from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from typing import Any

from fpdf import FPDF

from core.mapa_fuentes import CATALOGO_FUENTES_MAPA, indicadores_mapa_10


def _esc(s: str) -> str:
    return html.escape(s, quote=True)


def build_mapa_html_report(payload: dict[str, Any]) -> str:
    lat = float(payload.get("lat", 0))
    lng = float(payload.get("lng", 0))
    radio = int(payload.get("radio_m", 3000))
    ciudad = str(payload.get("ciudad", "Ubicación"))
    capas = payload.get("capas") or []
    capas_datos = payload.get("capas_datos") or []
    sm = str(payload.get("source_mode", "real_time_first"))
    at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    trazas = CATALOGO_FUENTES_MAPA[:4]
    indic = indicadores_mapa_10()

    return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Mapa exportado — {_esc(ciudad)}</title>
<style>
body{{font-family:system-ui,Segoe UI,sans-serif;margin:0;
background:#0f172a;color:#e2e8f8;padding:24px;}}
h1{{font-size:1.2rem;}}
h2{{font-size:1rem;margin-top:24px;color:#94a3b8;}}
table{{width:100%;border-collapse:collapse;font-size:13px;}}
th,td{{border:1px solid #334155;padding:6px 8px;text-align:left;}}
th{{background:#1e293b;}}
.m{{color:#94a3b8;font-size:13px;}}
a{{color:#38bdf8;}}
pre{{background:#111827;padding:12px;overflow:auto;font-size:11px;border-radius:8px;}}
</style>
</head>
<body>
<h1>Exportación de mapa — {_esc(ciudad)}</h1>
<p class="m">Generado: {_esc(at)} · Modo fuente: {_esc(sm)}</p>
<p>Coordenadas: {_esc(f"{lat:.6f}, {lng:.6f}")} · Radio: {_esc(str(radio))} m</p>
<p>Mapa embebido: <a href="https://www.openstreetmap.org/?mlat={lat}&mlon={lng}#map=15/{lat}/{
        lng
    }">OpenStreetMap</a></p>
<h2>Capas seleccionadas</h2>
<ul>{" ".join(f"<li>{_esc(str(c))}</li>" for c in capas)}</ul>
<h2>Trazabilidad de fuentes (MVP)</h2>
<table>
<tr><th>Origen</th><th>URL</th><th>Fase</th></tr>
{
        "".join(
            f"<tr><td>{_esc(str(f.get('nombre', '')))}</td>"
            f'<td><a href="{_esc(str(f.get("url", "")))}">enlace</a></td>'
            f"<td>{_esc(str(f.get('fase', '')))}</td></tr>"
            for f in trazas
        )
    }
</table>
<h2>Indicadores de mapa (catálogo 10 — referencia)</h2>
<table>
<tr><th>Clave</th><th>Bloque</th><th>Unidad</th><th>Dependencia de radio</th></tr>
{
        "".join(
            f"<tr><td>{_esc(i['clave'])}</td><td>{_esc(i['bloque'])}</td><td>{_esc(i['unidad'])}</td><td>{_esc(i['dependencia_radio'])}</td></tr>"
            for i in indic
        )
    }
</table>
<h2>Payload capas (GeoJSON resumido)</h2>
<pre>{_esc(json.dumps(capas_datos, ensure_ascii=False, indent=2)[:20000])}</pre>
</body>
</html>"""


def build_mapa_pdf_viabilidad(payload: dict[str, Any]) -> bytes:
    title = str(payload.get("titulo", "Informe de viabilidad (mapa)"))
    notas = str(payload.get("notas", ""))
    lat = float(payload.get("lat", 0))
    lng = float(payload.get("lng", 0))
    radio = int(payload.get("radio_m", 3000))
    ciudad = str(payload.get("ciudad", "Ubicación"))
    at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    ind = indicadores_mapa_10()[:5]

    pdf = FPDF()
    pdf.set_margins(12, 12, 12)
    pdf.set_auto_page_break(True, margin=12)
    pdf.add_page()
    pdf.set_title(title[:120])
    pdf.set_font("Helvetica", "B", 14)

    def _line(s: str) -> None:
        t = s.encode("latin-1", "replace").decode("latin-1")
        pdf.set_x(12)
        pdf.multi_cell(190 - 24, 6, t)

    _line(title)
    pdf.set_font("Helvetica", size=10)
    _line(f"Generado: {at}")
    _line(f"Ciudad/etiqueta: {ciudad}")
    _line(f"Coordenadas: {lat:.6f}, {lng:.6f}  Radio: {radio} m")
    _line("Indicadores (muestra):")
    for row in ind:
        _line(f" - {row['clave']}: {row['descripcion']} ({row['unidad']})")
    if notas:
        _line("Notas:")
        _line(notas[:2000])
    out = pdf.output()
    if isinstance(out, (bytes, bytearray)):
        return bytes(out)
    if isinstance(out, str):
        return out.encode("latin-1", "replace")
    return b""
