# api/core/gamma_export.py
"""Arma el prompt / inputText para Gamma a partir de gamma-prompt.md y datos de indicadores."""
from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

from PIL import Image

from core.presentacion_export import OrigenAnalisis, etiqueta_origen_analisis

# Límite conservador del cuerpo enviado a Gamma (caracteres)
_MAX_TOTAL_PROMPT_CHARS = 340_000
_MAX_IMAGE_BASE64_CHARS = 48_000


def _gamma_prompt_paths() -> list[Path]:
    """Raíz del monorepo (dev) y carpeta api/ (p. ej. imagen Docker con context=./api)."""
    here = Path(__file__).resolve()
    return [
        here.parents[2] / "gamma-prompt.md",
        here.parents[1] / "gamma-prompt.md",
    ]


def load_gamma_prompt_template() -> str:
    for path in _gamma_prompt_paths():
        if path.is_file():
            return path.read_text(encoding="utf-8")
    raise FileNotFoundError(
        "No se encontró gamma-prompt.md (buscado en la raíz del repo y en api/)."
    )


def _png_to_jpeg_data_url(png_bytes: bytes) -> str | None:
    try:
        im = Image.open(BytesIO(png_bytes)).convert("RGB")
    except Exception:
        return None
    im.thumbnail((900, 900))
    for q in (82, 72, 62, 52, 42):
        buf = BytesIO()
        im.save(buf, format="JPEG", quality=q, optimize=True)
        b64 = base64.b64encode(buf.getvalue()).decode("ascii")
        data_url = f"data:image/jpeg;base64,{b64}"
        if len(data_url) <= _MAX_IMAGE_BASE64_CHARS:
            return data_url
    return None


@dataclass(frozen=True)
class GammaDatoSeriePunto:
    """Punto exacto enviado desde la app (misma lógica que DatoIndicador en el front)."""

    periodo: str | int | float
    valor: float
    entidad_clave: str | None = None
    unidad: str | None = None


@dataclass(frozen=True)
class GammaIndicadorBloque:
    orden: int
    titulo: str
    subtitulo: str | None
    nivel_geografico: str
    origen: OrigenAnalisis
    texto_analisis: str
    imagen_png: bytes | None
    datos_serie: tuple[GammaDatoSeriePunto, ...] | None = None


_MAX_FILAS_TABLA_DATOS = 400
_RE_HEX_COLOR = re.compile(r"^#([0-9A-Fa-f]{3}|[0-9A-Fa-f]{4}|[0-9A-Fa-f]{6}|[0-9A-Fa-f]{8})$")


def normalize_palette_hex(raw: list[str] | None, *, max_colors: int = 24) -> list[str] | None:
    """Filtra y ordena colores hex enviados por el front (paleta del panel de estilo)."""
    if not raw:
        return None
    out: list[str] = []
    for c in raw[:max_colors]:
        s = str(c).strip()
        if _RE_HEX_COLOR.match(s) and s not in out:
            out.append(s)
    return out or None


def build_palette_instructions(theme_id: str, paleta_hex: list[str] | None) -> str:
    """Texto que sustituye {{PALETTE_INSTRUCTIONS}} en gamma-prompt.md."""
    tid = theme_id.strip() or "(predeterminado del workspace)"
    if paleta_hex:
        intro = (
            "La aplicación AEDMI envió la **paleta activa del panel de estilo** en el momento del envío. "
            "Para barras, líneas, áreas, donas, leyendas y **cualquier gráfica con varias series o "
            "segmentos de color**, asigna colores **en este orden cíclico** (serie 1 → primer color, "
            "serie 2 → segundo, etc.):"
        )
        lines = [intro, ""]
        labels = (
            "primario (app)",
            "secundario",
            "terciario",
            "cuarto",
            "quinto",
            "sexto",
            "séptimo",
            "octavo",
        )
        for i, hx in enumerate(paleta_hex):
            lab = labels[i] if i < len(labels) else f"posición {i + 1}"
            lines.append(f"- **{lab}:** `{hx}`")
        lines.extend(
            [
                "",
                "Si la tabla o la imagen muestran **varias series** (p. ej. varias filas con distinta "
                '"Serie / entidad", o barras agrupadas por categoría), usa **como mínimo tantos colores '
                "distintos de la lista anterior** como series relevantes (primer color → primera serie, "
                "segundo → segunda, etc.). Si hay **más series que colores**, repite el ciclo en el "
                "**mismo orden**. Si solo hay **una** serie, usa el **primer** color.",
                "",
                f"Tipografía, fondos de diapositiva y elementos no ligados a una serie concreta siguen "
                f"alineados al tema Gamma con `themeId` **`{tid}`** (incluye acentos del tema para texto o "
                "marcos). **No reduzcas** el diseño de series al solo color primario del tema ni sustituyas "
                "los hex anteriores por un único acento.",
            ]
        )
        return "\n".join(lines)
    return (
        f"Para gráficas con **varias series**, usa la paleta **completa** del tema Gamma con `themeId` "
        f"**`{tid}`**: aplica de forma explícita los **colores secundarios, terciarios y acentos "
        f"adicionales** definidos en ese tema, no solo el color primario. Asigna **una serie distinta** "
        f"a cada acento disponible en el tema, en el orden que el tema suela emplear (primario → secundario → "
        f"terciario…), y mantén la **misma** jerarquía cromática en todas las diapositivas. No uses un solo "
        f"color de acento para todas las series."
    )


def _fmt_periodo(p: str | int | float) -> str:
    return str(p).strip()


def _fmt_valor_tabla(v: float) -> str:
    if abs(v) >= 1e9 or (abs(v) < 1e-6 and v != 0.0):
        return f"{v:.6g}"
    rounded = round(v, 6)
    if abs(rounded - int(rounded)) < 1e-9:
        return f"{int(rounded):,}"
    return f"{rounded:,.6f}".rstrip("0").rstrip(".")


def _tabla_datos_markdown(puntos: tuple[GammaDatoSeriePunto, ...]) -> str:
    if not puntos:
        return ""
    tiene_entidad = any((p.entidad_clave or "").strip() for p in puntos)
    unidad_hint = next((p.unidad for p in puntos if (p.unidad or "").strip()), None)
    titulo = (
        "**Tabla de datos (origen aplicación)** — única fuente autoritativa de cifras para este bloque."
    )
    if (unidad_hint or "").strip():
        titulo += f" Unidad: `{unidad_hint.strip()}`."
    n = len(puntos)
    trunc = n > _MAX_FILAS_TABLA_DATOS
    slice_p = puntos[:_MAX_FILAS_TABLA_DATOS] if trunc else puntos
    if tiene_entidad:
        lines = ["| Período | Valor | Serie / entidad |", "| --- | --- | --- |"]
        for p in slice_p:
            ent = (p.entidad_clave or "").strip() or "—"
            lines.append(
                f"| {_fmt_periodo(p.periodo)} | {_fmt_valor_tabla(p.valor)} | {ent} |"
            )
    else:
        lines = ["| Período | Valor |", "| --- | --- |"]
        for p in slice_p:
            lines.append(f"| {_fmt_periodo(p.periodo)} | {_fmt_valor_tabla(p.valor)} |")
    note = ""
    if trunc:
        note = (
            f"\n\n*Nota: la app envió **{n}** filas; aquí se listan las primeras "
            f"{_MAX_FILAS_TABLA_DATOS}. **No inventes** valores para filas omitidas.*"
        )
    return titulo + "\n\n" + "\n".join(lines) + note


def build_dataset_markdown(bloques: list[GammaIndicadorBloque]) -> str:
    parts: list[str] = []
    for b in bloques:
        sub = (b.subtitulo or "").strip()
        geo = b.nivel_geografico
        ctx = f"{sub} — nivel **{geo}**" if sub else f"Nivel **{geo}**"
        label = etiqueta_origen_analisis(b.origen)
        tabla = ""
        if b.datos_serie:
            tabla = _tabla_datos_markdown(b.datos_serie) + "\n\n"
        chunk = (
            f"### {b.orden}. {b.titulo.strip()}\n\n"
            f"**Contexto:** {ctx}\n\n"
            f"{tabla}"
            f"**{label}** (reproducir **completo** en la diapositiva, sin resumir)\n\n"
            f"{b.texto_analisis.strip()}\n\n"
        )
        img_line = ""
        if b.imagen_png:
            du = _png_to_jpeg_data_url(b.imagen_png)
            if du:
                safe = re.sub(r"[\[\]]", "", b.titulo.strip())[:120]
                img_line = f"**Gráfica (imagen capturada en la app)**\n\n![{safe}]({du})\n\n"
            else:
                img_line = (
                    "**Gráfica:** captura disponible pero demasiado pesada para incrustar; "
                    "describe las tendencias solo con el texto del análisis.\n\n"
                )
        parts.append(chunk + img_line)
    return "\n---\n\n".join(parts)


def build_gamma_user_message(
    *,
    titulo_presentacion: str,
    theme_id: str,
    bloques: list[GammaIndicadorBloque],
    paleta_hex: list[str] | None = None,
) -> str:
    tpl = load_gamma_prompt_template()
    dataset = build_dataset_markdown(bloques)
    theme_label = theme_id.strip() or "(predeterminado del workspace)"
    palette_norm = normalize_palette_hex(paleta_hex)
    palette_block = build_palette_instructions(theme_id, palette_norm)
    combined = (
        tpl.replace("{{THEME_ID}}", theme_label)
        .replace("{{PALETTE_INSTRUCTIONS}}", palette_block)
        .replace("{{DATASET_MARKDOWN}}", dataset)
    )
    header = f"# Título de la presentación\n\n**{titulo_presentacion.strip()}**\n\n"
    aviso = (
        "## Instrucciones de cumplimiento\n\n"
        "1. Incluye el **texto de análisis** de cada bloque **en su totalidad** (ver etiqueta bajo cada bloque). "
        "2. Con **varias series** de datos, asigna **un color distinto por serie** según la paleta indicada; "
        "no dejes todas las series con un solo color.\n\n"
    )
    full = header + aviso + combined
    if len(full) > _MAX_TOTAL_PROMPT_CHARS:
        raise ValueError(
            "El prompt supera el límite aceptado por Gamma; "
            "reduce el número de gráficas en la cola."
        )
    return full


def num_cards_sugerido(n_indicadores: int) -> int:
    n = max(1, n_indicadores)
    return min(75, max(4, n * 2 + 2))
