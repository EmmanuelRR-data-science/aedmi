"""
Reemplaza el encabezado clásico (flex + título + solo ↓ PNG) por ChartExportToolbar.
Excluye archivos multi-gráfica (ver SKIP). Soporta slug con paréntesis (.replace(...)).
Ejecutar desde la raíz del repo: python tools/patch_chart_export_toolbars.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHARTS = ROOT / "frontend" / "src" / "components" / "charts"

SKIP = {
    "MercadoAereoChart.tsx",
    "PIBAnualChart.tsx",
    "PIBProyeccionChart.tsx",
    "MunicipiosPoblacionKpis.tsx",
    "LocalidadesPoblacionKpis.tsx",
    "CiudadesIndicadoresPanel.tsx",
    "RedCarreteraEstatalChart.tsx",
}

PIE_FILES = {"IEDPaisChart.tsx"}

MARKER = (
    "<div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', position: 'relative' }}>"
)

TITLE_RE = re.compile(
    r"<p\s+style=\{\{[\s\S]*?fontSize: `\$\{titleSize\}px`[\s\S]*?textAlign: 'center'[\s\S]*?\}\}\s*>([\s\S]*?)</p>",
)


def extract_leyenda(text: str) -> str:
    m = re.search(r"Fuente:\s*([^<\n]+)", text)
    return m.group(1).strip() if m else ""


def infer_nivel(text: str) -> str:
    m = re.search(r"useIndicadores\(\s*'([^']+)'", text)
    return m.group(1) if m else "nacional"


def datos_serie_expr(path: Path, text: str) -> str:
    b = path.name
    if b == "LlegadaTuristasEstatalChart.tsx":
        return (
            "mapDatosForGammaExport(datos.filter((d) => "
            "d.entidad_clave?.endsWith(`:${estado}`) && d.entidad_clave?.startsWith('tur_lleg:')))"
        )
    if "edoDatos" in text:
        return "mapDatosForGammaExport(edoDatos)"
    if b == "AnunciosInversionChart.tsx":
        return "mapDatosForGammaExport(datos)"
    return "mapDatosForGammaExport(datos)"


def entidad_clave_expr(path: Path, text: str) -> str:
    if "interface Props { estado: string }" in text or "({ estado }: Props)" in text:
        return "{estado}"
    return "{null}"


def title_props(title_inner: str) -> tuple[str, str]:
    t = title_inner.strip()
    if t.startswith("{"):
        return f"title={t}", f"titulo={t}"
    s = " ".join(t.split()).replace("\\", "\\\\").replace('"', '\\"')
    return f'title="{s}"', f'titulo="{s}"'


def parse_download_call(block: str) -> tuple[str, str] | None:
    needle = "downloadChartAsPng("
    i = block.find(needle)
    if i < 0:
        return None
    i += len(needle)
    depth = 1
    start = i
    k = i
    while k < len(block) and depth > 0:
        c = block[k]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        k += 1
    if depth != 0:
        return None
    inner = block[start : k - 1]
    parts = inner.split(",", 1)
    if len(parts) != 2:
        return None
    return parts[0].strip(), parts[1].strip()


def find_header_block(text: str, start: int = 0) -> tuple[int, int, str] | None:
    i = text.find(MARKER, start)
    if i < 0:
        return None
    png = text.find("↓ PNG", i)
    if png < 0:
        return None
    end_btn = text.find("</button>", png)
    if end_btn < 0:
        return None
    end_div = text.find("</div>", end_btn)
    if end_div < 0:
        return None
    end_div += len("</div>")
    return i, end_div, text[i:end_div]


def add_imports(text: str) -> str:
    if "from '@/components/presentation/ChartExportToolbar'" in text:
        return text
    block = (
        "import ChartExportToolbar from '@/components/presentation/ChartExportToolbar'\n"
        "import { mapDatosForGammaExport } from '@/lib/datosSerieGamma'"
    )
    if "import AnalisisRevisado from '@/components/ai/AnalisisRevisado'" in text:
        return text.replace(
            "import AnalisisRevisado from '@/components/ai/AnalisisRevisado'",
            "import AnalisisRevisado from '@/components/ai/AnalisisRevisado'\n" + block,
            1,
        )
    if "import type { DatoIndicador } from '@/types'" in text:
        return text.replace(
            "import type { DatoIndicador } from '@/types'",
            "import type { DatoIndicador } from '@/types'\n" + block,
            1,
        )
    if "import MexicoMap from './MexicoMap'" in text:
        return text.replace(
            "import MexicoMap from './MexicoMap'",
            "import MexicoMap from './MexicoMap'\n" + block,
            1,
        )
    return text


def has_indicador_var(text: str) -> bool:
    return bool(re.search(r"const\s+indicador\s*=", text))


def build_toolbar(
    path: Path,
    full_text: str,
    title_inner: str,
    chart_ref: str,
    slug_expr: str,
) -> str:
    nivel = infer_nivel(full_text)
    t1, t2 = title_props(title_inner)
    ley = extract_leyenda(full_text)
    ent = entidad_clave_expr(path, full_text)
    excel = "pie" if path.name in PIE_FILES else "column"
    ley_prop = f'leyendaFuente="{ley}"' if ley else ""
    has_ind = has_indicador_var(full_text)

    parts = [
        "<ChartExportToolbar",
        f"  chartRef={{{chart_ref}}}",
        f"  {t1}",
    ]
    if has_ind:
        parts.append("  indicadorId={indicador?.id ?? null}")
        ds = datos_serie_expr(path, full_text)
        parts.append(f'  nivelGeografico="{nivel}"')
        parts.append(f"  entidadClave={ent}")
        parts.append(f"  {t2}")
        parts.append('  subtitulo="Dashboard AEDMI"')
        parts.append(f"  datosSerie={{{ds}}}")
    else:
        parts.append("  indicadorId={null}")
        parts.append(f'  nivelGeografico="{nivel}"')
        parts.append("  entidadClave={null}")
        parts.append(f"  {t2}")
        parts.append('  subtitulo="Mapa estatal"')
    if ley_prop:
        parts.append(f"  {ley_prop}")
    parts.append(f'  excelChartKind="{excel}"')
    parts.append(f"  onDownloadPng={{() => downloadChartAsPng({chart_ref}, {slug_expr})}}")
    parts.append("/>")
    return "\n".join(parts)


def patch_file(path: Path) -> bool:
    if path.name in SKIP:
        return False
    text = path.read_text(encoding="utf-8")
    if "ChartExportToolbar" in text:
        return False
    if "downloadChartAsPng" not in text:
        return False

    found = find_header_block(text)
    if not found:
        return False
    start, end, block = found
    if text.count(MARKER) > 1:
        print(f"SKIP multi-header (manual): {path.relative_to(ROOT)}")
        return False

    tm = TITLE_RE.search(block)
    if not tm:
        print(f"SKIP sin título reconocido: {path.relative_to(ROOT)}")
        return False
    title_inner = tm.group(1)
    parsed = parse_download_call(block)
    if not parsed:
        print(f"SKIP sin downloadChartAsPng: {path.relative_to(ROOT)}")
        return False
    cref, slug = parsed

    replacement = build_toolbar(path, text, title_inner, cref, slug)
    new_text = text[:start] + replacement + text[end:]
    new_text = add_imports(new_text)
    if "ChartExportToolbar" not in new_text:
        print(f"SKIP imports: {path.relative_to(ROOT)}")
        return False
    path.write_text(new_text, encoding="utf-8")
    print(f"OK {path.relative_to(ROOT)}")
    return True


def main() -> int:
    n = 0
    for p in sorted(CHARTS.rglob("*.tsx")):
        if patch_file(p):
            n += 1
    print(f"Total parcheados: {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
