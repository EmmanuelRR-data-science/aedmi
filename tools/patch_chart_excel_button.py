"""
Inserta ExportChartExcelButton junto a AddToPresentationButton y envuelve en fragmento <>.
Uso: python tools/patch_chart_excel_button.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "frontend" / "src" / "components" / "charts"


def extract_add_props(inner: str) -> dict[str, str]:
    props: dict[str, str] = {}
    for raw in inner.splitlines():
        line = raw.strip()
        if not line or line.startswith("//"):
            continue
        if line.startswith("chartRef=") or line.startswith("compact="):
            continue
        if "=" not in line:
            continue
        key, _, rest = line.partition("=")
        key = key.strip()
        val = rest.strip()
        if val.endswith(","):
            val = val[:-1].strip()
        props[key] = val
    return props


def build_excel_button(props: dict[str, str], excel_kind: str) -> str:
    order = [
        "indicadorId",
        "nivelGeografico",
        "entidadClave",
        "titulo",
        "subtitulo",
        "datosSerie",
        "leyendaFuente",
    ]
    lines = ["            <ExportChartExcelButton"]
    for k in order:
        if k in props:
            lines.append(f"              {k}={props[k]}")
    lines.append(f'              excelChartKind="{excel_kind}"')
    lines.append("            />")
    return "\n".join(lines)


def infer_chart_kind(path: Path, text: str) -> str:
    pl = str(path).lower()
    if "mercadoaereo" in pl or "iedpais" in pl or "iedsector" in pl or "pibsector" in pl:
        return "pie"
    if "composedchart" in text or "linechart" in text:
        return "line"
    return "column"


def patch_file(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    if "ExportChartExcelButton" in text:
        return False
    if "AddToPresentationButton" not in text or "↓ PNG" not in text:
        return False

    if "import ExportChartExcelButton" not in text:
        old = "import AddToPresentationButton from '@/components/presentation/AddToPresentationButton'"
        new = (
            "import AddToPresentationButton from '@/components/presentation/AddToPresentationButton'\n"
            "import ExportChartExcelButton from '@/components/presentation/ExportChartExcelButton'"
        )
        if old not in text:
            return False
        text = text.replace(old, new, 1)

    kind = infer_chart_kind(path, text)

    block_re = re.compile(
        r"(\{\s*(\w+)\s*&&\s*\(\s*\n)(\s*<AddToPresentationButton\s+([\s\S]*?)/>\s*\n)(\s*\)\})",
        re.MULTILINE,
    )

    def repl(m: re.Match[str]) -> str:
        g_open, g_full_btn, g_close = m.group(1), m.group(3), m.group(5)
        inner = m.group(4)
        props = extract_add_props(inner)
        if "indicadorId" not in props or "titulo" not in props:
            return m.group(0)
        excel = build_excel_button(props, kind)
        return f"{g_open}<>\n{g_full_btn}{excel}\n            </>\n{g_close}"

    new_text, cnt = block_re.subn(repl, text)
    if cnt == 0:
        return False
    path.write_text(new_text, encoding="utf-8")
    return True


def main() -> int:
    n = 0
    for p in sorted(ROOT.rglob("*.tsx")):
        try:
            if patch_file(p):
                print("patched", p.relative_to(ROOT))
                n += 1
        except Exception as e:
            print("error", p, e, file=sys.stderr)
            return 1
    print("total", n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
