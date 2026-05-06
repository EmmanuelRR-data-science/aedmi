"""One-shot: añade imports ChartExportToolbar donde falten."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "frontend" / "src" / "components" / "charts"
BLOCK = """import ChartExportToolbar from '@/components/presentation/ChartExportToolbar'
import { mapDatosForGammaExport } from '@/lib/datosSerieGamma'"""

for p in sorted(ROOT.rglob("*.tsx")):
    t = p.read_text(encoding="utf-8")
    if "from '@/components/presentation/ChartExportToolbar'" in t:
        continue
    if "<ChartExportToolbar" not in t:
        continue
    if "import AnalisisRevisado from '@/components/ai/AnalisisRevisado'" in t:
        t = t.replace(
            "import AnalisisRevisado from '@/components/ai/AnalisisRevisado'",
            "import AnalisisRevisado from '@/components/ai/AnalisisRevisado'\n" + BLOCK,
            1,
        )
    elif "import type { DatoIndicador } from '@/types'" in t:
        t = t.replace(
            "import type { DatoIndicador } from '@/types'",
            "import type { DatoIndicador } from '@/types'\n" + BLOCK,
            1,
        )
    elif "import MexicoMap from './MexicoMap'" in t:
        t = t.replace(
            "import MexicoMap from './MexicoMap'",
            "import MexicoMap from './MexicoMap'\n" + BLOCK,
            1,
        )
    else:
        print("NO ANCHOR", p.relative_to(ROOT))
        continue
    p.write_text(t, encoding="utf-8")
    print("OK", p.relative_to(ROOT))
