"""
ETL UI en Streamlit.
Uso: desde la raíz del proyecto ejecutar:
  streamlit run etl_ui/app.py
Conecta a PostgreSQL local (variables de entorno POSTGRES_*).
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Cargar .env de la raíz del proyecto para POSTGRES_*
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except Exception:
    pass

import streamlit as st
import pandas as pd

from etl_ui.config import INDICATORS


def _data_to_df(data):
    """Convierte list[dict] o dict con listas (o KPIs indicator->{value,date}) a DataFrame(s)."""
    if isinstance(data, list):
        return [("", pd.DataFrame(data))] if data else [("", pd.DataFrame())]
    if isinstance(data, dict):
        # KPIs nacional: { "pib_usd": {value, date}, ... }
        if data and all(isinstance(v, dict) for v in data.values()):
            val0 = next(iter(data.values()))
            if "value" in val0 or "date" in val0:
                rows = [
                    {"indicador": k, "valor": v.get("value"), "fecha": v.get("date")}
                    for k, v in data.items()
                ]
                return [("KPIs", pd.DataFrame(rows))]
        out = []
        for key, rows in data.items():
            if isinstance(rows, list) and rows:
                out.append((key, pd.DataFrame(rows)))
            elif isinstance(rows, list):
                out.append((key, pd.DataFrame()))
        return out if out else [("", pd.DataFrame())]
    return [("", pd.DataFrame())]


def run_preview(indicator_id):
    cfg = INDICATORS.get(indicator_id)
    if not cfg:
        return None, "Indicador no encontrado"
    try:
        data = cfg["preview_fn"]()
    except Exception as e:
        return None, str(e)
    return data, None


def run_save(indicator_id, data):
    cfg = INDICATORS.get(indicator_id)
    if not cfg:
        return False, "Indicador no encontrado"
    try:
        ok = cfg["save_fn"](data)
        return bool(ok), None
    except Exception as e:
        return False, str(e)


def main():
    st.set_page_config(page_title="ETL UI", page_icon="📊", layout="wide")
    st.title("ETL UI — Preview y carga a PostgreSQL local")

    # Conexión BD (solo informativo)
    db_name = os.getenv("POSTGRES_DB", "aedmi")
    try:
        from services.db import get_conn
        with get_conn() as conn:
            st.success("Conectado a PostgreSQL local.")
    except Exception as e:
        st.error(f"No se pudo conectar a PostgreSQL: {e}")
        st.info("Revise POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB.")

    st.sidebar.caption(f"Base de datos: **{db_name}**")
    selected = st.sidebar.radio(
        "Indicador",
        options=list(INDICATORS.keys()),
        format_func=lambda k: INDICATORS[k]["name"],
    )
    cfg = INDICATORS[selected]
    st.header(cfg["name"])
    st.caption(cfg["description"])
    st.caption(f"Tabla(s): {cfg['table_name']}")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Preview (Extraer + Transformar)", key="preview"):
            with st.spinner("Ejecutando E+T..."):
                data, err = run_preview(selected)
            if err:
                st.error(err)
            else:
                st.session_state["etl_preview_data"] = data
                st.session_state["etl_preview_id"] = selected

    with col2:
        if st.button("Guardar en BD (Load)", key="save"):
            preview_data = st.session_state.get("etl_preview_data")
            preview_id = st.session_state.get("etl_preview_id")
            if preview_id != selected or preview_data is None:
                st.warning("Ejecute primero Preview para este indicador y luego Guardar.")
            else:
                with st.spinner("Guardando en PostgreSQL..."):
                    ok, err = run_save(selected, preview_data)
                if err:
                    st.error(err)
                else:
                    st.success("Datos guardados en PostgreSQL.")

    # Mostrar preview actual
    preview_data = st.session_state.get("etl_preview_data")
    preview_id = st.session_state.get("etl_preview_id")
    if preview_data is not None and preview_id == selected:
        st.subheader("Vista previa")
        dfs = _data_to_df(preview_data)
        for label, df in dfs:
            if label:
                st.write(f"**{label}**")
            st.dataframe(df, use_container_width=True)
            st.caption(f"Registros: {len(df)}")

    # Programación y ETL completo
    with st.expander("Programar ejecución y ETL completo"):
        st.markdown("**ETL completo por línea de comandos** (todos los indicadores, carga directa en BD):")
        st.code("python -m etl.run", language="bash")
        st.markdown(
            "**Periodicidad:** use **cron** (Linux/macOS) o **Programador de tareas** (Windows). "
            "Ejemplo cron cada día a las 6:00:"
        )
        st.code("0 6 * * * cd /ruta/al/proyecto && python -m etl.run_from_cron", language="text")
        st.markdown("Si una ejecución programada falla, revise los logs del cron o del Programador de tareas.")


if __name__ == "__main__":
    main()
