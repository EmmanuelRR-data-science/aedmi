"""
Aplicación para Estudios de Mercado Inmobiliario
Flask app con login y soporte para modo oscuro/claro
"""

import os

from dotenv import load_dotenv
from flask import (
    Flask,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-key-change-in-production")

# Credenciales desde variables de entorno
APP_USER = os.getenv("APP_USER")
APP_PASSWORD = os.getenv("APP_PASSWORD")
APP_SKIP_AUTH = os.getenv("APP_SKIP_AUTH", "").lower() in ("1", "true", "yes")


def _require_auth():
    """Verifica sesión; si APP_SKIP_AUTH está activo, permite continuar."""
    if APP_SKIP_AUTH:
        return True
    return session.get("logged_in")


@app.route("/api/init")
def api_init():
    """
    Inicializa datos: ejecuta ETL y seed. Sin auth.
    Útil para poblar la BD tras desplegar.
    """
    try:
        from etl.run import run_etl
        from services.db import seed_kpis_from_other_tables, get_kpis_nacional_from_db

        run_etl()
        seed_kpis_from_other_tables()
        kpis = get_kpis_nacional_from_db()
        return jsonify({
            "status": "ok",
            "message": "ETL y seed ejecutados",
            "kpis_count": len(kpis) if kpis else 0,
        })
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/api/health")
def api_health():
    """Diagnóstico: sin auth. Verifica conexión a BD y datos."""
    db_ok = False
    kpis_count = 0
    try:
        from services.db import get_conn, get_kpis_nacional_from_db
        conn = get_conn()
        conn.close()
        db_ok = True
        kpis = get_kpis_nacional_from_db()
        kpis_count = len(kpis) if kpis else 0
    except Exception:
        pass
    return jsonify({
        "status": "ok",
        "db_ok": db_ok,
        "kpis_count": kpis_count,
        "skip_auth": APP_SKIP_AUTH,
    })


@app.route("/")
def index():
    """Redirige al login o dashboard según sesión"""
    if _require_auth():
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    """Pantalla de login con validación de credenciales"""
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        theme = request.form.get("theme", "light")

        if username == APP_USER and password == APP_PASSWORD:
            session["logged_in"] = True
            session["theme"] = theme
            return redirect(url_for("dashboard"))
        return render_template(
            "login.html",
            error="Credenciales incorrectas. Intente nuevamente.",
            theme=theme,
        )

    theme = request.args.get("theme", session.get("theme", "light"))
    return render_template("login.html", theme=theme)


@app.route("/logout")
def logout():
    """Cierra la sesión del usuario"""
    session.clear()
    return redirect(url_for("login"))


@app.route("/dashboard")
def dashboard():
    """Pantalla principal después del login"""
    if not _require_auth():
        return redirect(url_for("login"))
    theme = session.get("theme", "light")
    return render_template("dashboard.html", theme=theme)


@app.route("/assets/<path:filename>")
def serve_assets(filename):
    """Sirve archivos estáticos desde la carpeta assets"""
    return send_from_directory("assets", filename)


def _format_kpis_from_db(db_data: dict) -> dict:
    """Formatea los KPIs desde la base de datos para el frontend."""

    def _fmt_pib_usd(v):
        if v is None:
            return "N/D"
        try:
            return f"${float(v):,.2f} B"
        except (ValueError, TypeError):
            return "N/D"

    def _fmt_tasa(v):
        if v is None:
            return "N/D"
        try:
            return f"${float(v):,.2f}"
        except (ValueError, TypeError):
            return "N/D"

    def _fmt_pib_mxn(v):
        if v is None:
            return "N/D"
        try:
            n = float(v)
            if n >= 1e12:
                return f"${n / 1e12:,.2f} T"
            if n >= 1e9:
                return f"${n / 1e9:,.2f} B"
            return f"${n:,.0f}"
        except (ValueError, TypeError):
            return "N/D"

    result = {}
    formatters = {
        "pib_usd": _fmt_pib_usd,
        "tipo_cambio": _fmt_tasa,
        "inflacion": lambda v: f"{v}%" if v is not None else "N/D",
        "pib_mxn": _fmt_pib_mxn,
    }
    for key in ("pib_usd", "tipo_cambio", "inflacion", "pib_mxn"):
        row = db_data.get(key, {})
        val = row.get("value")
        date = row.get("date") or "N/D"
        result[key] = {
            "value": val,
            "date": date,
            "formatted": formatters.get(key, lambda v: str(v) if v else "N/D")(val),
        }
    return result


@app.route("/api/kpis/nacional")
def api_kpis_nacional():
    """
    API: KPIs nacionales.
    Prioridad: 1) PostgreSQL, 2) Seed desde otras tablas si vacío, 3) ETL, 4) APIs externas.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_kpis_nacional
        from services.db import get_kpis_nacional_from_db, is_kpis_complete, seed_kpis_from_other_tables

        db_data = get_kpis_nacional_from_db()
        if is_kpis_complete(db_data):
            kpis = _format_kpis_from_db(db_data)
            return jsonify(kpis)
        # Si vacío: intentar poblar desde tipo_cambio e inflación
        if not db_data or not any(db_data.get(k, {}).get("value") for k in ("pib_usd", "tipo_cambio", "inflacion", "pib_mxn")):
            seed_kpis_from_other_tables()
            db_data = get_kpis_nacional_from_db()
        if db_data:
            kpis = _format_kpis_from_db(db_data)
            return jsonify(kpis)
        # Ejecutar ETL para intentar llenar
        try:
            from etl.run import run_etl

            run_etl()
            seed_kpis_from_other_tables()
            db_data = get_kpis_nacional_from_db()
            if db_data:
                kpis = _format_kpis_from_db(db_data)
                return jsonify(kpis)
        except Exception:
            pass
        # Último recurso: APIs externas
        kpis = get_kpis_nacional()
        return jsonify(kpis)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


GROQ_MODEL = "llama-3.3-70b-versatile"


def _build_analisis_prompt(kpis: dict) -> str:
    """Construye el prompt para el análisis de KPIs según .edmirules.md"""
    base = """Eres un analista experto en estudios de mercado inmobiliario en México. Analiza los siguientes indicadores macroeconómicos nacionales:

Indicadores:
- PIB Nacional (USD): {pib_usd} (fecha: {pib_usd_date})
- Tasa de cambio USD/MXN: {tipo_cambio} (fecha: {tipo_cambio_date})
- Inflación (%): {inflacion} (fecha: {inflacion_date})
- PIB Nacional (MXN): {pib_mxn} (fecha: {pib_mxn_date})

INSTRUCCIONES:
1. Interpreta los datos y busca patrones, tendencias y posibles outliers.
2. Explica por qué ocurre ese comportamiento.
3. Relaciona los datos con el contexto económico del país (México).
4. Tu respuesta debe estar en Español de México.
5. Sé conciso pero informativo. Usa párrafos cortos."""
    return base.format(**kpis)


@app.route("/api/analizar-ia", methods=["POST"])
def api_analizar_ia():
    """
    Analiza los 4 KPIs nacionales con IA (Groq llama-3.3-70b-versatile).
    Espera JSON: { pib_usd, tipo_cambio, inflacion, pib_mxn } con value/date/formatted.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return jsonify({"error": "GROQ_API_KEY no configurada"}), 500
    data = request.get_json() or {}
    kpis = {
        "pib_usd": data.get("pib_usd", {}).get("formatted", "N/D"),
        "pib_usd_date": data.get("pib_usd", {}).get("date", "N/D"),
        "tipo_cambio": data.get("tipo_cambio", {}).get("formatted", "N/D"),
        "tipo_cambio_date": data.get("tipo_cambio", {}).get("date", "N/D"),
        "inflacion": data.get("inflacion", {}).get("formatted", "N/D"),
        "inflacion_date": data.get("inflacion", {}).get("date", "N/D"),
        "pib_mxn": data.get("pib_mxn", {}).get("formatted", "N/D"),
        "pib_mxn_date": data.get("pib_mxn", {}).get("date", "N/D"),
    }
    try:
        from groq import Groq

        client = Groq(api_key=api_key)
        prompt = _build_analisis_prompt(kpis)
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=2048,
        )
        texto = resp.choices[0].message.content or ""
        return jsonify({"analisis": texto})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/distribucion-poblacion-edad")
def api_distribucion_poblacion_edad():
    """
    API: Distribución de la población por edad.
    Prioridad: 1) PostgreSQL, 2) API INEGI.
    Retorna [{year, pob_0_14, pob_15_64, pob_65_plus}, ...].
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_estructura_poblacional

        data = get_estructura_poblacional()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/pib-nacional-total-percapita")
def api_pib_nacional_total_percapita():
    """
    API: PIB Nacional (Total y Per Cápita) - Banco Mundial, anual.
    Fuente: pib_historico_percapita.ipynb
    Retorna [{anio, pib_total_mxn_billones, pib_per_capita_mxn}, ...].
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_pib_nacional

        data = get_pib_nacional()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/proyeccion-pib")
def api_proyeccion_pib():
    """
    API: Proyección PIB (FMI WEO).
    Fuente: pib_proyeccion.ipynb
    Retorna {data: [{anio, pib_total_mxn_billones, pib_total_usd_billones, pib_per_capita_mxn, pib_per_capita_usd}, ...], tc_fix, tc_date}.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_proyeccion_pib

        result = get_proyeccion_pib()
        if not result.get("data"):
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/tipo-cambio-mxn-usd")
def api_tipo_cambio_mxn_usd():
    """
    API: Tipo de cambio (MXN/USD) histórico.
    Fuente: pq-estudios-mercado-vps, Banxico SF43718.
    Retorna {diario: [{fecha, tc}, ...], mensual: [{fecha, tc_prom_mes}, ...]}.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_tipo_cambio_historico

        result = get_tipo_cambio_historico()
        if not result.get("diario") and not result.get("mensual"):
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/inflacion-nacional")
def api_inflacion_nacional():
    """
    API: Inflación nacional mensual (Banxico INPC).
    Fuente: inflacion_nacional.ipynb
    Prioridad: 1) PostgreSQL, 2) API Banxico, 3) CSV.
    Retorna [{anio, mes, inflacion, texto_fecha}, ...].
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_inflacion_nacional

        data = get_inflacion_nacional()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/ied-flujo-estado")
def api_ied_flujo_estado():
    """
    API: Flujo de inversión extranjera por entidad (últimos 4 trimestres).
    Fuente: inversion_extranjera_ied.ipynb (Secretaría de Economía).
    Retorna [{entidad, mdd_4t, rank, periodo}, ...].
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_ied_flujo_entidad

        data = get_ied_flujo_entidad()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/ranking-turismo-mundial")
def api_ranking_turismo_mundial():
    """
    API: Ranking Turismo Mundial (Banco Mundial WDI).
    Prioridad: 1) PostgreSQL, 2) API World Bank, 3) CSV.
    Retorna [{iso, country, year, val}, ...].
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_ranking_turismo_wb

        data = get_ranking_turismo_wb()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/balanza-visitantes")
def api_balanza_visitantes():
    """
    API: Balanza de Visitantes (INEGI BISE).
    Prioridad: 1) PostgreSQL, 2) API INEGI, 3) CSV.
    Retorna [{year, entradas, salidas, balance}, ...].
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_balanza_visitantes

        data = get_balanza_visitantes()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/participacion-mercado-aereo")
def api_participacion_mercado_aereo():
    """
    API: Participación Mercado Aéreo (Nacional e Internacional).
    Fuente: AFAC/DataTur (CUADRO_DGAC).
    Retorna {nacional: [{aerolinea, participacion}], internacional: [{region, pasajeros}]}.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_participacion_mercado_aereo

        data = get_participacion_mercado_aereo()
        if not data.get("nacional") and not data.get("internacional"):
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/participacion-internacional-region")
def api_participacion_internacional_region():
    """
    API: Participación Internacional (Región).
    Fuente: AFAC/DataTur (CUADRO_DGAC, sheet 0).
    Retorna [{region, pasajeros}, ...].
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_participacion_internacional_region

        data = get_participacion_internacional_region()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/ied-pais-origen")
def api_ied_pais_origen():
    """
    API: IED por País de Origen (Secretaría de Economía).
    Prioridad: 1) PostgreSQL, 2) API datos.gob.mx, 3) CSV.
    Retorna [{pais, monto_mdd, periodo}, ...] (top10 + Otros).
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_ied_paises

        data = get_ied_paises()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/ied-sector-economico")
def api_ied_sector_economico():
    """
    API: IED por Sector Económico (Secretaría de Economía).
    Prioridad: 1) PostgreSQL, 2) API datos.gob.mx, 3) CSV.
    Retorna [{sector, monto_mdd, periodo}, ...].
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_ied_sectores

        data = get_ied_sectores()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/poblacion-sector-actividad")
def api_poblacion_sector_actividad():
    """
    API: Población por sector de actividad económica.
    Prioridad: 1) PostgreSQL, 2) API INEGI, 3) CSV de respaldo.
    Retorna [{sector, valor, pct, es_residual}, ...].
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_pob_sector_actividad

        data = get_pob_sector_actividad()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/poblacion-economica-activa")
def api_poblacion_economica_activa():
    """
    API: Población Económicamente Activa (PEA).
    Prioridad: 1) PostgreSQL, 2) API INEGI, 3) CSV de respaldo.
    Retorna [{fecha_fmt, anio, trimestre, valor}, ...].
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_pea

        data = get_pea()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/distribucion-poblacion-sexo")
def api_distribucion_poblacion_sexo():
    """
    API: Distribución de la población por sexo.
    Prioridad: 1) PostgreSQL, 2) API INEGI, 3) CSV de respaldo.
    Retorna [{year, male, female}, ...].
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_distribucion_sexo

        data = get_distribucion_sexo()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/crecimiento-poblacional")
def api_crecimiento_poblacional():
    """
    API: Crecimiento poblacional nacional.
    Prioridad: 1) PostgreSQL, 2) API INEGI.
    Retorna [{year, value}, ...].
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_crecimiento_poblacional_nacional

        data = get_crecimiento_poblacional_nacional()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/analizar-ia-indicador", methods=["POST"])
def api_analizar_ia_indicador():
    """
    Analiza datos de un indicador con IA (Groq).
    Espera JSON: { "indicator": "nombre", "data": [{...}] }.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return jsonify({"error": "GROQ_API_KEY no configurada"}), 500
    payload = request.get_json() or {}
    indicator = payload.get("indicator", "Indicador")
    data = payload.get("data", [])
    if not data:
        return jsonify({"error": "No hay datos para analizar"}), 400
    try:
        import json as _json

        from groq import Groq

        data_str = _json.dumps(data[:50], ensure_ascii=False, indent=2)
        prompt = f"""Eres un analista experto en estudios de mercado inmobiliario en México.
Analiza los siguientes datos del indicador «{indicator}»:

{data_str}

INSTRUCCIONES:
1. Interpreta los datos y busca patrones, tendencias y posibles outliers.
2. Explica por qué ocurre ese comportamiento.
3. Relaciona con el contexto económico de México.
4. Responde en Español de México. Sé conciso pero informativo."""
        client = Groq(api_key=api_key)
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=2048,
        )
        texto = resp.choices[0].message.content or ""
        return jsonify({"analisis": texto})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/toggle-theme", methods=["POST"])
def toggle_theme():
    """Cambia entre modo oscuro y claro"""
    if not _require_auth():
        return redirect(url_for("login"))
    current = session.get("theme", "light")
    session["theme"] = "dark" if current == "light" else "light"
    return redirect(request.referrer or url_for("dashboard"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
