"""
Aplicación para Estudios de Mercado Inmobiliario
Flask app con login y soporte para modo oscuro/claro
"""

import os
import tempfile
from werkzeug.utils import secure_filename

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

# ── ETL on-demand trigger ──────────────────────────────────────────────
import threading
_etl_lock = threading.Lock()
_etl_running = False

def _should_run_etl():
    """Returns True if last ETL was >24 h ago or never ran."""
    try:
        print(f"[IA-DEBUG] Saving User Analysis: slug={slug}, ind_key={ind_key}")
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT updated_at FROM kpis_nacional
                ORDER BY updated_at DESC LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                return True
            from datetime import datetime, timezone, timedelta
            last = row[0]
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            return (now - last) > timedelta(hours=24)
    except Exception as e:
        print(f"[ETL-CHECK] Error checking last ETL: {e}", file=__import__('sys').stderr)
        return False

def _run_etl_background():
    """Run ETL in background thread."""
    global _etl_running
    if _etl_running:
        return
    with _etl_lock:
        if _etl_running:
            return
        _etl_running = True
    try:
        print("[ETL] Starting background ETL...", file=__import__('sys').stderr)
        from etl.run import run_etl
        result = run_etl()
        print(f"[ETL] Background ETL finished with code: {result}", file=__import__('sys').stderr)
    except Exception as e:
        print(f"[ETL] Background ETL error: {e}", file=__import__('sys').stderr)
    finally:
        _etl_running = False
# ── End ETL trigger ────────────────────────────────────────────────────
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


@app.route("/api/clear-cache/<table_name>")
def api_clear_cache(table_name):
    """Limpia caché de una tabla específica (solo tablas permitidas)."""
    allowed = {"anuncios_inversion_base", "anuncios_inversion_combinados"}
    if table_name not in allowed:
        return jsonify({"error": f"Tabla '{table_name}' no permitida"}), 400
    try:
        from services.db import get_conn
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table_name}")  # noqa: S608
            conn.commit()
        return jsonify({"status": "ok", "message": f"Tabla {table_name} limpiada"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Lista de estados (32 entidades) para sección Estatal (mismo orden que STATE_ID_TO_NAME)
ESTADOS_LIST = [
    {"id": 1, "nombre": "Aguascalientes"},
    {"id": 2, "nombre": "Baja California"},
    {"id": 3, "nombre": "Baja California Sur"},
    {"id": 4, "nombre": "Campeche"},
    {"id": 5, "nombre": "Coahuila de Zaragoza"},
    {"id": 6, "nombre": "Colima"},
    {"id": 7, "nombre": "Chiapas"},
    {"id": 8, "nombre": "Chihuahua"},
    {"id": 9, "nombre": "Ciudad de México"},
    {"id": 10, "nombre": "Durango"},
    {"id": 11, "nombre": "Guanajuato"},
    {"id": 12, "nombre": "Guerrero"},
    {"id": 13, "nombre": "Hidalgo"},
    {"id": 14, "nombre": "Jalisco"},
    {"id": 15, "nombre": "México"},
    {"id": 16, "nombre": "Michoacán de Ocampo"},
    {"id": 17, "nombre": "Morelos"},
    {"id": 18, "nombre": "Nayarit"},
    {"id": 19, "nombre": "Nuevo León"},
    {"id": 20, "nombre": "Oaxaca"},
    {"id": 21, "nombre": "Puebla"},
    {"id": 22, "nombre": "Querétaro"},
    {"id": 23, "nombre": "Quintana Roo"},
    {"id": 24, "nombre": "San Luis Potosí"},
    {"id": 25, "nombre": "Sinaloa"},
    {"id": 26, "nombre": "Sonora"},
    {"id": 27, "nombre": "Tabasco"},
    {"id": 28, "nombre": "Tamaulipas"},
    {"id": 29, "nombre": "Tlaxcala"},
    {"id": 30, "nombre": "Veracruz de Ignacio de la Llave"},
    {"id": 31, "nombre": "Yucatán"},
    {"id": 32, "nombre": "Zacatecas"},
]


@app.route("/api/estados")
def api_estados():
    """Lista de estados para el menú Estatal."""
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    return jsonify(ESTADOS_LIST)


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
    # Check if ETL needs to run (>24h since last update)
    try:
        if _should_run_etl():
            t = threading.Thread(target=_run_etl_background, daemon=True)
            t.start()
    except Exception:
        pass
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


@app.route("/api/indicadores/anuncios-inversion-combinados")
def api_anuncios_inversion_combinados():
    """API: Anuncios de Inversión Combinados (DataMéxico)."""
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_anuncios_inversion_combinados
        data = get_anuncios_inversion_combinados()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/anuncios-inversion-base")
def api_anuncios_inversion_base():
    """API: Anuncios de Inversión Base (DataMéxico)."""
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_anuncios_inversion_base
        data = get_anuncios_inversion_base()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/pib-sector-economico")
def api_pib_sector_economico():
    """API: PIB por Sector Económico (INEGI vía DataMéxico)."""
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_pib_sector_economico
        data = get_pib_sector_economico()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/balanza-comercial-producto")
def api_balanza_comercial_producto():
    """API: Balanza Comercial por Producto (PostgreSQL o API Economía). Siempre 200; lista vacía si no hay datos."""
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_balanza_comercial_producto
        data = get_balanza_comercial_producto()
        return jsonify(data if data else [])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/operaciones-aeroportuarias")
def api_operaciones_aeroportuarias():
    """API: Operaciones Aeroportuarias (PostgreSQL: producto_aeropuertos_nacional + participacion_mercado_aereo)."""
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_operaciones_aeroportuarias
        data = get_operaciones_aeroportuarias()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores/actividad-hotelera")
def api_actividad_hotelera():
    """API: Actividad Hotelera (SECTUR / DataTur)."""
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.data_sources import get_actividad_hotelera
        data = get_actividad_hotelera()
        # Siempre devolver 200; el frontend muestra mensaje si nacional/por_categoria están vacíos
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


@app.route("/api/indicadores-estatales/anuncios-inversion")
def api_estatal_anuncios_inversion():
    """
    Anuncios de Inversión (Combinados) filtrados por estado.
    Query: ?estado=Nombre+Estado (ej. Nuevo León).
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    if not estado:
        return jsonify({"error": "Falta parámetro estado"}), 400
    try:
        from services.data_sources import get_anuncios_inversion_combinados

        data = get_anuncios_inversion_combinados()
        if not data:
            return jsonify({"error": "No hay datos disponibles"}), 404
        # Filtrar por estado (estado_limpio o state normalizado)
        def match(r):
            e = (r.get("estado_limpio") or r.get("state") or "").strip()
            return e.lower() == estado.lower() or e == estado
        filtered = [r for r in data if match(r)]
        return jsonify(filtered)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-estatales/pib")
def api_estatal_pib():
    """
    Análisis Geo-Económico (PIB) por estado.
    Query: ?estado=Nombre+Estado (ej. Nuevo León).
    Retorna { series: [...], resumen: { anio, pib_millones, poblacion, extension_km2, pib_per_capita, variacion_pct } }.
    Los 4 indicadores del resumen coinciden con estado_poblacion_pib.ipynb.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    if not estado:
        return jsonify({"error": "Falta parámetro estado"}), 400
    try:
        from services.db import get_geo_economico_from_db

        payload = get_geo_economico_from_db(estado)
        if not payload:
            return jsonify({"error": f"No hay datos Geo-Económicos para el estado '{estado}'"}), 404
            
        series = payload.get("series", [])
        poblacion = payload.get("poblacion") or 0
        extension_km2 = payload.get("extension_km2") or 0
        
        # Generar el resumen (tomando el último año de la serie)
        resumen = None
        if series:
            ultimo = series[-1]
            pib_actual = ultimo.get("pib_actual") or 0
            pib_pc_resumen = (pib_actual * 1_000_000 / poblacion) if poblacion and pib_actual else 0
            
            # Recalcular pib per capita en la serie para cada año también si se desea,
            # pero el frontend principal solo usa el de resumen.
            for s in series:
                s_pib = s.get("pib_actual") or 0
                s["pib_per_capita"] = round((s_pib * 1_000_000 / poblacion), 2) if poblacion and s_pib else 0
                
            resumen = {
                "anio": ultimo["anio"],
                "pib_millones": pib_actual,
                "poblacion": poblacion,
                "extension_km2": extension_km2,
                "pib_per_capita": round(pib_pc_resumen, 2),
                "variacion_pct": ultimo.get("variacion_pct"),
            }
            
        return jsonify({"series": series, "resumen": resumen})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-estatales/demografia")
def api_estatal_demografia():
    """
    Análisis Demográfico (INEGI) por estado. estado_crecimiento_hist.ipynb.
    Query: ?estado=Nombre+Estado.
    Datos desde PostgreSQL (ETL); si no hay, fallback a cache/INEGI.
    Retorna { crecimiento: [...], genero: [...], edad: [...] }.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    if not estado:
        return jsonify({"error": "Falta parámetro estado"}), 400
    try:
        from services.data_sources import ESTADO_NOMBRE_TO_CODIGO, get_demografia_estatal
        from services.db import get_demografia_estatal_from_db

        codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado)
        data = get_demografia_estatal_from_db(codigo) if codigo else None
        if not data:
            data = get_demografia_estatal(estado)
        if not data:
            return jsonify({
                "error": "No hay datos de demografía para este estado. Ejecute el ETL para cargar desde INEGI.",
            }), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-estatales/proyecciones-conapo")
def api_estatal_proyecciones_conapo():
    """
    Proyecciones de Población (CONAPO) por estado. estado_proyeccion.ipynb.
    Query: ?estado=Nombre+Estado.
    Datos desde PostgreSQL (ETL); si no hay, fallback a descarga directa CSV.
    Retorna [{anio, total, hombres, mujeres}, ...] para años 2025-2030.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    if not estado:
        return jsonify({"error": "Falta parámetro estado"}), 400
    try:
        from services.data_sources import ESTADO_NOMBRE_TO_CODIGO, get_proyecciones_conapo
        from services.db import get_proyecciones_conapo_from_db

        codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado)
        data = get_proyecciones_conapo_from_db(codigo) if codigo else None
        if not data:
            data = get_proyecciones_conapo(estado)
        if not data:
            return jsonify({
                "error": "No hay proyecciones CONAPO para este estado. Ejecute el ETL para cargar desde datos.gob.mx.",
            }), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-estatales/itaee")
def api_estatal_itaee():
    """
    Actividad Económica ITAEE por estado. estado_pib_sectores.ipynb.
    Query: ?estado=Nombre+Estado.
    Datos desde PostgreSQL (ETL); si no hay, fallback a API INEGI directa.
    Retorna {anio, primario, secundario, terciario, total} para el último año disponible.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    if not estado:
        return jsonify({"error": "Falta parámetro estado"}), 400
    try:
        from services.data_sources import ESTADO_NOMBRE_TO_CODIGO, get_itaee_estatal
        from services.db import get_itaee_estatal_timeline_from_db
        
        codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado)
        data = get_itaee_estatal_timeline_from_db(codigo) if codigo else []
        
        if not data:
            from services.data_sources import get_itaee_estatal_timeline
            data = get_itaee_estatal_timeline(estado)
            
        if not data:
            # Fallback a un solo registro si no hay histórico
            from services.db import get_itaee_estatal_from_db
            latest = get_itaee_estatal_from_db(codigo) if codigo else None
            if latest:
                data = [latest]
            else:
                data = get_itaee_estatal(estado)
                if data:
                    data = [data]
        
        if not data:
            return jsonify({
                "error": "No hay datos ITAEE para este estado. Ejecute el ETL para cargar desde INEGI.",
            }), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-estatales/mapa-carretero")
def api_estatal_mapa_carretero():
    """
    Mapa Carretero por estado (SCT). estado_conectividad.ipynb.
    Query: ?estado=Nombre+Estado.
    Retorna imagen PNG del mapa carretero oficial de la SCT.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    if not estado:
        return jsonify({"error": "Falta parámetro estado"}), 400
    try:
        from services.data_sources import get_mapa_carretero_estatal
        
        img_bytes, error = get_mapa_carretero_estatal(estado)
        if error:
            import logging
            logging.error(f"Error al obtener mapa carretero para {estado}: {error}")
            return jsonify({"error": error}), 404
        if not img_bytes:
            return jsonify({"error": "No se pudo generar la imagen del mapa carretero"}), 500
        
        from flask import Response
        return Response(img_bytes, mimetype="image/png", headers={
            "Content-Disposition": f"inline; filename=mapa_carretero_{estado.replace(' ', '_')}.png",
            "Cache-Control": "public, max-age=3600"
        })
    except Exception as e:
        import logging
        import traceback
        logging.error(f"Excepción al obtener mapa carretero para {estado}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({"error": f"Error interno: {str(e)}"}), 500


@app.route("/api/indicadores-estatales/actividad-hotelera", methods=["GET"])
def api_estatal_actividad_hotelera():
    """
    Actividad Hotelera por estado (y opcionalmente año). Lee desde PostgreSQL.
    Query: ?estado=Nombre+Estado&anio=2024. Respuesta incluye "years" con los años disponibles.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    if not estado:
        return jsonify({"error": "Falta parámetro estado"}), 400
    anio_arg = request.args.get("anio", "").strip()
    anio = int(anio_arg) if anio_arg.isdigit() else None
    try:
        from services.data_sources import get_actividad_hotelera_estatal

        data, err, years = get_actividad_hotelera_estatal(estado, anio=anio)
        if err:
            return jsonify({"error": err, "years": years}), 404
        if not data:
            return jsonify({"error": "No hay datos de actividad hotelera para este estado", "years": years}), 404
        out = dict(data)
        out["years"] = years
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e), "years": []}), 500


@app.route("/api/indicadores-estatales/actividad-hotelera/upload", methods=["POST"])
def api_estatal_actividad_hotelera_upload():
    """
    Recibe el archivo Excel del CETM (SECTUR), lo procesa y guarda en PostgreSQL.
    Replica estado_turismo_llegadas.ipynb. Form: multipart/form-data, campo 'file'.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    f = request.files.get("file")
    if not f or f.filename == "":
        return jsonify({"error": "No se envió ningún archivo"}), 400
    if not (f.filename.lower().endswith(".xlsx") or f.filename.lower().endswith(".xls")):
        return jsonify({"error": "El archivo debe ser Excel (.xlsx o .xls)"}), 400
    import tempfile
    import os as _os
    try:
        from services.data_sources import process_actividad_hotelera_from_upload
        from services.db import save_actividad_hotelera_estatal_to_db

        suffix = ".xlsx" if f.filename.lower().endswith(".xlsx") else ".xls"
        fd, path = tempfile.mkstemp(suffix=suffix)
        try:
            f.save(path)
            data_by_estado, err = process_actividad_hotelera_from_upload(path)
            if err:
                return jsonify({"error": err}), 400
            if not data_by_estado:
                return jsonify({"error": "No se encontraron datos de estados en el archivo"}), 400
            saved = 0
            for codigo, data_by_year in data_by_estado.items():
                for anio, data in (data_by_year or {}).items():
                    if save_actividad_hotelera_estatal_to_db(codigo, data, anio=anio):
                        saved += 1
            return jsonify({"saved": saved, "estados": list(data_by_estado.keys())})
        finally:
            try:
                _os.close(fd)
                _os.unlink(path)
            except Exception:
                pass
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-estatales/actividad-hotelera/clear", methods=["POST"])
def api_estatal_actividad_hotelera_clear():
    """
    Limpia todos los datos de actividad hotelera de PostgreSQL.
    Útil para reprocesar el archivo Excel con la lógica corregida.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        print(f"[IA-DEBUG] GET Analisis: slug={slug}, ind_key={ind_key}")
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM actividad_hotelera_estatal")
            deleted = cur.rowcount
        return jsonify({"deleted": deleted, "message": "Datos eliminados. Puede subir el archivo nuevamente."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-estatales/llegada-turistas", methods=["GET"])
def api_estatal_llegada_turistas():
    """
    Llegada de Turistas (Histórico) por estado.
    Query: ?estado=Nombre+Estado.
    Retorna [{anio, total}, ...] (últimos 10 años).
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    if not estado:
        return jsonify({"error": "Falta parámetro estado"}), 400
        
    try:
        from services.data_sources import ESTADO_NOMBRE_TO_CODIGO
        from services.db import get_llegada_turistas_estatal_from_db

        codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado)
        if not codigo:
             # Fallback normalized search 
             from services.data_sources import _normalizar_estado, STATE_ID_TO_NAME
             norm_arg = _normalizar_estado(estado)
             for name in STATE_ID_TO_NAME.values():
                if _normalizar_estado(name) == norm_arg:
                    codigo = ESTADO_NOMBRE_TO_CODIGO.get(name)
                    break

        if not codigo:
             return jsonify({"error": "Estado no encontrado"}), 404
             
        data = get_llegada_turistas_estatal_from_db(codigo)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-estatales/exportaciones-estado", methods=["GET"])
def api_estatal_exportaciones_estado():
    """
    Exportaciones por Estado. Lee desde API DataMéxico.
    Query: ?estado=Nombre+Estado
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    if not estado:
        return jsonify({"error": "Se requiere parámetro 'estado'"}), 400
    try:
        from services.data_sources import get_exportaciones_por_estado
        from services.data_sources import _normalizar_estado, STATE_ID_TO_NAME, ESTADO_NOMBRE_TO_CODIGO

        all_data = get_exportaciones_por_estado()
        if not all_data:
            return jsonify({"error": "No se pudieron obtener datos de exportaciones"}), 500

        # Normalizar nombre del estado
        estado_norm = _normalizar_estado(estado)
        codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado)
        if not codigo:
            for name in STATE_ID_TO_NAME.values():
                if _normalizar_estado(name) == estado_norm:
                    codigo = ESTADO_NOMBRE_TO_CODIGO.get(name)
                    break

        if not codigo:
            return jsonify({"error": f"No se encontró el estado '{estado}'"}), 404

        # Buscar el slug del estado en los datos
        # El slug puede ser como "aguascalientes-ag", "coahuila-de-zaragoza-co", etc.
        estado_slug = None
        estado_norm_simple = estado_norm.replace(" de ", " ").replace(" ", "-")
        for row in all_data:
            state_slug = str(row.get("state_slug", "")).lower()
            if estado_norm_simple in state_slug or state_slug.startswith(estado_norm_simple.split()[0]):
                estado_slug = row["state_slug"]
                break

        # Mapeo especial para estados con nombres que pueden confundirse
        if not estado_slug:
            special_map = {
                "mexico": "mexico-em",
                "ciudad de mexico": "ciudad-de-mexico-cx",
                "coahuila de zaragoza": "coahuila-de-zaragoza-co",
                "michoacan de ocampo": "michoacan-de-ocampo-mi",
                "veracruz de ignacio de la llave": "veracruz-de-ignacio-de-la-llave-ve",
            }
            est_key = estado_norm.replace(" de ", " ").replace(" ", "-")
            if est_key in special_map:
                estado_slug = special_map[est_key]
            else:
                # Buscar por código (últimos 2 caracteres del slug)
                for row in all_data:
                    slug = str(row.get("state_slug", "")).lower()
                    if slug.endswith("-" + codigo.lower()):
                        estado_slug = row["state_slug"]
                        break

        if not estado_slug:
            return jsonify({"error": f"No se encontraron datos de exportación para '{estado}'"}), 404

        # Filtrar datos del estado y ordenar por año
        estado_data = [r for r in all_data if str(r.get("state_slug", "")).lower() == estado_slug.lower()]
        estado_data.sort(key=lambda x: x["year"])

        # Datos para ranking (último año disponible)
        latest_year = max(r["year"] for r in all_data) if all_data else None
        if latest_year:
            latest_data = [r for r in all_data if r["year"] == latest_year]
            latest_data.sort(key=lambda x: x["trade_value"], reverse=True)
            top_15 = latest_data[:15]
            ranking = {r["state_slug"]: i + 1 for i, r in enumerate(latest_data)}
        else:
            top_15 = []
            ranking = {}

        # Calcular participación nacional (último año)
        if latest_year:
            total_nacional = sum(r["trade_value"] for r in latest_data)
            valor_estado = sum(r["trade_value"] for r in estado_data if r["year"] == latest_year)
            participacion = (valor_estado / total_nacional * 100) if total_nacional > 0 else 0
        else:
            total_nacional = 0
            valor_estado = 0
            participacion = 0

        return jsonify({
            "estado": estado,
            "estado_slug": estado_slug,
            "timeline": estado_data,
            "ranking": ranking.get(estado_slug.lower(), 0),
            "top_15": top_15,
            "latest_year": latest_year,
            "valor_estado": valor_estado,
            "total_nacional": total_nacional,
            "participacion": participacion,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-estatales/aeropuertos", methods=["GET"])
def api_estatal_aeropuertos():
    """
    Operaciones Aeroportuarias por Estado. Lee desde PostgreSQL (prioridad) o Excel.
    Query: ?estado=Nombre+Estado
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    if not estado:
        return jsonify({"error": "Se requiere parámetro 'estado'"}), 400
    try:
        from services.data_sources import get_aeropuertos_por_estado
        from services.data_sources import _normalizar_estado, STATE_ID_TO_NAME, ESTADO_NOMBRE_TO_CODIGO

        # Normalizar nombre del estado
        estado_norm = _normalizar_estado(estado)
        codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado)
        if not codigo:
            for name in STATE_ID_TO_NAME.values():
                if _normalizar_estado(name) == estado_norm:
                    codigo = ESTADO_NOMBRE_TO_CODIGO.get(name)
                    break

        if not codigo:
            return jsonify({"error": f"No se encontró el estado '{estado}'"}), 404

        # Obtener datos (consulta BD primero)
        estado_data = get_aeropuertos_por_estado(codigo)
        
        if not estado_data:
            return jsonify({"error": f"No hay datos de aeropuertos para '{estado}'"}), 404

        # Agrupar por aeropuerto y año
        by_airport_year = {}
        for row in estado_data:
            ap = row["aeropuerto"]
            if ap not in by_airport_year:
                by_airport_year[ap] = {}
            by_airport_year[ap][row["anio"]] = {
                "operaciones": row["operaciones"],
                "grupo": row.get("grupo", ""),
            }

        # Obtener lista de aeropuertos y año más reciente
        aeropuertos = sorted(by_airport_year.keys())
        latest_year = max(max(by_airport_year[ap].keys()) for ap in aeropuertos) if aeropuertos else None
        
        # Calcular totales por año para evolución
        timeline = []
        if aeropuertos:
            years = set()
            for ap_data in by_airport_year.values():
                years.update(ap_data.keys())
            for year in sorted(years):
                total = sum(by_airport_year[ap].get(year, {}).get("operaciones", 0) for ap in aeropuertos)
                timeline.append({"year": year, "operaciones": total})

        # Ranking del último año
        ranking = []
        if latest_year:
            for ap in aeropuertos:
                ops = by_airport_year[ap].get(latest_year, {}).get("operaciones", 0)
                if ops > 0:
                    ranking.append({
                        "aeropuerto": ap,
                        "operaciones": ops,
                        "grupo": by_airport_year[ap].get(latest_year, {}).get("grupo", ""),
                    })
            ranking.sort(key=lambda x: x["operaciones"], reverse=True)

        # Datos detallados para evolución (últimos 5 años, top 5 aeropuertos)
        evolution_data = []
        if aeropuertos and latest_year:
            # Top 5 aeropuertos por volumen total
            ap_totals = {}
            for ap in aeropuertos:
                total = sum(by_airport_year[ap].get(year, {}).get("operaciones", 0) for year in by_airport_year[ap].keys())
                ap_totals[ap] = total
            top_5_airports = sorted(ap_totals.items(), key=lambda x: x[1], reverse=True)[:5]
            top_5_names = [ap for ap, _ in top_5_airports]
            
            years_set = set()
            for ap_data in by_airport_year.values():
                years_set.update(ap_data.keys())
            recent_years = sorted(years_set)[-5:] if len(years_set) > 5 else sorted(years_set)
            
            for ap in top_5_names:
                ap_data = []
                for year in recent_years:
                    ops = by_airport_year[ap].get(year, {}).get("operaciones", 0)
                    ap_data.append({"year": year, "operaciones": ops})
                evolution_data.append({"aeropuerto": ap, "data": ap_data})

        return jsonify({
            "estado": estado,
            "estado_codigo": codigo,
            "aeropuertos": aeropuertos,
            "num_aeropuertos": len(aeropuertos),
            "latest_year": latest_year,
            "timeline": timeline,
            "ranking": ranking,
            "evolution": evolution_data,
            "total_operaciones": sum(r["operaciones"] for r in ranking) if ranking else 0,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-estatales/llegada-turistas/upload", methods=["POST"])
def api_estatal_llegada_turistas_upload():
    """
    Sube Excel CETM para procesar 'Vista07a' (Llegada de Turistas).
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
        
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "No file"}), 400
        
    filename = secure_filename(file.filename)
    import tempfile
    
    try:
        suffix = ".xlsx" if filename.endswith(".xlsx") else ".xls"
        fd, path = tempfile.mkstemp(suffix=suffix)
        try:
            file.save(path)
            
            from services.data_sources import process_llegada_turistas_from_upload
            from services.db import save_llegada_turistas_estatal_to_db, db_connection
            
            # Asegurar que la tabla exista (Workaround por falta de psql/migration tool)
            try:
                with db_connection() as conn:
                    cur = conn.cursor()
                    cur.execute('''
                        CREATE TABLE IF NOT EXISTS llegada_turistas_estatal (
                            id SERIAL PRIMARY KEY, 
                            estado_codigo VARCHAR(2) NOT NULL, 
                            anio INTEGER NOT NULL, 
                            turistas_total INTEGER NOT NULL, 
                            UNIQUE(estado_codigo, anio)
                        )
                    ''')
            except Exception as e:
                print(f"Warning creating table: {e}")

            data_by_estado, err = process_llegada_turistas_from_upload(path)
            if err:
                return jsonify({"error": err}), 400
                
            saved_count = 0
            for codigo, anio_dict in data_by_estado.items():
                for anio, total in anio_dict.items():
                    if save_llegada_turistas_estatal_to_db(codigo, anio, total):
                        saved_count += 1
                        
            return jsonify({"message": "Procesado correctamente", "saved_records": saved_count})
            
        finally:
            try:
                os.close(fd)
                os.remove(path)
            except: pass
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
@app.route("/api/municipios")
def api_municipios():
    """Lista de municipios por estado."""
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    if not estado:
        return jsonify({"error": "Se requiere parámetro 'estado'"}), 400
    try:
        from services.data_sources import get_municipios_por_estado
        municipios = get_municipios_por_estado(estado)
        # Retornar array vacío si no hay municipios (no es un error)
        return jsonify(municipios if municipios else [])
    except Exception as e:
        import traceback
        print(f"Error en api_municipios: {e}")
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-municipales/distribucion-poblacion")
def api_municipal_distribucion_poblacion():
    """
    API: Distribución de población por municipio (Censo 2020).
    Parámetros: estado, municipio.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    municipio = request.args.get("municipio", "").strip()
    if not estado or not municipio:
        return jsonify({"error": "Se requieren parámetros 'estado' y 'municipio'"}), 400
    try:
        from services.data_sources import get_distribucion_poblacion_municipal
        data = get_distribucion_poblacion_municipal(estado, municipio)
        if not data:
            return jsonify({"error": "No hay datos disponibles para este municipio"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-municipales/proyeccion-poblacional")
def api_municipal_proyeccion_poblacional():
    """
    API: Proyección poblacional por municipio (CONAPO).
    Parámetros: estado, municipio.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    municipio = request.args.get("municipio", "").strip()
    if not estado or not municipio:
        return jsonify({"error": "Se requieren parámetros 'estado' y 'municipio'"}), 400
    try:
        from services.data_sources import get_proyeccion_poblacional_municipal
        data = get_proyeccion_poblacional_municipal(estado, municipio)
        if not data:
            return jsonify({"error": "No hay datos disponibles para este municipio"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ciudades")
def api_ciudades():
    """
    API: Lista de ciudades del menú (Mérida, Querétaro, CDMX, etc.).
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudades_from_db
        lista = get_ciudades_from_db()
        return jsonify(lista if lista else [])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ciudades/<slug>/distribucion-poblacion")
def api_ciudad_distribucion_poblacion(slug):
    """
    API: Distribución de población por ciudad (Censo 2020).
    Para CDMX agrega todos los municipios/alcaldías.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_distribucion_poblacion_municipal_from_db, get_distribucion_poblacion_entidad_from_db
        from services.data_sources import get_distribucion_poblacion_municipal
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad:
            return jsonify({"error": "Ciudad no encontrada"}), 404
        if ciudad.get("es_entidad_completa"):
            data = get_distribucion_poblacion_entidad_from_db(ciudad["estado_codigo"])
        else:
            data = get_distribucion_poblacion_municipal(ciudad["estado_nombre"], ciudad["municipio_nombre"])
        if not data:
            return jsonify({"error": "No hay datos de distribución para esta ciudad"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ciudades/<slug>/crecimiento-historico")
def api_ciudad_crecimiento_historico(slug):
    """
    API: Crecimiento poblacional anual por ciudad (2005, 2010, 2020).
    Desde PostgreSQL (tabla crecimiento_historico_municipal). Para CDMX agrega todo el estado.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_crecimiento_historico_municipal_from_db
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad:
            return jsonify({"error": "Ciudad no encontrada"}), 404
        municipio_codigo = None if ciudad.get("es_entidad_completa") else ciudad.get("municipio_codigo")
        data = get_crecimiento_historico_municipal_from_db(ciudad["estado_codigo"], municipio_codigo)
        return jsonify(data if data else [])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ciudades/<slug>/sexo-historico")
def api_ciudad_sexo_historico(slug):
    """
    API: Histórico de población por sexo por ciudad (CONAPO).
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_proyeccion_poblacional_sexo_from_db
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad:
            return jsonify({"error": "Ciudad no encontrada"}), 404
        municipio_codigo = None if ciudad.get("es_entidad_completa") else ciudad.get("municipio_codigo")
        data = get_proyeccion_poblacional_sexo_from_db(ciudad["estado_codigo"], municipio_codigo)
        return jsonify(data if data else [])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ciudades/<slug>/poblacion-ocupada-turismo")
def api_ciudad_poblacion_ocupada_turismo(slug):
    """
    API: Población ocupada en restaurantes y hoteles por ciudad.
    Obtenida de Observatorio Turístico de Yucatán (webscraping).
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_poblacion_ocupada_turismo_from_db
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad:
            return jsonify({"error": "Ciudad no encontrada"}), 404
        
        municipio_codigo = None if ciudad.get("es_entidad_completa") else ciudad.get("municipio_codigo")
        data = get_poblacion_ocupada_turismo_from_db(ciudad["estado_codigo"], municipio_codigo)
        return jsonify(data if data else [])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ciudades/<slug>/ocupacion-hotelera")
def api_ciudad_ocupacion_hotelera(slug):
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_ocupacion_hotelera_from_db, get_actividad_hotelera_estatal_from_db
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad: return jsonify({"error": "Ciudad no encontrada"}), 404
        
        # Monterrey usa datos estatales de NL (PED/SEDUM)
        if slug == 'monterrey':
            data, years = get_actividad_hotelera_estatal_from_db(ciudad["estado_codigo"])
            if data:
                return jsonify({"type": "estatal", "data": data})
        
        data = get_ocupacion_hotelera_from_db(ciudad["estado_codigo"], ciudad.get("municipio_codigo"))
        return jsonify(data)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/ciudades/<slug>/llegada-visitantes")
def api_ciudad_llegada_visitantes(slug):
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_llegada_visitantes_from_db
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad: return jsonify({"error": "Ciudad no encontrada"}), 404
        data = get_llegada_visitantes_from_db(ciudad["estado_codigo"], ciudad.get("municipio_codigo"))
        return jsonify(data)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/ciudades/<slug>/gasto-promedio")
def api_ciudad_gasto_promedio(slug):
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_gasto_promedio_from_db
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad: return jsonify({"error": "Ciudad no encontrada"}), 404
        data = get_gasto_promedio_from_db(ciudad["estado_codigo"], ciudad.get("municipio_codigo"))
        return jsonify(data)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/ciudades/<slug>/derrama-economica")
def api_ciudad_derrama_economica(slug):
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_derrama_economica_from_db
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad: return jsonify({"error": "Ciudad no encontrada"}), 404
        # Derrama es mayormente estatal, municipio_codigo puede ser None
        data = get_derrama_economica_from_db(ciudad["estado_codigo"], None)
        return jsonify(data)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/ciudades/<slug>/ingreso-hotelero")
def api_ciudad_ingreso_hotelero(slug):
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_ingreso_hotelero_from_db
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad: return jsonify({"error": "Ciudad no encontrada"}), 404
        data = get_ingreso_hotelero_from_db(ciudad["estado_codigo"], ciudad.get("municipio_codigo"))
        return jsonify(data)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/ciudades/<slug>/establecimientos-turismo")
def api_ciudad_establecimientos_turismo(slug):
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_establecimientos_turismo_from_db
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad: return jsonify({"error": "Ciudad no encontrada"}), 404
        data = get_establecimientos_turismo_from_db(ciudad["estado_codigo"], ciudad.get("municipio_codigo"))
        return jsonify(data)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/ciudades/<slug>/ventas-internacionales")
def api_ciudad_ventas_internacionales(slug):
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_ventas_internacionales_from_db
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad: return jsonify({"error": "Ciudad no encontrada"}), 404
        data = get_ventas_internacionales_from_db(ciudad["estado_codigo"], ciudad.get("municipio_codigo"))
        return jsonify(data)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/ciudades/<slug>/conectividad-aerea")
def api_ciudad_conectividad_aerea(slug):
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db
        from services.data_sources import get_aeropuertos_por_estado
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad: return jsonify({"error": "Ciudad no encontrada"}), 404
        data = get_aeropuertos_por_estado(ciudad["estado_codigo"])
        if not data: return jsonify({"error": "No hay datos de aeropuertos para este estado"}), 404
        return jsonify(data)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/ciudades/<slug>/oferta-servicios-turisticos")
def api_ciudad_oferta_servicios_turisticos(slug):
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_oferta_servicios_turisticos_from_db
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad: return jsonify({"error": "Ciudad no encontrada"}), 404
        data = get_oferta_servicios_turisticos_from_db(ciudad["estado_codigo"], ciudad.get("municipio_codigo"))
        return jsonify(data)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/ciudades/<slug>/vuelos-llegada")
def api_ciudad_vuelos_llegada(slug):
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_vuelos_llegada_aicm_from_db
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad: return jsonify({"error": "Ciudad no encontrada"}), 404
        data = get_vuelos_llegada_aicm_from_db(ciudad["estado_codigo"], ciudad.get("municipio_codigo"))
        return jsonify(data)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/api/ciudades/<slug>/comercio-internacional")
def api_ciudad_comercio_internacional(slug):
    """
    API: Comercio Internacional (IED) para la ciudad, basado en su estado.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_ciudad_by_slug_from_db, get_comercio_internacional_from_db
        ciudad = get_ciudad_by_slug_from_db(slug)
        if not ciudad: return jsonify({"error": "Ciudad no encontrada"}), 404
        
        # Le pasamos el nombre del estado para filtrar en la BD de IED
        data = get_comercio_internacional_from_db(ciudad["estado_nombre"])
        return jsonify(data if data else [])
    except Exception as e: 
        return jsonify({"error": str(e)}), 500

@app.route("/api/ciudades/<slug>/llegada-pasajeros")
def api_ciudad_llegada_pasajeros(slug):
    """API: Llegada de pasajeros al aeropuerto de la ciudad."""
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_llegada_pasajeros_from_db
        data = get_llegada_pasajeros_from_db(slug)
        return jsonify(data if data else [])
    except Exception as e: 
        return jsonify({"error": str(e)}), 500

@app.route("/api/ciudades/<slug>/visitantes-nac-ext")
def api_ciudad_visitantes_nac_ext(slug):
    """API: Visitantes nacionales y extranjeros de la ciudad."""
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import get_visitantes_nac_ext_from_db
        data = get_visitantes_nac_ext_from_db(slug)
        return jsonify(data if data else [])
    except Exception as e: 
        return jsonify({"error": str(e)}), 500

@app.route("/api/localidades")

def api_localidades():
    """
    API: Lista de localidades por estado y opcionalmente municipio.
    Parámetros: estado (requerido), municipio (opcional).
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    if not estado:
        return jsonify({"error": "Se requiere el parámetro 'estado'"}), 400
    municipio = request.args.get("municipio", "").strip() or None
    try:
        from services.data_sources import get_localidades
        lista = get_localidades(estado, municipio)
        return jsonify(lista if lista else [])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-localidades/distribucion-poblacion")
def api_localidad_distribucion_poblacion():
    """
    API: Distribución de población por localidad (Censo 2020).
    Parámetros: estado, municipio, localidad.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    municipio = request.args.get("municipio", "").strip()
    localidad = request.args.get("localidad", "").strip()
    if not estado or not municipio or not localidad:
        return jsonify({"error": "Se requieren 'estado', 'municipio' y 'localidad'"}), 400
    try:
        from services.data_sources import get_distribucion_poblacion_localidad
        data = get_distribucion_poblacion_localidad(estado, municipio, localidad)
        if not data:
            return jsonify({"error": "No hay datos para esta localidad"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indicadores-localidades/crecimiento-historico")
def api_localidad_crecimiento_historico():
    """
    API: Crecimiento histórico por localidad (Censo 2020; solo año 2020 disponible).
    Parámetros: estado, municipio, localidad.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    estado = request.args.get("estado", "").strip()
    municipio = request.args.get("municipio", "").strip()
    localidad = request.args.get("localidad", "").strip()
    if not estado or not municipio or not localidad:
        return jsonify({"error": "Se requieren 'estado', 'municipio' y 'localidad'"}), 400
    try:
        from services.data_sources import get_crecimiento_historico_localidad
        data = get_crecimiento_historico_localidad(estado, municipio, localidad)
        return jsonify(data if data else [])
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
    Espera JSON: { "indicator": "nombre", "data": [{...}], "slug": "X", "indicator_key": "Y" }.
    """
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return jsonify({"error": "GROQ_API_KEY no configurada"}), 500
    payload = request.get_json() or {}
    import sys
    print(f"[IA-PAYLOAD] Received: {payload}", file=sys.stderr)
    sys.stderr.flush()
    indicator = payload.get("indicator", "Indicador")
    slug = (payload.get("slug") or "").strip().lower()
    ind_key = (payload.get("indicator_key") or "").strip().lower()
    data = payload.get("data", [])
    if not data:
        return jsonify({"error": "No hay datos para analizar"}), 400
    try:
        import json as _json
        from groq import Groq

        data_str = _json.dumps(data[:50] if isinstance(data, list) else data, ensure_ascii=False, indent=2)
        prompt = f"""Eres un analista experto en estudios de mercado inmobiliario en México y estudios de Máximo y Mejor Uso (Highest and Best Use).
Analiza los siguientes datos del indicador «{indicator}»:

{data_str}

INSTRUCCIONES:
1. **Identificación:** Menciona claramente el nombre del Estado, Ciudad, Municipio o Localidad que se está analizando (según se deduce o se indica en el título del indicador «{indicator}»).
2. **Contexto Integral:** Complementa el análisis de estos datos específicos relacionándolo con los pilares del desarrollo de la zona: Demografía, Economía, Turismo y Conectividad (menciona los más relevantes para este indicador y zona).
3. **Interpretación:** Interpreta los datos del indicador buscando patrones, tendencias y posibles outliers. Explica por qué ocurre ese comportamiento.
4. **Enfoque Inmobiliario:** Todo el análisis debe estar firmemente proyectado hacia el Sector Inmobiliario y los Estudios de Mejor Uso (Highest and Best Use), explicando cómo estos indicadores influyen en la absorción, plusvalía o viabilidad de proyectos en la región.
5. **Formato:** Responde en Español de México. Sé conciso pero con alta densidad de valor analítico."""
        client = Groq(api_key=api_key)
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=2048,
        )
        texto = resp.choices[0].message.content or ""

        # Persistir análisis si tiene identificadores válidos
        if slug and ind_key:
            try:
                print(f"[IA-DEBUG] Saving Analysis: slug={slug}, ind_key={ind_key}")
                from services.db import db_connection
                with db_connection() as conn:
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO ia_analisis (slug, indicator, analisis, updated_at)
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (slug, indicator) DO UPDATE SET
                            analisis = EXCLUDED.analisis,
                            updated_at = CURRENT_TIMESTAMP
                    """, (str(slug), str(ind_key), texto))
            except Exception as dbe:
                import sys
                print(f"[WARN] Error guardando análisis IA: {dbe}", file=sys.stderr)

        return jsonify({"analisis": texto})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/get-ia-analisis")
def api_get_ia_analisis():
    slug = (request.args.get("slug") or "").strip().lower()
    ind_key = (request.args.get("indicator") or "").strip().lower()
    if not slug or not ind_key:
        return jsonify({"analisis": None})
    try:
        print(f"[IA-DEBUG] GET Analisis: slug={slug}, ind_key={ind_key}")
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT analisis, analisis_usuario FROM ia_analisis WHERE slug = %s AND indicator = %s", (str(slug), str(ind_key)))
            row = cur.fetchone()
            return jsonify({
                "analisis": row[0] if row else None,
                "analisis_usuario": row[1] if row else None
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route("/api/save-user-analisis", methods=["POST"])
def api_save_user_analisis():
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    
    payload = request.get_json() or {}
    slug = (payload.get("slug") or "").strip().lower()
    ind_key = (payload.get("indicator") or "").strip().lower()
    analisis_usuario = payload.get("analisis_usuario", "")
    
    if not slug or not ind_key:
        return jsonify({"error": "Faltan parámetros"}), 400
        
    try:
        print(f"[IA-DEBUG] Saving User Analysis: slug={slug}, ind_key={ind_key}")
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO ia_analisis (slug, indicator, analisis_usuario, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (slug, indicator) DO UPDATE SET
                    analisis_usuario = EXCLUDED.analisis_usuario,
                    updated_at = CURRENT_TIMESTAMP
            """, (str(slug), str(ind_key), analisis_usuario))
        return jsonify({"status": "ok"})
    except Exception as e:
        import sys
        print(f"[WARN] Error guardando análisis de usuario: {e}", file=sys.stderr)
        return jsonify({"error": str(e)}), 500


@app.route("/api/etl-status")
def api_etl_status():
    """Check ETL status and last update time."""
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT updated_at FROM kpis_nacional ORDER BY updated_at DESC LIMIT 1")
            row = cur.fetchone()
        return jsonify({
            "last_update": str(row[0]) if row else None,
            "etl_running": _etl_running,
            "needs_update": _should_run_etl()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/run-etl", methods=["POST"])
def api_run_etl():
    """Manually trigger ETL."""
    if not _require_auth():
        return jsonify({"error": "No autorizado"}), 401
    if _etl_running:
        return jsonify({"status": "already_running"})
    t = threading.Thread(target=_run_etl_background, daemon=True)
    t.start()
    return jsonify({"status": "started"})

@app.route("/toggle-theme", methods=["POST"])
def toggle_theme():
    """Cambia entre modo oscuro y claro"""
    if not _require_auth():
        return redirect(url_for("login"))
    current = session.get("theme", "light")
    session["theme"] = "dark" if current == "light" else "light"
    return redirect(request.referrer or url_for("dashboard"))




@app.route("/api/ciudades/<slug>/pea")
def api_ciudad_pea(slug):
    try:
        from services.db import get_pea_municipal_from_db
        data = get_pea_municipal_from_db(slug)
        if not data:
            return jsonify([])
        return jsonify(data)
    except Exception as e:
        print(f"Error api_ciudad_pea: {e}")
        return jsonify([])


@app.route("/api/ciudades/<slug>/tasa-participacion")
def api_ciudad_tasa_participacion(slug):
    try:
        from services.db import get_tasa_participacion_municipal_from_db
        data = get_tasa_participacion_municipal_from_db(slug)
        if not data:
            return jsonify([])
        return jsonify(data)
    except Exception as e:
        print(f"Error api_ciudad_tasa_participacion: {e}")
        return jsonify([])


@app.route("/api/ciudades/<slug>/tasa-desocupacion")
def api_ciudad_tasa_desocupacion(slug):
    try:
        from services.db import get_tasa_desocupacion_municipal_from_db
        data = get_tasa_desocupacion_municipal_from_db(slug)
        if not data:
            return jsonify([])
        return jsonify(data)
    except Exception as e:
        print(f"Error api_ciudad_tasa_desocupacion: {e}")
        return jsonify([])


@app.route("/api/ciudades/<slug>/composicion-sectorial")
def api_ciudad_composicion_sectorial(slug):
    try:
        from services.db import get_composicion_sectorial_municipal_from_db
        data = get_composicion_sectorial_municipal_from_db(slug)
        if not data:
            return jsonify([])
        return jsonify(data)
    except Exception as e:
        print(f"Error api_ciudad_composicion_sectorial: {e}")
        return jsonify([])


@app.route("/api/ciudades/<slug>/pib-estatal")
def api_ciudad_pib_estatal(slug):
    try:
        from services.db import get_pib_estatal_municipal_from_db
        data = get_pib_estatal_municipal_from_db(slug)
        if not data:
            return jsonify([])
        return jsonify(data)
    except Exception as e:
        print(f"Error api_ciudad_pib_estatal: {e}")
        return jsonify([])


@app.route('/api/ciudades/<slug>/pib-per-capita')
def api_pib_per_capita_ciudad(slug):
    try:
        from services.db import get_pib_per_capita_municipal_from_db
        data = get_pib_per_capita_municipal_from_db(slug)
        return jsonify(data)
    except Exception as e:
        print(f"[API] Error PIB Per Capita ciudad {slug}: {e}")
        return jsonify([])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
