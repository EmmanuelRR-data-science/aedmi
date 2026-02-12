"""
INGESTA: Obtiene datos crudos de INEGI y Banxico.
Usado por el ETL para el paso 1 del pipeline; fallback cuando PostgreSQL está vacío.
Fallback final: CSV en data/process/ cuando BD e INEGI fallan.
"""

import csv
import json
import os
import re
from datetime import datetime, timedelta
from io import StringIO

import requests

# Ruta base del proyecto (services/ -> padre)
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BANXICO_TOKEN = os.getenv("BANXICO_TOKEN")
INEGI_TOKEN = os.getenv("INEGI_TOKEN")

# Series Banxico
BANXICO_TIPO_CAMBIO = "SF43718"  # Pesos por Dólar FIX
# SP1 = INPC (inflacion_nacional.ipynb)
# Fuente: https://github.com/EmmanuelRR-data-science/phiqus-aedm/blob/main/scripts/inflacion_nacional.ipynb
BANXICO_INFLACION = "SP1"

# Serie INEGI PIB nacional (validado en pib_nacional.ipynb)
# Fuente: https://github.com/EmmanuelRR-data-science/phiqus-aedm/blob/main/scripts/pib_nacional.ipynb
SERIE_INEGI_PIB = "735879"
# PIB nacional histórico (trimestral, millones MXN) - misma que pq
SERIE_INEGI_PIB_NACIONAL = "733671"

# Serie INEGI Crecimiento poblacional nacional (anual, BISE)
SERIE_INEGI_POBLACION = "1002000001"

# PEA: 601614 (BIE-BISE), fallback 289244 (BIE)
SERIE_INEGI_PEA = "601614"
SERIE_INEGI_PEA_FALLBACK = "289244"

# Sector de actividad: 289242=ocupados total, 668907=primario%, 668908=secundario%, 668912=terciario%
IDS_INEGI_SECTOR = ["289242", "668907", "668908", "668912"]

# IDs INEGI para distribución por sexo (1002000002=Hombres, 1002000003=Mujeres)
IDS_INEGI_SEXO = ["1002000002", "1002000003"]

# IDs INEGI para estructura poblacional por edad (0-4, 5-9, 10-14, ..., 85+)
IDS_INEGI_EDAD = [
    "1002000058", "1002000061", "1002000088", "1002000067", "1002000070",
    "1002000073", "1002000076", "1002000079", "1002000082", "1002000085",
    "1002000091", "1002000094", "1002000097", "1002000100", "1002000103",
    "1002000106", "1002000109", "1002000112", "1002000115", "1002000118",
]


def _fetch_banxico(series_id: str, incremento: str = None) -> tuple:
    """Obtiene el dato más reciente de una serie de Banxico. Retorna (valor, fecha) o (None, None)."""
    if not BANXICO_TOKEN:
        return None, None
    url = f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/{series_id}/datos/oportuno"
    params = {"mediaType": "json"}
    if incremento:
        params["incremento"] = incremento
    try:
        # Replicar lógica del notebook: header Bmx-Token primero, fallback a query param
        resp = requests.get(url, headers={"Bmx-Token": BANXICO_TOKEN}, params=params, timeout=15)
        if not resp.ok:
            params["token"] = BANXICO_TOKEN
            resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        series = data.get("bmx", {}).get("series", [])
        if series and series[0].get("datos"):
            dato = series[0]["datos"][0]
            return dato.get("dato"), dato.get("fecha")
    except Exception:
        pass
    return None, None


def _fetch_inegi_pib() -> tuple:
    """
    Obtiene el PIB nacional de INEGI (millones MXN).
    Replica lógica de pib_nacional.ipynb: usa BIE-BISE y serie 735879.
    """
    if not INEGI_TOKEN:
        return None, None
    base_url = "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml"
    url = f"{base_url}/INDICATOR/{SERIE_INEGI_PIB}/es/00/true/BIE-BISE/2.0/{INEGI_TOKEN}?type=json"
    try:
        resp = requests.get(url, timeout=45)
        resp.raise_for_status()
        js = resp.json()
        series = js.get("Series")
        if not series:
            return None, None
        obs = series[0].get("OBSERVATIONS")
        if not obs:
            return None, None
        # Ordenar por TIME_PERIOD y tomar el más reciente (ej: 2025/03)
        obs_sorted = sorted(obs, key=lambda x: x.get("TIME_PERIOD", ""))
        ultimo = obs_sorted[-1]
        return ultimo.get("OBS_VALUE"), ultimo.get("TIME_PERIOD")
    except Exception:
        return None, None


def get_tipo_cambio() -> tuple:
    """Retorna (valor, fecha_str) o (None, None). Tasa USD/MXN."""
    result = _fetch_banxico(BANXICO_TIPO_CAMBIO)
    if result:
        return result
    return None, None


def _obtener_datos_banxico_rango(series_id: str, fecha_inicio: str, fecha_fin: str) -> list:
    """
    Obtiene datos de Banxico para un rango de fechas.
    Replica lógica de inflacion_nacional.ipynb.
    """
    if not BANXICO_TOKEN:
        return []
    url = f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/{series_id}/datos/{fecha_inicio}/{fecha_fin}"
    headers = {"Bmx-Token": BANXICO_TOKEN}
    try:
        resp = requests.get(url, headers=headers, params={"mediaType": "json"}, timeout=15)
        if not resp.ok:
            resp = requests.get(f"{url}?token={BANXICO_TOKEN}&mediaType=json", timeout=15)
        resp.raise_for_status()
        datos = resp.json()
        serie = datos.get("bmx", {}).get("series", [])
        if serie and serie[0].get("datos"):
            return serie[0]["datos"]
    except Exception:
        pass
    return []


def _get_dato_mes(datos: list, ano: int, mes: int) -> float | None:
    """Obtiene el último dato INPC del mes/año indicado."""
    candidatos = []
    for d in datos:
        try:
            fecha = datetime.strptime(d["fecha"], "%d/%m/%Y")
            if fecha.year == ano and fecha.month == mes:
                val = float(str(d.get("dato", "")).replace(",", ""))
                if not (val != val):  # not NaN
                    candidatos.append((fecha, val))
        except (ValueError, TypeError):
            continue
    if not candidatos:
        return None
    candidatos.sort(key=lambda x: x[0])
    return candidatos[-1][1]


def _add_months(dt: datetime, months: int) -> datetime:
    """Suma o resta meses a una fecha."""
    mes = dt.month - 1 + months
    ano = dt.year + mes // 12
    mes = mes % 12 + 1
    return datetime(ano, mes, 1)


MESES_ES = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}


def _fetch_tipo_cambio_banxico() -> tuple[list[dict], list[dict]]:
    """
    Obtiene tipo de cambio FIX (SF43718) desde Banxico.
    Replica lógica de pq-estudios-mercado-vps: diario + promedio mensual.
    Retorna (diario, mensual).
    """
    if not BANXICO_TOKEN:
        return [], []
    f_inicio = "2000-01-01"
    f_fin = datetime.now().strftime("%Y-%m-%d")
    datos = _obtener_datos_banxico_rango(BANXICO_TIPO_CAMBIO, f_inicio, f_fin)
    if not datos:
        return [], []

    diario = []
    for d in datos:
        try:
            fecha_str = d.get("fecha")
            val = d.get("dato")
            if not fecha_str or val is None or str(val) == "N/E":
                continue
            tc = float(str(val).replace(",", ""))
            dt_obj = datetime.strptime(fecha_str, "%d/%m/%Y")
            fecha_iso = dt_obj.strftime("%Y-%m-%d")
            diario.append({"fecha": fecha_iso, "tc": round(tc, 4)})
        except (ValueError, TypeError):
            continue
    diario.sort(key=lambda x: x["fecha"])

    by_month: dict[str, list[float]] = {}
    for r in diario:
        ym = r["fecha"][:7]
        if ym not in by_month:
            by_month[ym] = []
        by_month[ym].append(r["tc"])
    mensual = [
        {"fecha": f"{ym}-01", "tc_prom_mes": round(sum(vals) / len(vals), 4)}
        for ym, vals in sorted(by_month.items())
    ]
    return diario, mensual


def get_inflacion() -> tuple:
    """
    Retorna (inflacion_porcentaje, texto_fecha) o (None, None).
    Replica lógica de inflacion_nacional.ipynb:
    - Serie SP1 (INPC)
    - inflacion = ((valor_actual - valor_anterior) / valor_anterior) * 100
    - Intenta mes -1, mes -2 hasta encontrar datos
    """
    if not BANXICO_TOKEN:
        return None, None
    hoy = datetime.now()
    for i in (1, 2):
        mes_referencia = _add_months(datetime(hoy.year, hoy.month, 1), -i)
        mes_comparativo = _add_months(mes_referencia, -12)
        f_inicio = (mes_comparativo - timedelta(days=5)).strftime("%Y-%m-%d")
        # Extender f_fin para capturar publicación tardía del INPC (suele publicarse al mes siguiente)
        f_fin = (mes_referencia + timedelta(days=45)).strftime("%Y-%m-%d")
        datos = _obtener_datos_banxico_rango(BANXICO_INFLACION, f_inicio, f_fin)
        if not datos:
            continue
        valor_actual = _get_dato_mes(datos, mes_referencia.year, mes_referencia.month)
        valor_anterior = _get_dato_mes(datos, mes_comparativo.year, mes_comparativo.month)
        if valor_actual is not None and valor_anterior is not None and valor_anterior != 0:
            inflacion = ((valor_actual - valor_anterior) / valor_anterior) * 100
            nombre_mes = MESES_ES.get(mes_referencia.month, str(mes_referencia.month))
            texto_fecha = f"{nombre_mes} {mes_referencia.year}"
            return round(inflacion, 2), texto_fecha
    return None, None


def _fetch_inflacion_nacional_banxico() -> list[dict]:
    """
    Obtiene inflación mensual desde Banxico (SP1 INPC).
    Replica lógica de inflacion_nacional.ipynb: inflacion anual = (actual - anterior)/anterior * 100.
    Retorna [{anio, mes, inflacion, texto_fecha}, ...] para el histórico más completo y actual.
    Intenta mes-1, mes-2 para el más reciente (INPC se publica al mes siguiente).
    """
    if not BANXICO_TOKEN:
        return []
    hoy = datetime.now()
    # Rango amplio: 60 meses para tener 12 de comparación + 48 de inflación (4 años)
    mes_inicio = _add_months(datetime(hoy.year, hoy.month, 1), -60)
    mes_fin = _add_months(datetime(hoy.year, hoy.month, 1), 1)  # Incluir mes actual por si ya hay dato
    f_inicio = (mes_inicio - timedelta(days=5)).strftime("%Y-%m-%d")
    f_fin = (mes_fin + timedelta(days=45)).strftime("%Y-%m-%d")
    datos = _obtener_datos_banxico_rango(BANXICO_INFLACION, f_inicio, f_fin)
    if not datos:
        return []

    rows = []
    # Empezar por el mes más reciente disponible (intentar -1, luego -2)
    for offset_inicial in (1, 2):
        mes_actual = _add_months(datetime(hoy.year, hoy.month, 1), -offset_inicial)
        for _ in range(48):  # Hasta 48 meses de histórico
            mes_comparativo = _add_months(mes_actual, -12)
            valor_actual = _get_dato_mes(datos, mes_actual.year, mes_actual.month)
            valor_anterior = _get_dato_mes(datos, mes_comparativo.year, mes_comparativo.month)
            if valor_actual is not None and valor_anterior is not None and valor_anterior != 0:
                inflacion = ((valor_actual - valor_anterior) / valor_anterior) * 100
                nombre_mes = MESES_ES.get(mes_actual.month, str(mes_actual.month))
                texto_fecha = f"{nombre_mes} {mes_actual.year}"
                rows.append({
                    "anio": mes_actual.year,
                    "mes": mes_actual.month,
                    "inflacion": round(inflacion, 2),
                    "texto_fecha": texto_fecha,
                })
            mes_actual = _add_months(mes_actual, -1)
            if mes_actual < mes_inicio:
                break
        if rows:
            break  # Si obtuvimos datos con este offset, usar
    # Ordenar por año y mes
    rows.sort(key=lambda x: (x["anio"], x["mes"]))
    return rows


def get_pib_mxn() -> tuple:
    """Retorna (valor_millones_mxn, time_period) o (None, None). PIB en millones MXN."""
    return _fetch_inegi_pib()


def _format_periodo(time_period: str) -> str:
    """Formatea TIME_PERIOD '2025/03' -> '3º trimestre 2025'."""
    if not time_period:
        return "N/D"
    try:
        partes = str(time_period).split("/")
        if len(partes) == 2:
            anio, q = partes[0], partes[1]
            trimestre = f"{int(q)}º trimestre"
            return f"{trimestre} {anio}"
    except Exception:
        pass
    return str(time_period)


def get_pib_usd() -> tuple:
    """
    Retorna (valor_usd_billions, time_period) o (None, None).
    Replica cálculo del notebook: valor_usd_billions = (valor_mxn / fix) / 1000
    Dato INEGI en millones MXN (UNIT 1054) -> miles de millones USD.
    """
    valor_mxn, periodo = get_pib_mxn()
    tasa_val, _ = get_tipo_cambio()
    if valor_mxn and tasa_val and str(tasa_val) != "N/E":
        try:
            valor_mxn_num = float(str(valor_mxn).replace(",", ""))
            tasa_num = float(str(tasa_val).replace(",", ""))
            # Miles de millones USD = (millones MXN / tipo_cambio) / 1000
            valor_usd_billions = (valor_mxn_num / tasa_num) / 1000.0
            return valor_usd_billions, periodo
        except (ValueError, TypeError):
            pass
    return None, None


def _fetch_inegi_crecimiento_poblacional() -> list[dict]:
    """
    Obtiene crecimiento poblacional nacional desde API INEGI.
    Retorna lista de {year, value} ordenados por año.
    Serie 1002000001, fuente BISE (igual que pq-estudios-mercado-vps).
    """
    if not INEGI_TOKEN:
        return []
    url = (
        f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml"
        f"/INDICATOR/{SERIE_INEGI_POBLACION}/es/00/false/BISE/2.0/{INEGI_TOKEN}?type=json"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        js = resp.json()
        series = js.get("Series", [])
        if not series:
            return []
        obs = series[0].get("OBSERVATIONS", [])
        rows = []
        for o in obs:
            try:
                year = int(float(o.get("TIME_PERIOD", 0)))
                val = float(str(o.get("OBS_VALUE", "0")).replace(",", ""))
                rows.append({"year": year, "value": int(val)})
            except (ValueError, TypeError):
                continue
        return sorted(rows, key=lambda r: r["year"])
    except Exception:
        return []


def _fetch_inegi_estructura_poblacional() -> list[dict]:
    """
    Obtiene estructura poblacional por edad desde API INEGI.
    IDs por grupo: 0-4(0), 5-9(1), 10-14(2) -> pob_0_14;
    15-64 índices 3-11 -> pob_15_64; 65+ índices 12-19 -> pob_65_plus.
    """
    if not INEGI_TOKEN:
        return []
    ids_str = ",".join(IDS_INEGI_EDAD)
    url = (
        f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml"
        f"/INDICATOR/{ids_str}/es/00/false/BISE/2.0/{INEGI_TOKEN}?type=json"
    )
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        js = resp.json()
        series = js.get("Series", [])
        if len(series) < 12:
            return []
        # Agrupar por año: pob_0_14 = sum(0-2), pob_15_64 = sum(3-11), pob_65_plus = sum(12+)
        by_year: dict[int, dict] = {}
        for idx, s in enumerate(series):
            obs = s.get("OBSERVATIONS", [])
            for o in obs:
                try:
                    year = int(float(o.get("TIME_PERIOD", 0)))
                    val = float(str(o.get("OBS_VALUE", "0")).replace(",", ""))
                except (ValueError, TypeError):
                    continue
                if year not in by_year:
                    by_year[year] = {"pob_0_14": 0, "pob_15_64": 0, "pob_65_plus": 0}
                if idx <= 2:
                    by_year[year]["pob_0_14"] += val
                elif idx <= 11:
                    by_year[year]["pob_15_64"] += val
                else:
                    by_year[year]["pob_65_plus"] += val
        return [
            {"year": y, "pob_0_14": int(row["pob_0_14"]), "pob_15_64": int(row["pob_15_64"]), "pob_65_plus": int(row["pob_65_plus"])}
            for y, row in sorted(by_year.items())
        ]
    except Exception:
        return []


def _load_crecimiento_from_csv() -> list[dict]:
    """Carga crecimiento poblacional desde CSV de respaldo (data/process/)."""
    path = os.path.join(_BASE_DIR, "data", "process", "crecimiento_poblacional_nacional.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                year = int(float(row.get("year", 0)))
                val = float(str(row.get("value", "0")).replace(",", ""))
                rows.append({"year": year, "value": int(val)})
        return sorted(rows, key=lambda r: r["year"])
    except Exception:
        return []


def _get_tipo_cambio_para_conversion() -> float:
    """Obtiene tipo de cambio USD/MXN para conversión. Banxico o open.er-api.com como fallback."""
    tasa_val, _ = get_tipo_cambio()
    if tasa_val is not None:
        try:
            return float(str(tasa_val).replace(",", ""))
        except (ValueError, TypeError):
            pass
    try:
        resp = requests.get("https://open.er-api.com/v6/latest/USD", timeout=5)
        resp.raise_for_status()
        data = resp.json()
        return float(data.get("rates", {}).get("MXN", 20.0))
    except Exception:
        return 20.0


def _fetch_banco_mundo_pib_historico() -> list[dict]:
    """
    Obtiene PIB histórico anual desde Banco Mundial (pib_historico_percapita.ipynb).
    NY.GDP.MKTP.CD = PIB Total USD, NY.GDP.PCAP.CD = PIB Per Cápita USD.
    Convierte a MXN con tipo de cambio actual.
    Retorna [{anio, pib_total_mxn_billones, pib_per_capita_mxn}, ...].
    """
    tc = _get_tipo_cambio_para_conversion()
    headers = {"User-Agent": "PhiQus-EDMI/1.0"}
    base = "https://api.worldbank.org/v2/country/MX/indicator"
    params = {"format": "json", "date": "2005:2024", "per_page": 100}
    try:
        r_total = requests.get(f"{base}/NY.GDP.MKTP.CD", params=params, headers=headers, timeout=15)
        r_pc = requests.get(f"{base}/NY.GDP.PCAP.CD", params=params, headers=headers, timeout=15)
        r_total.raise_for_status()
        r_pc.raise_for_status()
        data_total = r_total.json()
        data_pc = r_pc.json()
        if len(data_total) < 2 or len(data_pc) < 2:
            return []
        by_year = {}
        for item in data_total[1]:
            if item.get("value") is not None:
                by_year[item["date"]] = {"pib_total_usd": float(item["value"])}
        for item in data_pc[1]:
            if item.get("value") is not None and item["date"] in by_year:
                by_year[item["date"]]["pib_pc_usd"] = float(item["value"])
        rows = []
        for anio_str, v in sorted(by_year.items()):
            if "pib_total_usd" in v and "pib_pc_usd" in v:
                pib_total_mxn_billones = (v["pib_total_usd"] * tc) / 1e9
                pib_per_capita_mxn = v["pib_pc_usd"] * tc
                rows.append({
                    "anio": int(anio_str),
                    "pib_total_mxn_billones": round(pib_total_mxn_billones, 2),
                    "pib_per_capita_mxn": round(pib_per_capita_mxn, 2),
                })
        return sorted(rows, key=lambda r: r["anio"])
    except Exception:
        return []


def _load_pib_historico_from_csv() -> list[dict]:
    """Carga PIB histórico Banco Mundial desde CSV de respaldo (pib_historico_banco_mundo.csv)."""
    path = os.path.join(_BASE_DIR, "data", "process", "pib_historico_banco_mundo.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                anio = int(float(row.get("anio", 0)))
                pib_total = float(str(row.get("pib_total_mxn_billones", "0")).replace(",", ""))
                pib_pc = float(str(row.get("pib_per_capita_mxn", "0")).replace(",", ""))
                rows.append({
                    "anio": anio,
                    "pib_total_mxn_billones": pib_total,
                    "pib_per_capita_mxn": pib_pc,
                })
        return sorted(rows, key=lambda x: x["anio"])
    except Exception:
        return []


def _load_pob_sector_actividad_from_csv() -> list[dict]:
    """Carga población por sector desde CSV de respaldo (data/process/)."""
    path = os.path.join(_BASE_DIR, "data", "process", "pob_sector_actividad.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                valor = float(str(row.get("valor", "0")).replace(",", ""))
                pct = float(str(row.get("pct", "0")).replace(",", ""))
                es_residual = str(row.get("es_residual", "false")).lower() == "true"
                rows.append({
                    "sector": row.get("sector", ""),
                    "valor": int(valor),
                    "pct": pct,
                    "es_residual": es_residual,
                })
        return rows
    except Exception:
        return []


def _load_pea_from_csv() -> list[dict]:
    """Carga PEA desde CSV de respaldo (data/process/)."""
    path = os.path.join(_BASE_DIR, "data", "process", "pea_inegi.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                anio = int(float(row.get("anio", 0)))
                trimestre = int(float(row.get("trimestre", 0)))
                valor = float(str(row.get("valor", "0")).replace(",", ""))
                rows.append({
                    "fecha_fmt": f"{anio}-T{trimestre}",
                    "anio": anio,
                    "trimestre": trimestre,
                    "valor": int(valor),
                })
        return sorted(rows, key=lambda x: (x["anio"], x["trimestre"]))
    except Exception:
        return []


def _load_distribucion_sexo_from_csv() -> list[dict]:
    """Carga distribución por sexo desde CSV de respaldo (data/process/)."""
    path = os.path.join(_BASE_DIR, "data", "process", "distribucion_sexo_inegi.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                year = int(float(row.get("year", 0)))
                male = float(str(row.get("male", "0")).replace(",", ""))
                female = float(str(row.get("female", "0")).replace(",", ""))
                rows.append({"year": year, "male": int(male), "female": int(female)})
        return sorted(rows, key=lambda x: x["year"])
    except Exception:
        return []


def _load_estructura_from_csv() -> list[dict]:
    """Carga estructura poblacional desde CSV de respaldo (data/process/)."""
    path = os.path.join(_BASE_DIR, "data", "process", "estructura_poblacional_inegi.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                year = int(float(row.get("year", 0)))
                p0 = float(str(row.get("pob_0_14", "0")).replace(",", ""))
                p1 = float(str(row.get("pob_15_64", "0")).replace(",", ""))
                p65 = float(str(row.get("pob_65_plus", "0")).replace(",", ""))
                rows.append({"year": year, "pob_0_14": int(p0), "pob_15_64": int(p1), "pob_65_plus": int(p65)})
        return sorted(rows, key=lambda r: r["year"])
    except Exception:
        return []


def _estimar_poblacion_nacional(year: int) -> float:
    """Estimación lineal para cálculos per cápita (Base Censo 2020)."""
    return 126_014_000 + (year - 2020) * 1_000_000


def _fetch_inegi_pib_nacional() -> list[dict]:
    """
    Obtiene PIB nacional trimestral desde API INEGI.
    Serie 733671 (BIE-BISE). TIME_PERIOD YYYY/MM (trimestre).
    pib_per_capita = (pib_total_millones * 1e6) / poblacion_estimada.
    Retorna [{fecha, anio, trimestre, pib_total_millones, pib_per_capita}, ...].
    """
    if not INEGI_TOKEN:
        return []
    url = (
        f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml"
        f"/INDICATOR/{SERIE_INEGI_PIB_NACIONAL}/es/00/true/BIE-BISE/2.0/{INEGI_TOKEN}?type=json"
    )
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        js = resp.json()
        series = js.get("Series", [])
        if not series:
            return []
        obs = series[0].get("OBSERVATIONS", [])
        rows = []
        for o in obs:
            tp = o.get("TIME_PERIOD", "")
            if "/" not in str(tp):
                continue
            parts = str(tp).split("/")
            anio = int(parts[0])
            trim = int(parts[1])
            val_millions = float(str(o.get("OBS_VALUE", "0")).replace(",", ""))
            pob = _estimar_poblacion_nacional(anio)
            mes = (trim - 1) * 3 + 1
            fecha = f"{anio}-{mes:02d}-01"
            pib_per_capita = (val_millions * 1_000_000) / pob if pob else 0
            rows.append({
                "fecha": fecha,
                "anio": anio,
                "trimestre": trim,
                "pib_total_millones": val_millions,
                "pib_per_capita": pib_per_capita,
            })
        return sorted(rows, key=lambda r: (r["anio"], r["trimestre"]))
    except Exception:
        return []


def _extract_latest_obs(series_data: dict) -> tuple[float, str]:
    """Extrae el último valor y periodo de una serie INEGI. Retorna (valor, time_period)."""
    try:
        series = series_data.get("Series", [])
        if not series:
            return None, None
        obs = series[0].get("OBSERVATIONS", [])
        if not obs:
            return None, None
        sorted_obs = sorted(obs, key=lambda x: x.get("TIME_PERIOD", ""), reverse=True)
        o = sorted_obs[0]
        val = float(str(o.get("OBS_VALUE", "0")).replace(",", ""))
        tp = o.get("TIME_PERIOD", "")
        return val, tp
    except (TypeError, ValueError, KeyError):
        return None, None


def _fetch_inegi_pob_sector_actividad() -> list[dict]:
    """
    Obtiene población ocupada por sector desde API INEGI.
    IDs: 289242 (ocupados total), 668907 (primario%), 668908 (secundario%), 668912 (terciario%).
    Retorna [{sector, valor, pct, es_residual}, ...].
    """
    if not INEGI_TOKEN:
        return []
    ids_str = ",".join(IDS_INEGI_SECTOR)
    url = (
        f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml"
        f"/INDICATOR/{ids_str}/es/00/false/BIE-BISE/2.0/{INEGI_TOKEN}?type=json"
    )
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        js = resp.json()
        series = js.get("Series", [])
        if len(series) < 4:
            return []
        # series[0]=289242 ocupados, [1]=668907 prim%, [2]=668908 sec%, [3]=668912 terc%
        ocupados_val, _ = _extract_latest_obs({"Series": [series[0]]})
        pct_prim, _ = _extract_latest_obs({"Series": [series[1]]})
        pct_sec, _ = _extract_latest_obs({"Series": [series[2]]})
        pct_terc, _ = _extract_latest_obs({"Series": [series[3]]})
        if ocupados_val is None:
            return []
        if ocupados_val < 200:  # API a veces trae millones
            ocupados_val *= 1_000_000
        usando_residual = False
        if pct_sec is not None and pct_terc is not None:
            if pct_prim is None or pct_prim > 80:
                pct_prim = 100.0 - pct_sec - pct_terc - 0.6
                usando_residual = True
        else:
            pct_sec = pct_sec if pct_sec is not None else 0
            pct_terc = pct_terc if pct_terc is not None else 0
            pct_prim = pct_prim if pct_prim is not None else 0
        total_pct = (pct_prim or 0) + (pct_sec or 0) + (pct_terc or 0)
        if total_pct > 0:
            factor = 100.0 / total_pct
            pct_prim = (pct_prim or 0) * factor
            pct_sec = (pct_sec or 0) * factor
            pct_terc = (pct_terc or 0) * factor
        val_prim = ocupados_val * (pct_prim / 100)
        val_sec = ocupados_val * (pct_sec / 100)
        val_terc = ocupados_val * (pct_terc / 100)
        return [
            {"sector": "Primario (Agro)", "valor": int(val_prim), "pct": round(pct_prim, 2), "es_residual": usando_residual},
            {"sector": "Secundario (Industria)", "valor": int(val_sec), "pct": round(pct_sec, 2), "es_residual": False},
            {"sector": "Terciario (Servicios)", "valor": int(val_terc), "pct": round(pct_terc, 2), "es_residual": False},
        ]
    except Exception:
        return []


def _fetch_inegi_pea() -> list[dict]:
    """
    Obtiene PEA (Población Económicamente Activa) desde API INEGI.
    Serie 601614 (BIE-BISE), fallback 289244 (BIE).
    TIME_PERIOD formato "2025/03" -> anio, trimestre.
    Retorna [{fecha_fmt, anio, trimestre, valor}, ...].
    """
    if not INEGI_TOKEN:
        return []
    for serie in (SERIE_INEGI_PEA, SERIE_INEGI_PEA_FALLBACK):
        source = "BIE-BISE" if serie == SERIE_INEGI_PEA else "BIE"
        url = (
            f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml"
            f"/INDICATOR/{serie}/es/00/true/{source}/2.0/{INEGI_TOKEN}?type=json"
        )
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            js = resp.json()
            series = js.get("Series", [])
            if not series:
                continue
            obs = series[0].get("OBSERVATIONS", [])
            rows = []
            for o in obs:
                tp = o.get("TIME_PERIOD", "")
                if "/" not in str(tp):
                    continue
                parts = str(tp).split("/")
                anio = int(parts[0])
                trimestre = int(parts[1])
                val = float(str(o.get("OBS_VALUE", "0")).replace(",", ""))
                rows.append({
                    "fecha_fmt": f"{anio}-T{trimestre}",
                    "anio": anio,
                    "trimestre": trimestre,
                    "valor": int(val),
                })
            return sorted(rows, key=lambda r: (r["anio"], r["trimestre"]))
        except Exception:
            continue
    return []


def _fetch_inegi_distribucion_sexo() -> list[dict]:
    """
    Obtiene distribución de población por sexo desde API INEGI.
    IDs: 1002000002 (Hombres), 1002000003 (Mujeres), fuente BISE.
    Retorna [{year, male, female}, ...].
    """
    if not INEGI_TOKEN:
        return []
    ids_str = ",".join(IDS_INEGI_SEXO)
    url = (
        f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml"
        f"/INDICATOR/{ids_str}/es/00/false/BISE/2.0/{INEGI_TOKEN}?type=json"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        js = resp.json()
        series = js.get("Series", [])
        if len(series) < 2:
            return []
        # series[0] = Hombres, series[1] = Mujeres
        obs_h = {int(float(o.get("TIME_PERIOD", 0))): float(str(o.get("OBS_VALUE", "0")).replace(",", ""))
                 for o in series[0].get("OBSERVATIONS", [])}
        obs_m = {int(float(o.get("TIME_PERIOD", 0))): float(str(o.get("OBS_VALUE", "0")).replace(",", ""))
                 for o in series[1].get("OBSERVATIONS", [])}
        years = sorted(set(obs_h.keys()) | set(obs_m.keys()))
        return [
            {
                "year": y,
                "male": int(obs_h.get(y, 0)),
                "female": int(obs_m.get(y, 0)),
            }
            for y in years
        ]
    except Exception:
        return []


def _load_inflacion_nacional_from_csv() -> list[dict]:
    """Carga inflación nacional desde CSV de respaldo."""
    path = os.path.join(_BASE_DIR, "data", "process", "inflacion_nacional.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                anio = int(float(row.get("anio", 0)))
                mes = int(float(row.get("mes", 0)))
                inf = float(str(row.get("inflacion", "0")).replace(",", ""))
                texto = row.get("texto_fecha", f"{MESES_ES.get(mes, mes)} {anio}")
                rows.append({"anio": anio, "mes": mes, "inflacion": inf, "texto_fecha": texto})
        return sorted(rows, key=lambda x: (x["anio"], x["mes"]))
    except Exception:
        return []


def get_tipo_cambio_historico() -> dict:
    """
    Obtiene tipo de cambio histórico (diario y mensual).
    Prioridad: 1) PostgreSQL, 2) API Banxico, 3) CSV.
    Retorna {diario: [...], mensual: [...]}.
    """
    from services.db import get_tipo_cambio_diario_from_db, get_tipo_cambio_mensual_from_db, save_tipo_cambio_to_db

    diario = get_tipo_cambio_diario_from_db()
    mensual = get_tipo_cambio_mensual_from_db()
    if diario and mensual:
        return {"diario": diario, "mensual": mensual}
    diario, mensual = _fetch_tipo_cambio_banxico()
    if not diario:
        diario = _load_tipo_cambio_diario_from_csv()
        mensual = _load_tipo_cambio_mensual_from_csv()
        if diario and not mensual:
            by_month: dict[str, list[float]] = {}
            for r in diario:
                ym = r["fecha"][:7] if len(r["fecha"]) >= 7 else ""
                if ym:
                    by_month.setdefault(ym, []).append(r["tc"])
            mensual = [{"fecha": f"{ym}-01", "tc_prom_mes": round(sum(v) / len(v), 4)} for ym, v in sorted(by_month.items())]
    if diario and mensual:
        save_tipo_cambio_to_db(diario, mensual)
    return {"diario": diario or [], "mensual": mensual or []}


def _load_tipo_cambio_diario_from_csv() -> list[dict]:
    """Carga tipo de cambio diario desde CSV de respaldo."""
    path = os.path.join(_BASE_DIR, "data", "process", "tipo_cambio_banxico_diario.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                fecha = row.get("fecha", "")
                tc = float(str(row.get("tc", row.get("dato", "0"))).replace(",", ""))
                rows.append({"fecha": fecha, "tc": tc})
        return sorted(rows, key=lambda x: x["fecha"])
    except Exception:
        return []


def _load_tipo_cambio_mensual_from_csv() -> list[dict]:
    """Carga tipo de cambio mensual desde CSV de respaldo."""
    path = os.path.join(_BASE_DIR, "data", "process", "tipo_cambio_banxico_mensual.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                fecha = row.get("fecha", "")
                tc = float(str(row.get("tc_prom_mes", "0")).replace(",", ""))
                rows.append({"fecha": fecha, "tc_prom_mes": tc})
        return sorted(rows, key=lambda x: x["fecha"])
    except Exception:
        return []


def get_inflacion_nacional() -> list[dict]:
    """
    Obtiene inflación nacional mensual (Banxico INPC).
    Prioridad: 1) PostgreSQL, 2) API Banxico, 3) CSV de respaldo.
    Si obtiene de Banxico, guarda en BD.
    Retorna [{anio, mes, inflacion, texto_fecha}, ...].
    """
    from services.db import get_inflacion_nacional_from_db, save_inflacion_nacional_to_db

    db_data = get_inflacion_nacional_from_db()
    if db_data:
        return db_data
    data = _fetch_inflacion_nacional_banxico()
    if not data:
        data = _load_inflacion_nacional_from_csv()
    if data:
        save_inflacion_nacional_to_db(data)
    return data


def get_estructura_poblacional() -> list[dict]:
    """
    Obtiene estructura poblacional por edad.
    Prioridad: 1) PostgreSQL, 2) API INEGI, 3) CSV de respaldo.
    """
    from services.db import get_estructura_poblacional_from_db

    db_data = get_estructura_poblacional_from_db()
    if db_data:
        return db_data
    inegi_data = _fetch_inegi_estructura_poblacional()
    if inegi_data:
        return inegi_data
    return _load_estructura_from_csv()


def get_crecimiento_poblacional_nacional() -> list[dict]:
    """
    Obtiene datos de crecimiento poblacional nacional.
    Prioridad: 1) PostgreSQL, 2) API INEGI, 3) CSV de respaldo.
    """
    from services.db import get_crecimiento_poblacional_from_db

    db_data = get_crecimiento_poblacional_from_db()
    if db_data:
        return db_data
    inegi_data = _fetch_inegi_crecimiento_poblacional()
    if inegi_data:
        return inegi_data
    return _load_crecimiento_from_csv()


def get_distribucion_sexo() -> list[dict]:
    """
    Obtiene distribución de población por sexo.
    Prioridad: 1) PostgreSQL, 2) API INEGI, 3) CSV de respaldo.
    Retorna [{year, male, female}, ...].
    """
    from services.db import get_distribucion_sexo_from_db

    db_data = get_distribucion_sexo_from_db()
    if db_data:
        return db_data
    inegi_data = _fetch_inegi_distribucion_sexo()
    if inegi_data:
        return inegi_data
    return _load_distribucion_sexo_from_csv()


def get_pea() -> list[dict]:
    """
    Obtiene PEA (Población Económicamente Activa).
    Prioridad: 1) PostgreSQL, 2) API INEGI, 3) CSV de respaldo.
    Retorna [{fecha_fmt, anio, trimestre, valor}, ...].
    """
    from services.db import get_pea_from_db

    db_data = get_pea_from_db()
    if db_data:
        return db_data
    inegi_data = _fetch_inegi_pea()
    if inegi_data:
        return inegi_data
    return _load_pea_from_csv()


def get_pob_sector_actividad() -> list[dict]:
    """
    Obtiene población ocupada por sector de actividad.
    Prioridad: 1) PostgreSQL, 2) API INEGI, 3) CSV de respaldo.
    Retorna [{sector, valor, pct, es_residual}, ...].
    """
    from services.db import get_pob_sector_actividad_from_db

    db_data = get_pob_sector_actividad_from_db()
    if db_data:
        return db_data
    inegi_data = _fetch_inegi_pob_sector_actividad()
    if inegi_data:
        return inegi_data
    return _load_pob_sector_actividad_from_csv()


def get_pib_nacional() -> list[dict]:
    """
    Obtiene PIB histórico anual (Banco Mundial, pib_historico_percapita.ipynb).
    Prioridad: 1) API Banco Mundial, 2) CSV de respaldo.
    Retorna [{anio, pib_total_mxn_billones, pib_per_capita_mxn}, ...].
    """
    data = _fetch_banco_mundo_pib_historico()
    if data:
        return data
    return _load_pib_historico_from_csv()


# -----------------------------------------------------------------------------
# Proyección PIB (FMI WEO) - pib_proyeccion.ipynb
# -----------------------------------------------------------------------------
BASE_IMF = "https://www.imf.org/external/datamapper/api/v1"
IMF_INDICATORS = {"total": "NGDPD", "pc": "NGDPDPC"}  # Billions USD, USD per capita
IMF_COUNTRY = "MEX"
YEARS_AHEAD_PROYECCION = 5


def _fetch_imf_series(indicator_id: str, country_id: str) -> dict[int, float]:
    """Obtiene serie del FMI (DataMapper API). Retorna {año: valor}."""
    url = f"{BASE_IMF}/{indicator_id}/{country_id}"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        payload = resp.json()
        node = payload.get("values", {}).get(indicator_id, {}).get(country_id, {})
        if not node:
            return {}
        return {int(y): float(v) for y, v in node.items() if v is not None}
    except Exception:
        return {}


def _fetch_proyeccion_pib_fmi() -> tuple[list[dict], float, str]:
    """
    Obtiene proyección PIB desde FMI WEO.
    Retorna ([(anio, pib_total_mxn_billones, pib_total_usd_billones, pib_per_capita_mxn, pib_per_capita_usd), ...], tc_fix, tc_date).
    """
    tc_fix, tc_date = get_tipo_cambio()
    fx = float(str(tc_fix).replace(",", "")) if tc_fix else _get_tipo_cambio_para_conversion()
    tc_date_str = tc_date or "Estimado"

    s_total = _fetch_imf_series(IMF_INDICATORS["total"], IMF_COUNTRY)
    s_pc = _fetch_imf_series(IMF_INDICATORS["pc"], IMF_COUNTRY)
    if not s_total or not s_pc:
        return [], fx, tc_date_str

    start_year = datetime.now().year
    all_years = sorted(set(s_total.keys()) | set(s_pc.keys()))
    target_years = [y for y in all_years if y >= start_year][:YEARS_AHEAD_PROYECCION]
    if not target_years:
        return [], fx, tc_date_str

    rows = []
    for anio in target_years:
        v_total_usd = s_total.get(anio)
        v_pc_usd = s_pc.get(anio)
        if v_total_usd is None or v_pc_usd is None:
            continue
        pib_total_mxn = v_total_usd * fx  # Billones MXN (NGDPD ya en billions USD)
        pib_pc_mxn = v_pc_usd * fx
        rows.append({
            "anio": anio,
            "pib_total_mxn_billones": round(pib_total_mxn, 2),
            "pib_total_usd_billones": round(v_total_usd, 2),
            "pib_per_capita_mxn": round(pib_pc_mxn, 2),
            "pib_per_capita_usd": round(v_pc_usd, 2),
        })
    return rows, fx, tc_date_str


def _load_proyeccion_pib_from_csv() -> tuple[list[dict], float, str]:
    """Carga proyección PIB desde CSV de respaldo."""
    path = os.path.join(_BASE_DIR, "data", "process", "pib_proyeccion_fmi.csv")
    if not os.path.isfile(path):
        return [], 20.0, "Estimado"
    rows = []
    try:
        with open(path, encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                anio = int(float(row.get("anio", 0)))
                pib_total_mxn = float(str(row.get("pib_total_mxn_billones", "0")).replace(",", ""))
                pib_total_usd = float(str(row.get("pib_total_usd_billones", "0")).replace(",", ""))
                pib_pc_mxn = float(str(row.get("pib_per_capita_mxn", "0")).replace(",", ""))
                pib_pc_usd = float(str(row.get("pib_per_capita_usd", "0")).replace(",", ""))
                rows.append({
                    "anio": anio,
                    "pib_total_mxn_billones": pib_total_mxn,
                    "pib_total_usd_billones": pib_total_usd,
                    "pib_per_capita_mxn": pib_pc_mxn,
                    "pib_per_capita_usd": pib_pc_usd,
                })
        tc = _get_tipo_cambio_para_conversion() if rows else 20.0
        return sorted(rows, key=lambda x: x["anio"]), tc, "CSV (respaldo)"
    except Exception:
        return [], 20.0, "Estimado"


def get_proyeccion_pib() -> dict:
    """
    Obtiene proyección PIB (FMI WEO).
    Prioridad: 1) PostgreSQL, 2) API FMI, 3) CSV de respaldo.
    Si obtiene de FMI, guarda en BD para solicitudes siguientes.
    Retorna {data: [...], tc_fix: float, tc_date: str}.
    """
    from services.db import get_proyeccion_pib_from_db, save_proyeccion_pib_to_db

    db_result = get_proyeccion_pib_from_db()
    if db_result and db_result.get("data"):
        return db_result
    data, tc_fix, tc_date = _fetch_proyeccion_pib_fmi()
    if not data:
        data, tc_fix, tc_date = _load_proyeccion_pib_from_csv()
    if data:
        save_proyeccion_pib_to_db(data, tc_fix, tc_date)
    return {"data": data, "tc_fix": tc_fix, "tc_date": tc_date}


# -----------------------------------------------------------------------------
# IED por Sector Económico (Secretaría de Economía - pq-estudios-mercado-vps)
# -----------------------------------------------------------------------------
SCIAN_SECTORES = {
    "11": "Agroindustria",
    "21": "Minería",
    "22": "Elec./Agua/Gas",
    "23": "Construcción",
    "31": "Manufactura",
    "32": "Manufactura",
    "33": "Manufactura",
    "43": "Comercio Mayor",
    "46": "Comercio Menor",
    "48": "Transporte/Logística",
    "49": "Transporte/Logística",
    "51": "Info. Medios",
    "52": "Serv. Financieros",
    "53": "Inmobiliarios",
    "54": "Serv. Profesionales",
    "55": "Corporativos",
    "56": "Apoyo Negocios",
    "61": "Educación",
    "62": "Salud",
    "71": "Cultura/Deporte",
    "72": "Alojamiento/Alimentos",
    "81": "Otros Servicios",
}


def _normalize_str(s: str) -> str:
    import unicodedata

    if not isinstance(s, str):
        return ""
    return "".join(
        c
        for c in unicodedata.normalize("NFD", s.strip())
        if unicodedata.category(c) != "Mn"
    ).lower()


def _fetch_ckan_ied_sectores_url() -> str | None:
    """Descubre URL de descarga IED sectores en datos.gob.mx (CKAN)."""
    url = "https://www.datos.gob.mx/api/3/action/package_show?id=inversion_extranjera_directa"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return None
        data = r.json()
        resources = data.get("result", {}).get("resources", [])
        for res in resources:
            name = _normalize_str(
                (res.get("name") or "") + " " + (res.get("description") or "")
            )
            if all(
                k in name for k in ["sector", "subsector"]
            ) and (res.get("format") or "").lower() in ["csv", "txt"]:
                return res.get("url")
    except Exception:
        pass
    return None


def _parse_ied_monto(val: str) -> float:
    try:
        s = str(val).strip().replace("$", "").replace(",", "").replace(" ", "")
        if s in ("", "C", "N/E", "N/D"):
            return 0.0
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _extract_scian_code(sector_str: str) -> str:
    import re

    s = str(sector_str).strip()
    m = re.match(r"^(\d{2})", s)
    return m.group(1) if m else ""


def _fetch_and_process_ied_sectores() -> list[dict]:
    """
    Obtiene IED sectores desde datos.gob.mx, procesa con SCIAN y retorna
    [{sector, monto_mdd, periodo}, ...] para el trimestre más reciente.
    """
    url = _fetch_ckan_ied_sectores_url()
    if not url:
        return []
    try:
        r = requests.get(url, timeout=90)
        r.raise_for_status()
        content = r.content.decode("utf-8-sig", errors="replace")
    except Exception:
        return []

    # Detectar columnas (puede tener millones_de_dolares, fn_millones_de_dolares, monto, etc)
    reader_preview = csv.reader(StringIO(content))
    header_row = next(reader_preview, None)
    if not header_row or len(header_row) < 2:
        return []
    headers_lower = {h.strip().lower(): h.strip() for h in header_row}
    val_col = None
    for k in ["fn_millones_de_dolares", "millones_de_dolares", "monto", "valor", "millones"]:
        for hk, hv in headers_lower.items():
            if k in hk:
                val_col = hv
                break
        if val_col:
            break
    sec_col = None
    for k in ["sector_subsector_rama", "sector"]:
        if k in headers_lower:
            sec_col = k
            break
    anio_col = None
    for k in ["anio", "año", "year"]:
        if k in headers_lower:
            anio_col = headers_lower[k]
            break
    trim_col = None
    for k in ["trimestre", "quarter"]:
        if k in headers_lower:
            trim_col = headers_lower[k]
            break
    if not val_col or not sec_col or not anio_col or not trim_col:
        return []

    # Parsear CSV
    reader = csv.DictReader(StringIO(content), delimiter=",")
    rows = []
    for r in reader:
        val = _parse_ied_monto(r.get(val_col, r.get("millones_de_dolares", "0")))
        sec = r.get(sec_col, "")
        anio = int(float(r.get(anio_col, 0) or 0))
        trim = int(float(r.get(trim_col, 0) or 0))
        if anio > 0 and trim > 0:
            rows.append({"anio": anio, "trim": trim, "sector_str": sec, "val": val})

    if not rows:
        return []

    # Último periodo
    max_t = max(r["anio"] * 4 + r["trim"] for r in rows)
    filtered = [r for r in rows if r["anio"] * 4 + r["trim"] == max_t]

    # Agrupar por sector SCIAN
    by_sector: dict[str, float] = {}
    for r in filtered:
        code = _extract_scian_code(r["sector_str"])
        sector_name = SCIAN_SECTORES.get(code, "Otros")
        by_sector[sector_name] = by_sector.get(sector_name, 0) + r["val"]

    max_y = max(r["anio"] for r in filtered)
    max_q = max(r["trim"] for r in filtered if r["anio"] == max_y)
    periodo = f"{max_y}-T{max_q}"

    return [
        {"sector": s, "monto_mdd": round(v, 2), "periodo": periodo}
        for s, v in sorted(by_sector.items(), key=lambda x: -x[1])
    ]


def _load_ied_sectores_from_csv() -> list[dict]:
    """Carga IED sectores desde CSV de respaldo (data/process/ied_sectores.csv)."""
    path = os.path.join(_BASE_DIR, "data", "process", "ied_sectores.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                sector = row.get("sector", "")
                monto = float(str(row.get("monto_mdd", "0")).replace(",", ""))
                periodo = row.get("periodo", "")
                rows.append({"sector": sector, "monto_mdd": monto, "periodo": periodo})
        return sorted(rows, key=lambda x: -x["monto_mdd"])
    except Exception:
        return []


# -----------------------------------------------------------------------------
# IED Flujo por Entidad (últimos 4 trimestres - inversion_extranjera_ied.ipynb)
# -----------------------------------------------------------------------------
ESTADO_ALIASES = {
    "cdmx": "Ciudad de México",
    "ciudad de mexico": "Ciudad de México",
    "distrito federal": "Ciudad de México",
    "edomex": "Estado de México",
    "mexico": "Estado de México",
    "coahuila de zaragoza": "Coahuila",
    "veracruz de ignacio de la llave": "Veracruz",
    "michoacan de ocampo": "Michoacán",
    "quintana roo": "Quintana Roo",
    "yucatan": "Yucatán",
    "nuevo leon": "Nuevo León",
    "san luis potosi": "San Luis Potosí",
    "baja california": "Baja California",
    "baja california sur": "Baja California Sur",
}


def _pick_ied_flujo_resource_url(resources: list) -> str | None:
    """
    Selecciona el recurso CSV 'por entidad' con granularidad trimestral.
    Replica pick_resource_url del notebook inversion_extranjera_ied.ipynb.
    """
    candidates = []
    for res in resources:
        fmt = (res.get("format") or "").lower()
        name = (res.get("name") or "") + " " + (res.get("description") or "")
        name_l = _normalize_str(name)
        if fmt != "csv":
            continue
        score = 0
        if "entidad" in name_l:
            score += 2
        if "trimestre" in name_l or "trimestral" in name_l:
            score += 2
        if "tipo" in name_l:
            score += 1
        if score > 0:
            updated = res.get("last_modified") or res.get("revision_timestamp") or ""
            candidates.append((score, updated, res.get("url")))
    if not candidates:
        for res in resources:
            fmt = (res.get("format") or "").lower()
            name = (res.get("name") or "") + " " + (res.get("description") or "")
            name_l = _normalize_str(name)
            if fmt == "csv" and "entidad" in name_l:
                updated = res.get("last_modified") or res.get("revision_timestamp") or ""
                candidates.append((1, updated, res.get("url")))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return candidates[0][2]


def _detectar_columnas_ied_flujo(header_keys: list[str]) -> tuple[str | None, str | None, str | None, str | None]:
    """Detecta columnas: entidad, anio, trimestre, valor. Replica detectar_columnas del notebook."""
    cols = {c.lower().strip(): c for c in header_keys}
    col_entidad = next(
        (cols[k] for k in cols if "entidad" in k and "federativa" in k), None
    ) or next((cols[k] for k in cols if "entidad" in k), None)
    if not col_entidad:
        col_entidad = next((cols[k] for k in cols if "entidad" in k), None)
    if not col_entidad:
        col_entidad = next((cols[k] for k in cols if "estado" in k), None)
    col_anio = next((cols.get(k) for k in ("anio", "año", "year") if k in cols), None)
    col_trim = next((cols[k] for k in cols if "trimestre" in k or k in ("trim", "quarter")), None)
    col_valor = None
    for key in ["usd", "mdd", "millones", "valor", "monto"]:
        col_valor = next((cols[k] for k in cols if key in k), None)
        if col_valor:
            break
    return col_entidad, col_anio, col_trim, col_valor


def _rolling_4t_ied(
    rows: list[dict],
    col_entidad: str,
    col_anio: str,
    col_trim: str,
    col_valor: str,
) -> tuple[list[dict], str]:
    """
    Suma últimos 4 trimestres por entidad. Replica rolling_4t del notebook.
    Retorna ([{entidad, mdd_4t, rank, periodo}, ...], periodo).
    """
    if not rows or not all([col_entidad, col_anio, col_trim, col_valor]):
        return [], ""

    valid = []
    for r in rows:
        try:
            anio = int(float(str(r.get(col_anio, 0) or 0)))
            trim_raw = str(r.get(col_trim, "") or "")
            trim_match = re.search(r"(\d+)", trim_raw)
            trim = int(trim_match.group(1)) if trim_match else 0
            val = float(str(r.get(col_valor, 0) or 0).replace(",", "").replace("$", ""))
            ent = str(r.get(col_entidad, "") or "").strip()
            if ent and ent.lower() not in ("total", "nacional", "no especificado"):
                valid.append({"entidad": ent, "anio": anio, "trim": trim, "val": val})
        except (ValueError, TypeError):
            continue

    if not valid:
        return [], ""

    max_y = max(r["anio"] for r in valid)
    max_q = max(r["trim"] for r in valid if r["anio"] == max_y)
    t_max = max_y * 4 + max_q
    window = {t_max, t_max - 1, t_max - 2, t_max - 3}

    filtrados = [r for r in valid if r["anio"] * 4 + r["trim"] in window]
    by_ent: dict[str, float] = {}
    for r in filtrados:
        ent = r["entidad"]
        by_ent[ent] = by_ent.get(ent, 0) + r["val"]

    sorted_ent = sorted(by_ent.items(), key=lambda x: -x[1])
    periodo = f"{max_y}-T{max_q}"
    result = [
        {"entidad": ent, "mdd_4t": round(v, 2), "rank": i + 1, "periodo": periodo}
        for i, (ent, v) in enumerate(sorted_ent, 1)
    ]
    return result, periodo


def _fetch_and_process_ied_flujo() -> list[dict]:
    """
    Obtiene IED flujo por entidad desde datos.gob.mx (CKAN).
    Replica lógica de inversion_extranjera_ied.ipynb.
    """
    url = "https://www.datos.gob.mx/api/3/action/package_show?id=inversion_extranjera_directa"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            return []
        resources = data.get("result", {}).get("resources", [])
        resource_url = _pick_ied_flujo_resource_url(resources)
        if not resource_url:
            return []
    except Exception:
        return []

    try:
        resp = requests.get(resource_url, timeout=90)
        resp.raise_for_status()
        content = resp.content.decode("utf-8-sig", errors="replace")
    except Exception:
        return []

    reader = csv.DictReader(StringIO(content))
    header_row = reader.fieldnames or []
    col_entidad, col_anio, col_trim, col_valor = _detectar_columnas_ied_flujo(header_row)
    if not all([col_entidad, col_anio, col_trim, col_valor]):
        return []

    rows = list(reader)
    result, _ = _rolling_4t_ied(rows, col_entidad, col_anio, col_trim, col_valor)
    return result


def _load_ied_flujo_from_csv() -> list[dict]:
    """Carga IED flujo desde CSV de respaldo (data/process/ied_flujo.csv)."""
    path = os.path.join(_BASE_DIR, "data", "process", "ied_flujo.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                ent = row.get("entidad", "")
                mdd = float(str(row.get("mdd_4t", "0")).replace(",", ""))
                rank = int(float(row.get("rank", 0)))
                periodo = row.get("periodo_corte", row.get("periodo", ""))
                rows.append({"entidad": ent, "mdd_4t": mdd, "rank": rank, "periodo": periodo})
        return sorted(rows, key=lambda x: x["rank"])
    except Exception:
        return []


def get_ied_flujo_entidad() -> list[dict]:
    """
    Obtiene IED flujo por entidad (últimos 4 trimestres).
    Prioridad: 1) PostgreSQL, 2) API datos.gob.mx, 3) CSV.
    Retorna [{entidad, mdd_4t, rank, periodo}, ...].
    """
    from services.db import get_ied_flujo_entidad_from_db, save_ied_flujo_entidad_to_db

    db_data = get_ied_flujo_entidad_from_db()
    if db_data:
        return db_data
    data = _fetch_and_process_ied_flujo()
    if not data:
        data = _load_ied_flujo_from_csv()
    if data:
        save_ied_flujo_entidad_to_db(data)
    return data


def get_ied_sectores() -> list[dict]:
    """
    Obtiene IED por sector económico.
    Prioridad: 1) PostgreSQL, 2) API datos.gob.mx + procesamiento, 3) CSV de respaldo.
    Retorna [{sector, monto_mdd, periodo}, ...].
    """
    from services.db import get_ied_sectores_from_db, save_ied_sectores_to_db

    db_data = get_ied_sectores_from_db()
    if db_data:
        return db_data
    data = _fetch_and_process_ied_sectores()
    if not data:
        data = _load_ied_sectores_from_csv()
    if data:
        save_ied_sectores_to_db(data)
    return data


# -----------------------------------------------------------------------------
# IED por País de Origen (pq-estudios-mercado-vps proc_investment)
# -----------------------------------------------------------------------------
def _fetch_ckan_ied_paises_url() -> str | None:
    """Descubre URL de descarga IED por país en datos.gob.mx (CKAN)."""
    url = "https://www.datos.gob.mx/api/3/action/package_show?id=inversion_extranjera_directa"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return None
        data = r.json()
        resources = data.get("result", {}).get("resources", [])
        for res in resources:
            name = _normalize_str(
                (res.get("name") or "") + " " + (res.get("description") or "")
            )
            if all(
                k in name for k in ["pais", "origen"]
            ) and (res.get("format") or "").lower() in ["csv", "txt"]:
                return res.get("url")
    except Exception:
        pass
    return None


def _fetch_and_process_ied_paises() -> list[dict]:
    """
    Obtiene IED por país desde datos.gob.mx, procesa top10 + Otros.
    Replica proc_investment process_ied_detalle países.
    """
    resource_url = _fetch_ckan_ied_paises_url()
    if not resource_url:
        return []
    try:
        resp = requests.get(resource_url, timeout=90)
        resp.raise_for_status()
        content = resp.content.decode("utf-8-sig", errors="replace")
    except Exception:
        return []

    reader_preview = csv.reader(StringIO(content))
    header_row = next(reader_preview, None)
    if not header_row or len(header_row) < 2:
        return []
    headers_lower = {h.strip().lower(): h.strip() for h in header_row}

    col_pais = next(
        (headers_lower[k] for k in headers_lower if "pais" in k and "origen" in k), None
    )
    if not col_pais:
        col_pais = next((headers_lower[k] for k in headers_lower if "pais" in k), None)
    col_anio = next((headers_lower.get(k) for k in ("anio", "año", "year") if k in headers_lower), None)
    col_trim = next(
        (headers_lower[k] for k in headers_lower if "trimestre" in k or k in ("trim", "quarter")),
        None,
    )
    col_valor = None
    for key in ["fn_millones_de_dolares", "millones_de_dolares", "monto", "valor", "millones"]:
        col_valor = next((headers_lower[k] for k in headers_lower if key in k), None)
        if col_valor:
            break

    if not all([col_pais, col_anio, col_trim, col_valor]):
        return []

    reader = csv.DictReader(StringIO(content))

    rows = []
    for r in reader:
        try:
            anio = int(float(r.get(col_anio, 0) or 0))
            trim_raw = str(r.get(col_trim, "") or "")
            trim_match = re.search(r"(\d+)", trim_raw)
            trim = int(trim_match.group(1)) if trim_match else 0
            val = _parse_ied_monto(r.get(col_valor, r.get("millones_de_dolares", "0")))
            pais = str(r.get(col_pais, "") or "").strip()
            if pais:
                rows.append({"pais": pais, "anio": anio, "trim": trim, "val": val})
        except (ValueError, TypeError):
            continue

    if not rows:
        return []

    max_y = max(r["anio"] for r in rows)
    max_q = max(r["trim"] for r in rows if r["anio"] == max_y)
    filtrados = [r for r in rows if r["anio"] == max_y and r["trim"] == max_q]

    by_pais: dict[str, float] = {}
    for r in filtrados:
        p = r["pais"]
        by_pais[p] = by_pais.get(p, 0) + r["val"]

    sorted_p = sorted(by_pais.items(), key=lambda x: -x[1])
    top10 = sorted_p[:10]
    if len(sorted_p) > 10:
        otros_sum = sum(v for _, v in sorted_p[10:])
        top10.append(("Otros", round(otros_sum, 2)))

    periodo = f"{max_y}-T{max_q}"
    return [
        {"pais": p, "monto_mdd": round(float(v), 2), "periodo": periodo}
        for p, v in top10
    ]


def _load_ied_paises_from_csv() -> list[dict]:
    """Carga IED por país desde CSV de respaldo (data/process/ied_paises.csv)."""
    path = os.path.join(_BASE_DIR, "data", "process", "ied_paises.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                pais = row.get("pais", "")
                monto = float(str(row.get("monto_mdd", "0")).replace(",", ""))
                periodo = row.get("periodo", "")
                rows.append({"pais": pais, "monto_mdd": monto, "periodo": periodo})
        return rows
    except Exception:
        return []


# -----------------------------------------------------------------------------
# Ranking Turismo Mundial (Banco Mundial WDI - pq-estudios-mercado-vps)
# -----------------------------------------------------------------------------
WB_TOURISM_URL = "https://api.worldbank.org/v2/country/all/indicator/ST.INT.RCPT.CD?format=json&per_page=20000"


def _fetch_and_process_ranking_turismo_wb() -> list[dict]:
    """
    Obtiene ranking turismo mundial desde World Bank API.
    Replica proc_tourism_market process_ranking_turismo.
    Retorna [{iso, country, year, val}, ...] para top 10 países × últimos 3 años.
    """
    try:
        r = requests.get(WB_TOURISM_URL, timeout=60)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or len(data) < 2:
            return []
        items = data[1]
    except Exception:
        return []

    rows = []
    for it in items:
        try:
            iso = it.get("countryiso3code") or ""
            if not iso or len(iso) != 3:
                continue
            val = it.get("value")
            if val is None:
                continue
            country = (it.get("country") or {}).get("value") or ""
            year = int(it.get("date", 0))
            if year > 0 and country:
                rows.append({
                    "iso": iso,
                    "country": country,
                    "year": year,
                    "val": float(val),
                })
        except (ValueError, TypeError):
            continue

    if not rows:
        return []

    years = sorted({r["year"] for r in rows})
    latest_year = max(years) if years else 0
    plot_years = [y for y in years if y >= latest_year - 2][:5]
    plot_years.sort()

    by_country_year: dict[str, dict[int, float]] = {}
    for r in rows:
        c = r["country"]
        if c not in by_country_year:
            by_country_year[c] = {}
        by_country_year[c][r["year"]] = r["val"]

    top_by_latest = sorted(
        by_country_year.keys(),
        key=lambda c: by_country_year[c].get(latest_year, 0),
        reverse=True,
    )[:10]

    country_iso = {r["country"]: r["iso"] for r in rows}

    result = []
    for c in top_by_latest:
        for y in plot_years:
            v = by_country_year.get(c, {}).get(y, 0)
            if v > 0:
                result.append({
                    "iso": country_iso.get(c, ""),
                    "country": c,
                    "year": y,
                    "val": v,
                })
    return result


def _load_ranking_turismo_from_csv() -> list[dict]:
    """Carga ranking turismo desde CSV de respaldo (data/process/ranking_turismo_wb.csv)."""
    path = os.path.join(_BASE_DIR, "data", "process", "ranking_turismo_wb.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                iso = row.get("iso", "")
                country = row.get("country", "")
                year = int(float(row.get("year", 0)))
                val = float(str(row.get("val", "0")).replace(",", ""))
                if country and year > 0:
                    rows.append({"iso": iso, "country": country, "year": year, "val": val})
        return rows
    except Exception:
        return []


def get_ranking_turismo_wb() -> list[dict]:
    """
    Obtiene ranking turismo mundial (ingresos por turismo internacional).
    Prioridad: 1) PostgreSQL, 2) API Banco Mundial, 3) CSV.
    Retorna [{iso, country, year, val}, ...].
    """
    from services.db import get_ranking_turismo_wb_from_db, save_ranking_turismo_wb_to_db

    db_data = get_ranking_turismo_wb_from_db()
    if db_data:
        return db_data
    data = _fetch_and_process_ranking_turismo_wb()
    if not data:
        data = _load_ranking_turismo_from_csv()
    if data:
        save_ranking_turismo_wb_to_db(data)
    return data


# -----------------------------------------------------------------------------
# Balanza de Visitantes (INEGI BISE - pq-estudios-mercado-vps)
# -----------------------------------------------------------------------------
IDS_INEGI_BALANZA_ENTRADAS = [
    "6207123161", "6207123163", "6207123165", "6207123166", "6207123167", "6207123175"
]
IDS_INEGI_BALANZA_SALIDAS = [
    "6207132414", "6207132416", "6207132418", "6207132421", "6207132427", "6207132429"
]


def _fetch_inegi_json(ids: list[str], source: str = "BISE") -> dict | None:
    """Obtiene datos de varios indicadores INEGI en una sola llamada."""
    if not INEGI_TOKEN:
        return None
    ids_str = ",".join(ids)
    url = (
        f"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml"
        f"/INDICATOR/{ids_str}/es/00/false/{source}/2.0/{INEGI_TOKEN}?type=json"
    )
    try:
        resp = requests.get(url, headers={"User-Agent": "PhiQus Market Intelligence Client/1.0"}, timeout=60)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def _parse_balanza_json(path: str) -> dict[int, float]:
    """
    Parsea JSON de INEGI (BISE) con Series/OBSERVATIONS.
    Retorna {year: suma_valores}.
    """
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        return {}
    obs_list = []
    for s in d.get("Series", []):
        obs_list.extend(s.get("OBSERVATIONS", []))
    result: dict[int, float] = {}
    for o in obs_list:
        try:
            tp = o.get("TIME_PERIOD", "")
            year = int(float(tp.split("/")[0]) if "/" in tp else tp)
            val = float(str(o.get("OBS_VALUE", "0")).replace(",", ""))
            result[year] = result.get(year, 0) + val
        except (ValueError, TypeError):
            continue
    return result


def _fetch_and_process_balanza_visitantes() -> list[dict]:
    """
    Obtiene Balanza de Visitantes desde API INEGI.
    Replica proc_tourism_market process_aereo_y_balanza.
    Retorna [{year, entradas, salidas, balance}, ...].
    """
    raw_dir = os.path.join(_BASE_DIR, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    entradas_json = _fetch_inegi_json(IDS_INEGI_BALANZA_ENTRADAS)
    salidas_json = _fetch_inegi_json(IDS_INEGI_BALANZA_SALIDAS)
    if not entradas_json or not salidas_json:
        return []

    f_ent = os.path.join(raw_dir, "balanza_visitantes_entradas.json")
    f_sal = os.path.join(raw_dir, "balanza_visitantes_salidas.json")
    with open(f_ent, "w", encoding="utf-8") as f:
        json.dump(entradas_json, f, ensure_ascii=False, indent=2)
    with open(f_sal, "w", encoding="utf-8") as f:
        json.dump(salidas_json, f, ensure_ascii=False, indent=2)

    by_ent = _parse_balanza_json(f_ent)
    by_sal = _parse_balanza_json(f_sal)
    years = sorted(set(by_ent.keys()) & set(by_sal.keys()))
    if not years:
        return []

    result = []
    for yr in years:
        e = by_ent.get(yr, 0)
        s = by_sal.get(yr, 0)
        result.append({
            "year": yr,
            "entradas": round(e, 2),
            "salidas": round(s, 2),
            "balance": round(e - s, 2),
        })
    return result


def _load_balanza_from_csv() -> list[dict]:
    """Carga desde CSV de respaldo (data/process/balanza_visitantes_inegi.csv)."""
    path = os.path.join(_BASE_DIR, "data", "process", "balanza_visitantes_inegi.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                year = int(float(row.get("year", 0)))
                entradas = float(str(row.get("entradas", "0")).replace(",", ""))
                salidas = float(str(row.get("salidas", "0")).replace(",", ""))
                balance = float(str(row.get("balance", "0")).replace(",", ""))
                if year > 0:
                    rows.append({"year": year, "entradas": entradas, "salidas": salidas, "balance": balance})
        return rows
    except Exception:
        return []


def get_balanza_visitantes() -> list[dict]:
    """
    Obtiene Balanza de Visitantes (entradas vs salidas turísticas).
    Prioridad: 1) PostgreSQL, 2) API INEGI, 3) CSV.
    Retorna [{year, entradas, salidas, balance}, ...].
    """
    from services.db import get_balanza_visitantes_from_db, save_balanza_visitantes_to_db

    db_data = get_balanza_visitantes_from_db()
    if db_data:
        return db_data
    data = _fetch_and_process_balanza_visitantes()
    if not data:
        data = _load_balanza_from_csv()
    if data:
        save_balanza_visitantes_to_db(data)
    return data


# -----------------------------------------------------------------------------
# Participación Mercado Aéreo (AFAC/DataTur - pq-estudios-mercado-vps)
# -----------------------------------------------------------------------------
CUADRO_DGAC_URL = "https://datatur.sectur.gob.mx/Documentos%20compartidos/CUADRO_DGAC.zip"


def _extract_first_excel_from_zip(zip_bytes: bytes) -> bytes | None:
    """Extrae el primer archivo Excel (.xlsx/.xls) de un ZIP."""
    import zipfile

    from io import BytesIO

    try:
        with zipfile.ZipFile(BytesIO(zip_bytes)) as z:
            candidates = [f for f in z.namelist() if f.lower().endswith((".xlsx", ".xls"))]
            if not candidates:
                return None
            return z.read(candidates[0])
    except Exception:
        return None


def _fetch_and_process_participacion_mercado_aereo() -> tuple[list[dict], list[dict]]:
    """
    Descarga ZIP DataTur, extrae Excel y procesa Nacional e Internacional.
    Retorna (nacional_rows, internacional_rows).
    nacional: [{aerolinea, participacion}, ...] participacion en decimal (0.15 = 15%)
    internacional: [{region, pasajeros}, ...]
    """
    try:
        import pandas as pd

        resp = requests.get(CUADRO_DGAC_URL, timeout=60)
        resp.raise_for_status()
        excel_bytes = _extract_first_excel_from_zip(resp.content)
        if not excel_bytes:
            return [], []

        from io import BytesIO

        raw_dir = os.path.join(_BASE_DIR, "data", "raw")
        os.makedirs(raw_dir, exist_ok=True)
        xlsx_path = os.path.join(raw_dir, "participacion_mercado_aereo_raw.xlsx")
        with open(xlsx_path, "wb") as f:
            f.write(excel_bytes)

        # Sheet 1: Nacionales - Aerolinea col 2, Participacion/pasajeros col 6 (estructura puede variar)
        df_n = pd.read_excel(xlsx_path, sheet_name=1, header=None, engine="openpyxl")
        col_2 = df_n.iloc[10:30, 2].astype(str).str.strip()
        col_6 = df_n.iloc[10:30, 6]
        num_2 = pd.to_numeric(col_2, errors="coerce")
        num_6 = pd.to_numeric(col_6, errors="coerce")
        if num_2.notna().sum() > num_6.notna().sum():
            col_aero, col_part = col_6.astype(str).str.strip(), num_2
        else:
            col_aero, col_part = col_2, num_6
        df_n = pd.DataFrame({"Aerolinea": col_aero, "Participacion": col_part}).dropna(subset=["Participacion"])
        df_n = df_n[df_n["Aerolinea"].str.len() > 0]
        df_n = df_n[~df_n["Aerolinea"].str.match(r"^\d+\.?\d*$", na=False)]
        part = df_n["Participacion"]
        total = part.sum()
        if total > 0:
            if part.max() > 1.5:
                df_n["Participacion"] = part / total
            elif part.max() > 1:
                df_n["Participacion"] = part / 100.0
        nacional = [
            {"aerolinea": str(r["Aerolinea"]).strip(), "participacion": float(r["Participacion"])}
            for _, r in df_n.iterrows()
            if str(r["Aerolinea"]).strip()
        ]

        # Sheet 0: Internacionales - Region (Total X), Pasajeros col 3
        df_i = pd.read_excel(xlsx_path, sheet_name=0, header=None, engine="openpyxl")
        rows_i = []
        for _, r in df_i.iterrows():
            v = str(r.iloc[1] or "").strip()
            if v.lower().startswith("total ") and "general" not in v.lower():
                region = v.replace("Total ", "").strip()
                pasajeros = pd.to_numeric(r.iloc[3], errors="coerce") or 0.0
                if region:
                    rows_i.append({"region": region, "pasajeros": float(pasajeros)})
        internacional = rows_i

        return nacional, internacional
    except Exception:
        return [], []


def _load_participacion_from_csv() -> tuple[list[dict], list[dict]]:
    """Carga desde CSVs de respaldo."""
    nacional = []
    internacional = []
    path_n = os.path.join(_BASE_DIR, "data", "process", "participacion_mercado_aereo.csv")
    path_i = os.path.join(_BASE_DIR, "data", "process", "participacion_internacional_region.csv")
    try:
        if os.path.isfile(path_n):
            with open(path_n, encoding="utf-8-sig", newline="") as f:
                r = csv.DictReader(f)
                for row in r:
                    aero = str(row.get("aerolinea", row.get("Aerolinea", ""))).strip()
                    part = float(str(row.get("participacion", row.get("Participacion", "0"))).replace(",", ""))
                    if aero and part >= 0 and not aero.replace(".", "").isdigit():
                        if part > 1.5:
                            pass
                        elif part > 1:
                            part = part / 100.0
                        nacional.append({"aerolinea": aero, "participacion": part})
            if nacional and sum(r["participacion"] for r in nacional) > 1.5:
                total = sum(r["participacion"] for r in nacional)
                nacional = [{"aerolinea": r["aerolinea"], "participacion": r["participacion"] / total} for r in nacional]
        if os.path.isfile(path_i):
            with open(path_i, encoding="utf-8-sig", newline="") as f:
                r = csv.DictReader(f)
                for row in r:
                    reg = str(row.get("region", row.get("Region", ""))).strip()
                    pax = float(str(row.get("pasajeros", row.get("Pasajeros", "0"))).replace(",", ""))
                    if reg:
                        internacional.append({"region": reg, "pasajeros": pax})
    except Exception:
        pass
    return nacional, internacional


def _normalize_participacion_nacional(nacional: list[dict]) -> list[dict]:
    """Convierte participacion a decimal 0-1 si vienen como conteos (suma > 1.5)."""
    if not nacional:
        return nacional
    total = sum(r.get("participacion", 0) for r in nacional)
    if total <= 0:
        return nacional
    if total > 1.5:
        return [{"aerolinea": r["aerolinea"], "participacion": r["participacion"] / total} for r in nacional]
    if any(r.get("participacion", 0) > 1 for r in nacional):
        return [{"aerolinea": r["aerolinea"], "participacion": r["participacion"] / 100.0} for r in nacional]
    return nacional


def get_participacion_mercado_aereo() -> dict:
    """
    Obtiene Participación Mercado Aéreo (Nacional e Internacional).
    Prioridad: 1) PostgreSQL, 2) ZIP DataTur, 3) CSV.
    Retorna {nacional: [{aerolinea, participacion}], internacional: [{region, pasajeros}]}.
    participacion siempre en decimal 0-1. Etiquetas sin números puros.
    """
    from services.db import (
        get_participacion_internacional_from_db,
        get_participacion_mercado_from_db,
        save_participacion_internacional_to_db,
        save_participacion_mercado_to_db,
    )

    nac_db = get_participacion_mercado_from_db()
    int_db = get_participacion_internacional_from_db()
    if nac_db or int_db:
        nac_norm = _normalize_participacion_nacional(nac_db or [])
        nac_filt = [r for r in nac_norm if r.get("aerolinea") and not str(r.get("aerolinea", "")).replace(".", "").isdigit()]
        return {"nacional": nac_filt or nac_norm, "internacional": int_db or []}

    nac, intl = _fetch_and_process_participacion_mercado_aereo()
    if not nac and not intl:
        nac, intl = _load_participacion_from_csv()

    nac = _normalize_participacion_nacional(nac)
    nac = [r for r in nac if r.get("aerolinea") and not str(r.get("aerolinea", "")).replace(".", "").isdigit()]
    if nac:
        save_participacion_mercado_to_db(nac)
    if intl:
        save_participacion_internacional_to_db(intl)
    return {"nacional": nac, "internacional": intl}


# -----------------------------------------------------------------------------
# Anuncios de Inversión Combinados (DataMéxico - pq-estudios-mercado-vps)
# -----------------------------------------------------------------------------
ANUNCIOS_COMBINADOS_URL = (
    "https://www.economia.gob.mx/apidatamexico/tesseract/cubes/Anuncios_Inversion_Combinados_General/aggregate.jsonrecords"
    "?captions%5B%5D=ent_name.Geography.State.State+slug+ES"
    "&drilldowns%5B%5D=ent_name.Geography.State"
    "&drilldowns%5B%5D=Anio.Anio.Anio"
    "&measures%5B%5D=Numero+de+Anuncios"
    "&measures%5B%5D=Monto+Inversion"
)

STATE_ID_TO_NAME = {
    1: "Aguascalientes", 2: "Baja California", 3: "Baja California Sur", 4: "Campeche",
    5: "Coahuila de Zaragoza", 6: "Colima", 7: "Chiapas", 8: "Chihuahua",
    9: "Ciudad de México", 10: "Durango", 11: "Guanajuato", 12: "Guerrero",
    13: "Hidalgo", 14: "Jalisco", 15: "México", 16: "Michoacán de Ocampo",
    17: "Morelos", 18: "Nayarit", 19: "Nuevo León", 20: "Oaxaca",
    21: "Puebla", 22: "Querétaro", 23: "Quintana Roo", 24: "San Luis Potosí",
    25: "Sinaloa", 26: "Sonora", 27: "Tabasco", 28: "Tamaulipas",
    29: "Tlaxcala", 30: "Veracruz de Ignacio de la Llave", 31: "Yucatán", 32: "Zacatecas",
}


def _parse_anuncios_record(r: dict) -> dict | None:
    """Extrae Anio, Numero de Anuncios, Monto Inversion, State de un registro DataMéxico."""
    anio = None
    for k in ("Anio", "Date.Date.Year", "Year"):
        if k in r and r[k] is not None:
            try:
                anio = int(float(r[k]))
                break
            except (TypeError, ValueError):
                pass
    if anio is None:
        for key in r:
            if "anio" in key.lower() or "year" in key.lower() or "date" in key.lower():
                try:
                    anio = int(float(r[key]))
                    break
                except (TypeError, ValueError):
                    pass
    if anio is None:
        return None
    num = 0
    for key in r:
        if "numero" in key.lower() or "anuncios" in key.lower():
            try:
                num = int(float(r[key] or 0))
                break
            except (TypeError, ValueError):
                pass
    monto = 0
    for key in r:
        if "monto" in key.lower() or "inversion" in key.lower():
            try:
                monto = float(r[key] or 0)
                break
            except (TypeError, ValueError):
                pass
    state = ""
    for key in r:
        if "state" in key.lower() or "ent_name" in key.lower() or "geography" in key.lower() or "slug" in key.lower():
            v = r[key]
            if v is not None:
                state = str(v).strip()
                break
    return {"anio": anio, "num_anuncios": num, "monto_inversion": monto, "state": state}


def _fetch_and_process_anuncios_combinados() -> list[dict]:
    """Obtiene Anuncios de Inversión Combinados desde API DataMéxico."""
    try:
        resp = requests.get(ANUNCIOS_COMBINADOS_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        records = data.get("data") or []
    except Exception:
        return []
    rows = []
    for r in records:
        if not isinstance(r, dict):
            continue
        parsed = _parse_anuncios_record(r)
        if parsed:
            rows.append(parsed)
    return rows


def _load_anuncios_combinados_from_csv() -> list[dict]:
    """Carga desde CSV de respaldo."""
    path = os.path.join(_BASE_DIR, "data", "process", "anuncios_inversion_combinados.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                anio = int(float(row.get("Anio", row.get("anio", "0"))))
                num = int(float(row.get("Numero de Anuncios", row.get("num_anuncios", "0"))))
                monto = float(str(row.get("Monto Inversion", row.get("monto_inversion", "0"))).replace(",", ""))
                state = str(row.get("State", row.get("state", ""))).strip()
                if anio > 0:
                    rows.append({"anio": anio, "num_anuncios": num, "monto_inversion": monto, "state": state})
        return rows
    except Exception:
        return []


def _estado_limpio(val: str | int) -> str:
    """Convierte State a nombre legible: slug -> 'Baja California', id numérico -> nombre."""
    if val is None or val == "":
        return ""
    s = str(val).strip()
    try:
        idx = int(float(s))
        return STATE_ID_TO_NAME.get(idx, s) or s
    except (TypeError, ValueError):
        pass
    return s.replace("-", " ").title() if s else ""


def get_anuncios_inversion_combinados() -> list[dict]:
    """
    Obtiene Anuncios de Inversión Combinados.
    Prioridad: 1) PostgreSQL, 2) API DataMéxico, 3) CSV.
    Retorna [{anio, num_anuncios, monto_inversion, state, estado_limpio}, ...].
    Excluye registros con state/estado_limpio = 'nacional-nc' o 'Nacional Nc'.
    """
    from services.db import get_anuncios_combinados_from_db, save_anuncios_combinados_to_db

    db_data = get_anuncios_combinados_from_db()
    if db_data:
        out = []
        for r in db_data:
            state = r.get("state", "") or ""
            if str(state).strip().lower() == "nacional-nc":
                continue
            out.append({**r, "estado_limpio": _estado_limpio(state)})
        return out
    data = _fetch_and_process_anuncios_combinados()
    if not data:
        data = _load_anuncios_combinados_from_csv()
    if data:
        save_anuncios_combinados_to_db(data)
    out = []
    for r in data:
        state = r.get("state", "") or ""
        if str(state).strip().lower() == "nacional-nc":
            continue
        out.append({**r, "estado_limpio": _estado_limpio(state)})
    return out


# -----------------------------------------------------------------------------
# Anuncios de Inversión Base (DataMéxico - pq-estudios-mercado-vps)
# -----------------------------------------------------------------------------
ANUNCIOS_BASE_URL = (
    "https://www.economia.gob.mx/apidatamexico/tesseract/cubes/Anuncios_Inversion_BaseInversion/aggregate.jsonrecords"
    "?captions%5B%5D=Country.Country.Country.Country+ES"
    "&captions%5B%5D=Geography+State.Geography.State.State+slug+ES"
    "&drilldowns%5B%5D=Date.Date.Year"
    "&drilldowns%5B%5D=Country.Country.Country"
    "&drilldowns%5B%5D=Geography+State.Geography.State"
    "&drilldowns%5B%5D=IA_sector.IA_Sectores.IA_Sector"
    "&measures%5B%5D=Monto+Inversion"
)


def _parse_anuncios_base_record(r: dict) -> dict | None:
    """Extrae Year, Country, State, IA_Sector, Monto Inversion de un registro DataMéxico."""
    year = None
    for key in r:
        if "year" in key.lower() or "date" in key.lower() or "anio" in key.lower():
            try:
                year = int(float(r[key]))
                break
            except (TypeError, ValueError):
                pass
    if year is None:
        return None
    monto = 0
    for key in r:
        if "monto" in key.lower() or "inversion" in key.lower():
            try:
                monto = float(r[key] or 0)
                break
            except (TypeError, ValueError):
                pass
    country = ""
    for key in r:
        if "country" in key.lower() and "nation" not in key.lower():
            if r[key]:
                country = str(r[key]).strip()
                break
    state = ""
    for key in r:
        k = key.lower()
        if ("state" in k or "slug" in k) and "nation" not in k and r[key]:
            state = str(r[key]).strip()
            break
    ia_sector = ""
    for key in r:
        if "sector" in key.lower() or "ia_sector" in key.lower():
            if r[key]:
                ia_sector = str(r[key]).strip()
                break
    return {"year": year, "country": country, "state": state, "ia_sector": ia_sector, "monto_inversion": monto}


def _fetch_and_process_anuncios_base() -> list[dict]:
    """Obtiene Anuncios de Inversión Base desde API DataMéxico."""
    try:
        resp = requests.get(ANUNCIOS_BASE_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        records = data.get("data") or []
    except Exception:
        return []
    rows = []
    for r in records:
        if not isinstance(r, dict):
            continue
        parsed = _parse_anuncios_base_record(r)
        if parsed:
            rows.append(parsed)
    return rows


def _load_anuncios_base_from_csv() -> list[dict]:
    """Carga desde CSV de respaldo (Year, Country, State, IA_Sector, Monto Inversion)."""
    path = os.path.join(_BASE_DIR, "data", "process", "anuncios_inversion_base.csv")
    if not os.path.isfile(path):
        return []
    rows = []
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                year = int(float(row.get("Year", row.get("year", "0"))))
                country = str(row.get("Country", row.get("country", ""))).strip()
                state = str(row.get("State", row.get("state", ""))).strip()
                ia_sector = str(row.get("IA_Sector", row.get("ia_sector", ""))).strip()
                monto = float(str(row.get("Monto Inversion", row.get("monto_inversion", "0"))).replace(",", ""))
                if year > 0:
                    rows.append({"year": year, "country": country, "state": state, "ia_sector": ia_sector, "monto_inversion": monto})
        return rows
    except Exception:
        return []


def get_anuncios_inversion_base() -> list[dict]:
    """
    Obtiene Anuncios de Inversión Base.
    Prioridad: 1) PostgreSQL, 2) API DataMéxico, 3) CSV.
    Retorna [{year, country, state, ia_sector, monto_inversion, estado_limpio}, ...].
    """
    from services.db import get_anuncios_base_from_db, save_anuncios_base_to_db

    db_data = get_anuncios_base_from_db()
    if db_data:
        out = []
        for r in db_data:
            state = r.get("state", "") or ""
            est = _estado_limpio(state) if state else ""
            out.append({**r, "estado_limpio": est})
        return out
    data = _fetch_and_process_anuncios_base()
    if not data:
        data = _load_anuncios_base_from_csv()
    if data:
        save_anuncios_base_to_db(data)
    out = []
    for r in data:
        state = r.get("state", "") or ""
        est = _estado_limpio(state) if state else ""
        out.append({**r, "estado_limpio": est})
    return out


def get_participacion_internacional_region() -> list[dict]:
    """
    Obtiene Participación Internacional por Región (pasajeros por región).
    Prioridad: 1) PostgreSQL, 2) via get_participacion_mercado_aereo (Excel/CSV).
    Retorna [{region, pasajeros}, ...].
    """
    from services.db import get_participacion_internacional_from_db

    int_db = get_participacion_internacional_from_db()
    if int_db:
        return int_db
    pma = get_participacion_mercado_aereo()
    return pma.get("internacional", [])


def get_ied_paises() -> list[dict]:
    """
    Obtiene IED por país de origen.
    Prioridad: 1) PostgreSQL, 2) API datos.gob.mx, 3) CSV de respaldo.
    Retorna [{pais, monto_mdd, periodo}, ...] (top10 + Otros).
    """
    from services.db import get_ied_paises_from_db, save_ied_paises_to_db

    db_data = get_ied_paises_from_db()
    if db_data:
        return db_data
    data = _fetch_and_process_ied_paises()
    if not data:
        data = _load_ied_paises_from_csv()
    if data:
        save_ied_paises_to_db(data)
    return data


def get_kpis_nacional() -> dict:
    """
    Obtiene los 4 KPIs nacionales.
    Retorna dict con keys: pib_usd, tipo_cambio, inflacion, pib_mxn
    Cada uno tiene: value, date, formatted
    """
    pib_usd_val, pib_usd_fecha = get_pib_usd()
    tasa_val, tasa_fecha = get_tipo_cambio()
    inf_val, inf_fecha = get_inflacion()
    pib_mxn_val, pib_mxn_fecha = get_pib_mxn()

    def _format_tasa(v):
        if not v or v == "N/E":
            return "N/D"
        try:
            return f"${float(str(v).replace(',', '')):,.2f}"
        except (ValueError, TypeError):
            return str(v)

    def _format_date(d):
        if not d:
            return "N/D"
        try:
            if "-" in str(d):
                dt = datetime.strptime(str(d)[:10], "%Y-%m-%d")
            else:
                dt = datetime.strptime(str(d)[:10], "%d/%m/%Y")
            return dt.strftime("%d/%m/%Y")
        except Exception:
            return str(d)

    def _format_pib_usd(v):
        """PIB USD en miles de millones (billions)."""
        if v is None:
            return "N/D"
        try:
            n = float(str(v).replace(",", ""))
            return f"${n:,.2f} B"
        except Exception:
            return str(v)

    def _format_pib_mxn(v):
        """PIB MXN en millones -> mostrar en miles de millones (B) o billones (T)."""
        if v is None:
            return "N/D"
        try:
            n = float(str(v).replace(",", ""))
            if n >= 1e12:
                return f"${n / 1e12:,.2f} T"
            if n >= 1e9:
                return f"${n / 1e9:,.2f} B"
            return f"${n:,.0f}"
        except Exception:
            return str(v)

    # PIB USD: usar _format_periodo para fecha (ej: "3º trimestre 2025")
    pib_usd_date = _format_periodo(pib_usd_fecha) if pib_usd_fecha else _format_date(pib_usd_fecha)
    pib_mxn_date = _format_periodo(pib_mxn_fecha) if pib_mxn_fecha else _format_date(pib_mxn_fecha)

    return {
        "pib_usd": {
            "value": pib_usd_val,
            "date": pib_usd_date,
            "formatted": _format_pib_usd(pib_usd_val),
        },
        "tipo_cambio": {
            "value": tasa_val,
            "date": _format_date(tasa_fecha),
            "formatted": _format_tasa(tasa_val),
        },
        "inflacion": {
            "value": inf_val,
            "date": inf_fecha if inf_fecha else "N/D",
            "formatted": f"{inf_val}%" if inf_val else "N/D",
        },
        "pib_mxn": {
            "value": pib_mxn_val,
            "date": pib_mxn_date,
            "formatted": _format_pib_mxn(pib_mxn_val),
        },
    }
