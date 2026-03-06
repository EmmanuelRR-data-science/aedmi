"""

INGESTA: Obtiene datos crudos de INEGI y Banxico.

Usado por el ETL para el paso 1 del pipeline; fallback cuando PostgreSQL está vacío.

Fallback final: CSV en data/process/ cuando BD e INEGI fallan.

"""



import csv

import json

ITAEE_INDICATOR_TOTAL = "6207067158"
ITAEE_INDICATORS_SECTOR = {"Primario": "6207067160", "Secundario": "6207067161", "Terciario": "6207067162"}

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


def fetch_actividad_hotelera_nacional() -> tuple[list[dict], list[dict]]:
    """
    Extrae actividad hotelera nacional desde DataTur Base70centros.csv.
    Retorna (por_anio, por_categoria) para guardar en actividad_hotelera_nacional
    y actividad_hotelera_nacional_por_categoria.
    """
    import csv
    import io
    import requests

    def _safe_float(v):
        if v is None or v == "":
            return 0.0
        try:
            s = str(v).strip().replace(",", "").replace(" ", "")
            return float(s) if s else 0.0
        except (TypeError, ValueError):
            return 0.0

    url = "https://repodatos.atdt.gob.mx/s_turismo/ocupacion_hotelera/Base70centros.csv"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    text = resp.content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    by_year = {}
    by_year_cat = {}
    for row in reader:
        try:
            anio = int(row.get("anio") or 0)
        except Exception:
            continue
        if not anio:
            continue
        disp = _safe_float(row.get("cuartos_disponibles"))
        occ_nr = _safe_float(row.get("cuartos_ocupados_no_residentes"))
        occ_r = _safe_float(row.get("cuartos_ocupados_residentes"))
        occ = occ_nr + occ_r
        if anio not in by_year:
            by_year[anio] = {"disp": 0.0, "occ": 0.0}
        by_year[anio]["disp"] += disp
        by_year[anio]["occ"] += occ
        cat = (row.get("categoria") or "Sin categoría").strip() or "Sin categoría"
        key_cat = (anio, cat)
        if key_cat not in by_year_cat:
            by_year_cat[key_cat] = {"disp": 0.0, "occ": 0.0}
        by_year_cat[key_cat]["disp"] += disp
        by_year_cat[key_cat]["occ"] += occ

    por_anio = []
    for anio in sorted(by_year.keys()):
        d = by_year[anio]
        disp, occ = d["disp"], d["occ"]
        pct = (occ / disp * 100.0) if disp else 0.0
        por_anio.append({
            "anio": anio,
            "cuartos_disponibles_pd": disp,
            "cuartos_ocupados_pd": occ,
            "porc_ocupacion": pct,
        })
    por_categoria = []
    for (anio, cat) in sorted(by_year_cat.keys()):
        d = by_year_cat[(anio, cat)]
        disp, occ = d["disp"], d["occ"]
        pct = (occ / disp * 100.0) if disp else 0.0
        por_categoria.append({
            "anio": anio,
            "categoria": cat,
            "cuartos_disponibles_pd": disp,
            "cuartos_ocupados_pd": occ,
            "porc_ocupacion": pct,
        })
    return (por_anio, por_categoria)


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





def process_participacion_mercado_aereo_from_excel(xlsx_path: str) -> tuple[list[dict], list[dict]]:
    """Procesa el Excel de Participación de Mercado Aéreo directamente desde un archivo."""
    try:
        import pandas as pd
        # Sheet 0 (Nuevo formato SEP 2025): Nacionales - Aerolinea col 2, Participacion/pasajeros col 6
        df_n = pd.read_excel(xlsx_path, sheet_name=0, header=None, engine="openpyxl")
        # El formato ahora empieza unas filas antes, ampliamos el rango a 4:50
        col_2 = df_n.iloc[4:50, 2].astype(str).str.strip()
        col_6 = df_n.iloc[4:50, 6]
        num_2 = pd.to_numeric(col_2, errors="coerce")
        num_6 = pd.to_numeric(col_6, errors="coerce")
        if num_2.notna().sum() > num_6.notna().sum():
            col_aero, col_part = col_6.astype(str).str.strip(), num_2
        else:
            col_aero, col_part = col_2, num_6

        df_n = pd.DataFrame({"Aerolinea": col_aero, "Participacion": col_part}).dropna(subset=["Participacion"])
        df_n = df_n[df_n["Aerolinea"].str.len() > 0]
        # Quitar los "T O T A L", "TOTAL" que pudieran venir como aerolinea
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
            if str(r["Aerolinea"]).strip() and "TOTAL" not in str(r["Aerolinea"]).upper()
        ]

        # Sheet 1: Internacionales - Region (Total X), Pasajeros col 3
        df_i = pd.read_excel(xlsx_path, sheet_name=1, header=None, engine="openpyxl")
        rows_i = []
        for _, r in df_i.iterrows():
            v = str(r.iloc[1] or "").strip()
            if v.lower().startswith("total ") and "general" not in v.lower():
                region = v.replace("Total ", "").replace("TOTAL ", "").strip()
                pasajeros = pd.to_numeric(r.iloc[3], errors="coerce")
                if pd.isna(pasajeros):
                    pasajeros = 0.0
                if region:
                    rows_i.append({"region": region, "pasajeros": float(pasajeros)})
        internacional = rows_i

        return nacional, internacional
    except Exception as e:
        import traceback
        traceback.print_exc()
        return [], []


def _fetch_and_process_participacion_mercado_aereo() -> tuple[list[dict], list[dict]]:
    """
    Descarga ZIP DataTur, extrae Excel y procesa Nacional e Internacional.
    Retorna (nacional_rows, internacional_rows).
    nacional: [{aerolinea, participacion}, ...] participacion en decimal (0.15 = 15%)
    internacional: [{region, pasajeros}, ...]
    """
    try:
        import pandas as pd
        import requests

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
        
        return process_participacion_mercado_aereo_from_excel(xlsx_path)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return [], []





def _fetch_aeropuertos_estatal_from_dgac() -> list[dict]:
    """
    Descarga CUADRO_DGAC.zip (AFAC/DGAC), extrae el Excel y obtiene operaciones por aeropuerto por estado.
    Retorna [{estado_codigo, aeropuerto, grupo, anio, operaciones}, ...] para save_aeropuertos_estatal_to_db.
    """
    try:
        import pandas as pd
        resp = requests.get(CUADRO_DGAC_URL, timeout=60)
        resp.raise_for_status()
        excel_bytes = _extract_first_excel_from_zip(resp.content)
        if not excel_bytes:
            return []
        raw_dir = os.path.join(_BASE_DIR, "data", "raw")
        os.makedirs(raw_dir, exist_ok=True)
        xlsx_path = os.path.join(raw_dir, "CUADRO_DGAC_aeropuertos.xlsx")
        with open(xlsx_path, "wb") as f:
            f.write(excel_bytes)
        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
        out = []
        # Mapeo nombre estado -> código
        name_to_code = ESTADO_NOMBRE_TO_CODIGO
        def norm(s):
            s = (s or "").strip().lower()
            return "".join(c for c in _unicodedata.normalize("NFD", s) if _unicodedata.category(c) != "Mn")
        for sheet_name in xls.sheet_names:
            try:
                df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                if df.empty or df.shape[1] < 3:
                    continue
                # Buscar fila con headers: Aeropuerto, Estado/Entidad, Año, Operaciones
                for h in range(min(5, len(df))):
                    row = df.iloc[h].astype(str).str.strip().str.lower()
                    if "aeropuerto" in row.values and ("estado" in row.values or "entidad" in row.values or "operacion" in str(row.values)):
                        df_head = pd.read_excel(xls, sheet_name=sheet_name, header=h)
                        col_aero = None
                        col_estado = None
                        col_anio = None
                        col_oper = None
                        for c in df_head.columns:
                            sc = str(c).strip().lower()
                            if "aeropuerto" in sc:
                                col_aero = c
                            if "estado" in sc or "entidad" in sc:
                                col_estado = c
                            if "año" in sc or "anio" in sc or "year" in sc:
                                col_anio = c
                            if "operacion" in sc or "movimiento" in sc:
                                col_oper = c
                        if col_aero is None or col_estado is None:
                            continue
                        if col_oper is None:
                            for c in df_head.columns:
                                if df_head[c].dtype in ("int64", "float64") and "operacion" in str(c).lower():
                                    col_oper = c
                                    break
                        for _, r in df_head.iterrows():
                            aero = str(r.get(col_aero, "")).strip() if col_aero else ""
                            estado_nom = str(r.get(col_estado, "")).strip() if col_estado else ""
                            if not aero or not estado_nom or aero.lower() == "nan":
                                continue
                            codigo = name_to_code.get(estado_nom)
                            if not codigo:
                                for nm, cc in name_to_code.items():
                                    if norm(nm) == norm(estado_nom):
                                        codigo = cc
                                        break
                            if not codigo:
                                continue
                            anio = 2023
                            if col_anio:
                                try:
                                    anio = int(pd.to_numeric(r.get(col_anio), errors="coerce") or anio)
                                except Exception:
                                    pass
                            oper = 0
                            if col_oper is not None:
                                try:
                                    oper = int(pd.to_numeric(r.get(col_oper), errors="coerce") or 0)
                                except Exception:
                                    pass
                            if oper > 0:
                                out.append({
                                    "estado_codigo": codigo,
                                    "aeropuerto": aero[:200],
                                    "grupo": None,
                                    "anio": anio,
                                    "operaciones": oper,
                                })
                        if out:
                            return out
            except Exception:
                continue
        return out
    except Exception:
        return []


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




# ──────────────────────────────────────────────────────────────
# Funciones recuperadas (21) – se perdieron con git restore
# ──────────────────────────────────────────────────────────────

import unicodedata as _unicodedata

# ── 1. Diccionario nombre → código INEGI (2 dígitos) ────────
ESTADO_NOMBRE_TO_CODIGO = {
    "Aguascalientes": "01",
    "Baja California": "02",
    "Baja California Sur": "03",
    "Campeche": "04",
    "Coahuila": "05", "Coahuila de Zaragoza": "05",
    "Colima": "06",
    "Chiapas": "07",
    "Chihuahua": "08",
    "CDMX": "09", "Ciudad de México": "09",
    "Durango": "10",
    "Guanajuato": "11",
    "Guerrero": "12",
    "Hidalgo": "13",
    "Jalisco": "14",
    "Estado de México": "15", "México": "15",
    "Michoacán": "16", "Michoacán de Ocampo": "16",
    "Morelos": "17",
    "Nayarit": "18",
    "Nuevo León": "19",
    "Oaxaca": "20",
    "Puebla": "21",
    "Querétaro": "22",
    "Quintana Roo": "23",
    "San Luis Potosí": "24",
    "Sinaloa": "25",
    "Sonora": "26",
    "Tabasco": "27",
    "Tamaulipas": "28",
    "Tlaxcala": "29",
    "Veracruz": "30", "Veracruz de Ignacio de la Llave": "30",
    "Yucatán": "31",
    "Zacatecas": "32",
}

POBLACION_ESTADO_2020 = {
    "Aguascalientes": 1425607,
    "Baja California": 3769020,
    "Baja California Sur": 798447,
    "Campeche": 928363,
    "Coahuila de Zaragoza": 3146771,
    "Colima": 731391,
    "Chiapas": 5543828,
    "Chihuahua": 3741869,
    "Ciudad de México": 9209944,
    "Durango": 1832650,
    "Guanajuato": 6166934,
    "Guerrero": 3540685,
    "Hidalgo": 3082841,
    "Jalisco": 8348151,
    "Estado de México": 16992418,
    "Michoacán de Ocampo": 4748846,
    "Morelos": 1971520,
    "Nayarit": 1235456,
    "Nuevo León": 5784442,
    "Oaxaca": 4132148,
    "Puebla": 6583278,
    "Querétaro": 2368467,
    "Quintana Roo": 1857985,
    "San Luis Potosí": 2822255,
    "Sinaloa": 3026943,
    "Sonora": 2944840,
    "Tabasco": 2402598,
    "Tamaulipas": 3527735,
    "Tlaxcala": 1342977,
    "Veracruz de Ignacio de la Llave": 8062579,
    "Yucatán": 2320898,
    "Zacatecas": 1622138
}

SURFACE_AREA_KM2 = {
    "Aguascalientes": 5616,
    "Baja California": 71450,
    "Baja California Sur": 73909,
    "Campeche": 57507,
    "Coahuila de Zaragoza": 151562,
    "Colima": 5626,
    "Chiapas": 74211,
    "Chihuahua": 247455,
    "Ciudad de México": 1495,
    "Durango": 123317,
    "Guanajuato": 30607,
    "Guerrero": 63596,
    "Hidalgo": 20813,
    "Jalisco": 78588,
    "Estado de México": 22351,
    "Michoacán de Ocampo": 58599,
    "Morelos": 4879,
    "Nayarit": 27857,
    "Nuevo León": 64156,
    "Oaxaca": 93757,
    "Puebla": 34306,
    "Querétaro": 11699,
    "Quintana Roo": 44705,
    "San Luis Potosí": 61137,
    "Sinaloa": 57365,
    "Sonora": 179355,
    "Tabasco": 24731,
    "Tamaulipas": 80249,
    "Tlaxcala": 3991,
    "Veracruz de Ignacio de la Llave": 71826,
    "Yucatán": 39524,
    "Zacatecas": 75284
}


# ── 2. Normalizar nombre de estado ──────────────────────────
def _normalizar_estado(name: str) -> str:
    if not name:
        return ""
    name = name.lower().strip()
    return "".join(
        c for c in _unicodedata.normalize("NFD", name)
        if _unicodedata.category(c) != "Mn"
    )


# ── 3. get_pib_sector_economico ─────────────────────────────
def get_pib_sector_economico():
    """Lee pob_sector_actividad de PostgreSQL."""
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT sector, valor, pct, es_residual FROM pob_sector_actividad ORDER BY valor DESC")
            rows = cur.fetchall()
            return [
                {"sector": r[0], "valor": r[1], "pct": float(r[2]) if r[2] is not None else 0, "es_residual": r[3]}
                for r in rows
            ]
    except Exception:
        return []


# ── 4. Balanza Comercial por Producto (ETL → PostgreSQL → API) ───
# URL oficial Economía/DataMéxico: cubo inegi_foreign_trade_product.
# Estructura respuesta: [{ "Year", "Flow ID", "Flow", "HS2 2 Digit", "Trade Value" }, ...]
BASE_BCP_URL = (
    "https://www.economia.gob.mx/apidatamexico/tesseract/cubes/inegi_foreign_trade_product/aggregate.jsonrecords"
)
BCP_PARAMS = {
    "captions[]": [
        "Flow.Flow.Flow.Flow ES",
        "Product.Product 2 Digit.HS2 2 Digit.Long HS2 ES",
    ],
    "drilldowns[]": [
        "Date.Date.Year",
        "Flow.Flow.Flow",
        "Product.Product 2 Digit.HS2 2 Digit",
    ],
    "measures[]": "Trade Value",
}


def _bcp_parse_number(v):
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "").replace(" ", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except (TypeError, ValueError):
        return 0.0


def _bcp_find_trade_value(row):
    if not isinstance(row, dict):
        return 0.0
    for key in ("Trade Value", "trade_value", "Trade value", "TradeValue", "trade value"):
        if key in row and row[key] is not None:
            n = _bcp_parse_number(row[key])
            if n != 0:
                return n
    for k, v in row.items():
        if v is None:
            continue
        klo = (k or "").strip().lower().replace(" ", "")
        if klo in ("tradevalue", "trade_value") or ("trade" in klo and "value" in klo):
            n = _bcp_parse_number(v)
            if n != 0:
                return n
        if isinstance(v, dict):
            for k2, v2 in v.items():
                k2lo = (k2 or "").strip().lower().replace(" ", "")
                if k2lo in ("tradevalue", "trade_value") or ("trade" in k2lo and "value" in k2lo):
                    n = _bcp_parse_number(v2)
                    if n != 0:
                        return n
    for k, v in row.items():
        if isinstance(v, (int, float)) and v > 0:
            return float(v)
        if isinstance(v, str):
            n = _bcp_parse_number(v)
            if n > 0:
                return n
    return 0.0


def _bcp_normalize_row(row):
    if not isinstance(row, dict):
        return None
    year = row.get("Year") or row.get("year")
    if year is not None:
        try:
            year = int(float(year)) if isinstance(year, (int, float)) else int(str(year).strip().replace(",", ""))
        except (TypeError, ValueError):
            year = None
    flow_id = row.get("Flow ID") or row.get("flow_id") or row.get("Flow")
    if flow_id is not None:
        if isinstance(flow_id, (int, float)):
            flow_id = int(flow_id)
        elif isinstance(flow_id, str):
            s = flow_id.lower().strip()
            flow_id = int(s) if s.isdigit() else (2 if "export" in s or s == "2" else 1)
        else:
            flow_id = 1
    else:
        flow_id = 1
    trade_value = _bcp_find_trade_value(row)
    product = (
        row.get("HS2 2 Digit")
        or row.get("Product")
        or row.get("product")
        or row.get("HS2")
        or row.get("hs2")
        or "Total"
    )
    return {"year": year, "flow_id": flow_id, "trade_value": trade_value, "product": product}


def fetch_balanza_comercial_producto_from_api():
    """
    Obtiene datos de la API de Economía (inegi_foreign_trade_product) y los normaliza.
    Usado por el ETL para cargar en PostgreSQL. Retorna lista de {year, flow_id, trade_value, product}.
    """
    try:
        r = requests.get(BASE_BCP_URL, params=BCP_PARAMS, timeout=25)
        if r.status_code != 200:
            return []
        body = r.json()
        data = body.get("data", [])
        if not isinstance(data, list) or not data:
            return []
        out = []
        headers = data[0] if isinstance(data[0], (list, tuple)) else None
        for row in data:
            if isinstance(row, (list, tuple)) and headers and len(row) >= 2:
                row = {
                    (headers[j].strip() if isinstance(headers[j], str) else f"col{j}"): row[j]
                    for j in range(min(len(headers), len(row)))
                }
            if not isinstance(row, dict):
                continue
            n = _bcp_normalize_row(row)
            if n and n.get("year") is not None:
                out.append(n)
        if out:
            max_v = max((x.get("trade_value") or 0) for x in out)
            if max_v > 0 and max_v < 1e6:
                for x in out:
                    x["trade_value"] = (x.get("trade_value") or 0) * 1e6
        return out
    except Exception:
        return []


def get_balanza_comercial_producto():
    """
    Devuelve datos de balanza comercial por producto para el frontend.
    Prioridad: 1) PostgreSQL (ETL), 2) API Economía (fallback si BD vacía). Si no hay datos, retorna [].
    """
    try:
        from services.db import get_balanza_comercial_producto_from_db
        data = get_balanza_comercial_producto_from_db()
        if data:
            return data
        data = fetch_balanza_comercial_producto_from_api()
        if data:
            return data
    except Exception:
        pass
    return []


# ── 5. Operaciones Aeroportuarias (producto aeropuertos Excel → PostgreSQL → API) ───
def load_producto_aeropuertos_from_excel(filepath: str) -> list[dict]:
    """
    Lee el Excel producto-aeropuertos (2006-2025) y retorna [{anio, aeropuerto, operaciones}, ...].
    Prueba: varias hojas, varias filas de cabecera, detección por nombre de columna y por contenido (año/aeropuerto/operaciones).
    Soporta tabla pivote: filas = aeropuertos, columnas = años.
    """
    try:
        import pandas as pd
        if not filepath or not os.path.isfile(filepath):
            return []
        engine = "openpyxl" if str(filepath).lower().endswith(".xlsx") else None
        xls = pd.ExcelFile(filepath, engine=engine)
        sheet_names = xls.sheet_names

        def _parse_as_year(v):
            try:
                y = int(float(v))
                if 1990 <= y <= 2030:
                    return y
            except (ValueError, TypeError):
                pass
            return None

        def _normal_columns(df):
            """Formato: columnas año, aeropuerto, operaciones (por nombre o por inferencia)."""
            if df.empty or len(df.columns) < 2:
                return []
            cols = [str(c).strip().lower() for c in df.columns]
            col_anio = None
            col_aeropuerto = None
            col_operaciones = None
            for i, c in enumerate(cols):
                if not c or c.startswith("unnamed"):
                    continue
                if col_anio is None and any(x in c for x in ("año", "anio", "year", "ano")):
                    col_anio = df.columns[i]
                if col_aeropuerto is None and any(x in c for x in ("aeropuerto", "nombre", "airport", "centro", "sistema", "trabajo", "sitio")):
                    col_aeropuerto = df.columns[i]
                if col_operaciones is None and any(x in c for x in ("operacion", "movimiento", "total", "vuelo", "movimientos", "trafico")):
                    col_operaciones = df.columns[i]
            if col_anio is None or col_aeropuerto is None:
                idx_anio, idx_aero, idx_ops = _infer_columns_by_content(df)
                if idx_anio is not None and idx_aero is not None:
                    col_anio = df.columns[idx_anio]
                    col_aeropuerto = df.columns[idx_aero]
                    col_operaciones = df.columns[idx_ops] if idx_ops is not None else df.columns[min(idx_anio, idx_aero) + 1] if len(df.columns) > 2 else df.columns[idx_aero]
            if col_anio is None:
                col_anio = df.columns[0]
            if col_aeropuerto is None:
                col_aeropuerto = df.columns[1] if len(df.columns) > 1 else df.columns[0]
            if col_operaciones is None:
                col_operaciones = df.columns[2] if len(df.columns) > 2 else df.columns[1]
            out = []
            for _, row in df.iterrows():
                try:
                    anio = row.get(col_anio)
                    aeropuerto = row.get(col_aeropuerto)
                    ops = row.get(col_operaciones)
                    if pd.isna(anio) and pd.isna(aeropuerto):
                        continue
                    anio_val = _parse_as_year(anio)
                    if anio_val is None:
                        continue
                    aeropuerto_str = str(aeropuerto).strip() if not pd.isna(aeropuerto) else ""
                    if not aeropuerto_str or aeropuerto_str.isdigit():
                        continue
                    ops_val = 0 if pd.isna(ops) else int(float(ops))
                    out.append({"anio": anio_val, "aeropuerto": aeropuerto_str[:250], "operaciones": ops_val})
                except (ValueError, TypeError):
                    continue
            return out

        def _infer_columns_by_content(df, max_rows=20):
            """Infiere índice columna año / aeropuerto / operaciones por contenido."""
            idx_anio, idx_aero, idx_ops = None, None, None
            ncols = min(len(df.columns), 10)
            for c in range(ncols):
                col = df.columns[c]
                years = 0
                strings = 0
                numbers = 0
                for _, row in df.head(max_rows).iterrows():
                    v = row.get(col)
                    if pd.isna(v):
                        continue
                    if _parse_as_year(v) is not None:
                        years += 1
                    elif isinstance(v, (int, float)) and (v > 100 or v < 0):
                        numbers += 1
                    else:
                        s = str(v).strip()
                        if s and not s.replace(".", "").replace("-", "").isdigit():
                            strings += 1
                if years >= 2 and idx_anio is None:
                    idx_anio = c
                if strings >= 2 and idx_aero is None:
                    idx_aero = c
                if numbers >= 2 and idx_ops is None and c != idx_anio:
                    idx_ops = c
            if idx_ops is None and ncols >= 3:
                for j in range(ncols):
                    if j != idx_anio and j != idx_aero:
                        idx_ops = j
                        break
            return idx_anio, idx_aero, idx_ops

        def _pivot_format(df):
            """Tabla pivote: col0 = aeropuerto, resto columnas = años (header numérico o año)."""
            if df.empty or len(df.columns) < 2:
                return []
            out = []
            col_name = df.columns[0]
            for _, row in df.iterrows():
                nombre = row.get(col_name)
                if pd.isna(nombre):
                    continue
                nombre = str(nombre).strip()[:250]
                if not nombre or nombre.isdigit():
                    continue
                for j, col in enumerate(df.columns):
                    if j == 0:
                        continue
                    anio = None
                    try:
                        anio = int(float(col))
                    except (ValueError, TypeError):
                        s = str(col).strip()
                        for part in s.replace(",", " ").split():
                            anio = _parse_as_year(part)
                            if anio is not None:
                                break
                    if anio is None or anio < 1990 or anio > 2030:
                        continue
                    val = row.get(col)
                    if pd.isna(val):
                        continue
                    try:
                        ops = int(float(val))
                    except (ValueError, TypeError):
                        continue
                    out.append({"anio": anio, "aeropuerto": nombre, "operaciones": ops})
            return out

        for sheet in sheet_names:
            if "grafic" in str(sheet).lower() or "graphic" in str(sheet).lower():
                continue
            for header_row in (0, 1, 2, 3, 4, 5, 6):
                try:
                    df = pd.read_excel(xls, sheet_name=sheet, header=header_row, engine=engine)
                except Exception:
                    continue
                if df.empty or len(df.columns) < 2:
                    continue
                out = _normal_columns(df)
                if out:
                    return out
                out = _pivot_format(df)
                if out:
                    return out
        return []
    except Exception:
        return []


def get_operaciones_aeroportuarias():
    """
    Retorna datos para la sección Operaciones Aeroportuarias.
    Lee desde PostgreSQL: producto_aeropuertos_nacional (total por año, top aeropuertos) y participacion_mercado_aereo (por_grupo).
    Formato: { total: [{year, operaciones}], por_grupo: [{nombre, pct}], top_aeropuertos: [{aeropuerto, operaciones, year}] }.
    """
    try:
        from services.db import (
            get_producto_aeropuertos_nacional_from_db,
            get_participacion_mercado_from_db,
        )
        rows = get_producto_aeropuertos_nacional_from_db()
        por_grupo_raw = get_participacion_mercado_from_db()

        total_list = []
        top_aeropuertos = []
        if rows:
            by_year = {}
            for r in rows:
                y = r.get("anio")
                ops = r.get("operaciones", 0) or 0
                if y is not None:
                    by_year[y] = by_year.get(y, 0) + ops
            total_list = [{"year": y, "operaciones": by_year[y]} for y in sorted(by_year.keys())]
            if rows:
                last_year = max(r.get("anio") for r in rows if r.get("anio") is not None)
                by_aero = {}
                for r in rows:
                    if r.get("anio") != last_year:
                        continue
                    name = (r.get("aeropuerto") or "").strip()
                    if not name:
                        continue
                    by_aero[name] = by_aero.get(name, 0) + (r.get("operaciones") or 0)
                top_aeropuertos = [
                    {"aeropuerto": k, "operaciones": v, "year": last_year}
                    for k, v in sorted(by_aero.items(), key=lambda x: -x[1])[:12]
                ]

        por_grupo = []
        if por_grupo_raw:
            por_grupo = [
                {"nombre": r.get("aerolinea", ""), "operaciones": 0, "pct": round(float(r.get("participacion", 0) or 0) * 100, 2)}
                for r in por_grupo_raw
            ]

        return {
            "total": total_list,
            "por_grupo": por_grupo,
            "top_aeropuertos": top_aeropuertos,
        }
    except Exception:
        return {"total": [], "por_grupo": [], "top_aeropuertos": []}


# ── 6. get_actividad_hotelera ────────────────────────────────
def get_actividad_hotelera():
    """Retorna {nacional: [...], por_categoria: [...], ultimo_anio: int}."""
    try:
        from services.db import db_connection
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT anio, cuartos_disponibles_pd, cuartos_ocupados_pd, porc_ocupacion
                FROM actividad_hotelera_nacional
                ORDER BY anio ASC
                """
            )
            rows = cur.fetchall()
            nacional = [
                {
                    "anio": int(r[0]),
                    "cuartos_disponibles_pd": float(r[1]) if r[1] is not None else 0,
                    "cuartos_ocupados_pd": float(r[2]) if r[2] is not None else 0,
                    "porc_ocupacion": float(r[3]) if r[3] is not None else 0,
                }
                for r in rows
            ]
            ultimo_anio = nacional[-1]["anio"] if nacional else None
            por_categoria = []
            if ultimo_anio is not None:
                cur.execute(
                    """
                    SELECT categoria, cuartos_disponibles_pd, cuartos_ocupados_pd, porc_ocupacion
                    FROM actividad_hotelera_nacional_por_categoria
                    WHERE anio = %s
                    ORDER BY cuartos_disponibles_pd DESC NULLS LAST
                    """,
                    (ultimo_anio,),
                )
                por_categoria = [
                    {
                        "categoria": r[0] or "Sin categoría",
                        "cuartos_disponibles_pd": float(r[1]) if r[1] is not None else 0,
                        "cuartos_ocupados_pd": float(r[2]) if r[2] is not None else 0,
                        "porc_ocupacion": float(r[3]) if r[3] is not None else 0,
                    }
                    for r in cur.fetchall()
                ]
            return {"nacional": nacional, "por_categoria": por_categoria, "ultimo_anio": ultimo_anio}
    except Exception:
        return {"nacional": [], "por_categoria": [], "ultimo_anio": None}


# ── 7. get_demografia_estatal ────────────────────────────────
def get_demografia_estatal(estado_nombre: str):
    codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado_nombre)
    if not codigo:
        # Try normalized matching
        norm = _normalizar_estado(estado_nombre)
        for name, c in ESTADO_NOMBRE_TO_CODIGO.items():
            if _normalizar_estado(name) == norm:
                codigo = c
                break
    if not codigo:
        return None
    try:
        from services.db import get_demografia_estatal_from_db
        data = get_demografia_estatal_from_db(codigo)
        if data:
            return data
    except Exception:
        pass
    # Fallback JSON local
    import os, json
    path = os.path.join("data", "process", f"demografia_estatal_{codigo}.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


# ── 8. get_proyecciones_conapo y descarga CONAPO ───────────────
# URL típica CONAPO proyecciones (entidades federativas). Si falla, usar variable de entorno CONAPO_CSV_URL.
CONAPO_PROYECCIONES_CSV_URL = os.getenv(
    "CONAPO_CSV_URL",
    "https://raw.githubusercontent.com/DataMx/conapo/master/datos/proyecciones/entidades/proyecciones_entidades.csv",
)

def _download_conapo_proyecciones_csv() -> bool:
    """Descarga el CSV de proyecciones CONAPO y lo guarda en data/process/proyecciones_conapo.csv. Retorna True si ok."""
    path = os.path.join(_BASE_DIR, "data", "process", "proyecciones_conapo.csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        r = requests.get(CONAPO_PROYECCIONES_CSV_URL, timeout=45)
        if r.status_code != 200 or len(r.content) < 500:
            return False
        with open(path, "wb") as f:
            f.write(r.content)
        return True
    except Exception:
        return False


def get_proyecciones_conapo(estado_nombre: str):
    codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado_nombre)
    if not codigo:
        norm = _normalizar_estado(estado_nombre)
        for name, c in ESTADO_NOMBRE_TO_CODIGO.items():
            if _normalizar_estado(name) == norm:
                codigo = c
                break
    if not codigo:
        return None
    # 1. PostgreSQL
    try:
        from services.db import get_proyecciones_conapo_from_db
        data = get_proyecciones_conapo_from_db(codigo)
        if data:
            return data
    except Exception:
        pass
    # 2. Fallback: local CSV (columnas pueden ser CVE_GEO, AÑO, POB_MIT_AÑO, HOMBRES, MUJERES u otras)
    try:
        path = os.path.join(_BASE_DIR, "data", "process", "proyecciones_conapo.csv")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                results = []
                for row in reader:
                    cve = str(row.get("CVE_GEO", row.get("cve_geo", ""))).strip().zfill(2)
                    if cve != codigo:
                        continue
                    anio = row.get("AÑO", row.get("anio", row.get("Año", 0)))
                    total = row.get("POB_MIT_AÑO", row.get("total", row.get("POB", 0)))
                    hombres = row.get("HOMBRES", row.get("hombres", 0))
                    mujeres = row.get("MUJERES", row.get("mujeres", 0))
                    try:
                        results.append({
                            "anio": int(float(anio)),
                            "total": float(total or 0),
                            "hombres": float(hombres or 0),
                            "mujeres": float(mujeres or 0),
                        })
                    except (TypeError, ValueError):
                        continue
                if results:
                    return sorted(results, key=lambda x: x["anio"])
    except Exception:
        pass
    return None


# ── 9. get_itaee_estatal (INEGI BIE con token) ─────────────────
# Indicadores ITAEE por entidad: total 6207067158; sectores pueden variar (BIE por entidad).
# ITAEE_INDICATOR_TOTAL = "6207067158"
# ITAEE_INDICATORS_SECTOR = {"Primario": "6207067160", "Secundario": "6207067161", "Terciario": "6207067162"}

def get_itaee_estatal(estado_nombre: str):
    codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado_nombre)
    if not codigo:
        norm = _normalizar_estado(estado_nombre)
        for name, c in ESTADO_NOMBRE_TO_CODIGO.items():
            if _normalizar_estado(name) == norm:
                codigo = c
                break
    if not codigo:
        return None
    # 1. PostgreSQL
    try:
        from services.db import get_itaee_estatal_from_db
        data = get_itaee_estatal_from_db(codigo)
        if data:
            return data
    except Exception:
        pass
    # 2. Fallback: INEGI BIE API con token (geo = código entidad sin cero: 1-32, API usa 01-32)
    token = INEGI_TOKEN
    if not token:
        return None
    try:
        # geo: 00=nacional, 01-32=entidad (API acepta 01, 02, ...)
        geo = str(codigo).zfill(2)
        base_url = "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR"
        anio = None
        total_val = 0.0
        primario = 0.0
        secundario = 0.0
        terciario = 0.0
        # Total ITAEE
        url = f"{base_url}/{ITAEE_INDICATOR_TOTAL}/es/{geo}/false/BISE/2.0/{token}?type=json"
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            body = r.json()
            for s in body.get("Series", []):
                obs = s.get("OBSERVATIONS", [])
                if obs:
                    last = obs[-1]
                    anio = last.get("TIME_PERIOD", "")
                    total_val = float(last.get("OBS_VALUE", 0) or 0)
                    break
        # Sectores (si los indicadores existen)
        for sector_name, ind_id in ITAEE_INDICATORS_SECTOR.items():
            url_s = f"{base_url}/{ind_id}/es/{geo}/false/BISE/2.0/{token}?type=json"
            try:
                r2 = requests.get(url_s, timeout=10)
                if r2.status_code == 200:
                    b2 = r2.json()
                    for s in b2.get("Series", []):
                        obs = s.get("OBSERVATIONS", [])
                        if obs:
                            v = float(obs[-1].get("OBS_VALUE", 0) or 0)
                            if sector_name == "Primario":
                                primario = v
                            elif sector_name == "Secundario":
                                secundario = v
                            else:
                                terciario = v
                            break
            except Exception:
                pass
        if anio is not None and total_val > 0:
            return {
                "anio": str(anio),
                "total": total_val,
                "primario": primario,
                "secundario": secundario,
                "terciario": terciario,
            }
    except Exception:
        pass
    return None


def get_itaee_estatal_timeline(estado_nombre: str):
    """Obtiene el histórico (timeline) del ITAEE desde INEGI BIE API."""
    from services.data_sources import ESTADO_NOMBRE_TO_CODIGO, INEGI_TOKEN
    codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado_nombre)
    if not codigo:
        return []
    
    token = INEGI_TOKEN
    if not token:
        return []
    
    try:
        geo = str(codigo).zfill(2)
        base_url = "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR"
        
        # Mapear indicadores a sectores
        indicators = {
            "total": ITAEE_INDICATOR_TOTAL,
            "primario": ITAEE_INDICATORS_SECTOR["Primario"],
            "secundario": ITAEE_INDICATORS_SECTOR["Secundario"],
            "terciario": ITAEE_INDICATORS_SECTOR["Terciario"]
        }
        
        # Diccionario para agrupar por año: { "2023": { "anio": "2023", "total": 0, ... } }
        by_year = {}
        
        for key, ind_id in indicators.items():
            url = f"{base_url}/{ind_id}/es/{geo}/false/BISE/2.0/{token}?type=json"
            try:
                r = requests.get(url, timeout=15)
                if r.status_code == 200:
                    body = r.json()
                    for s in body.get("Series", []):
                        for obs in s.get("OBSERVATIONS", []):
                            # El BIE suele traer trimestrales como "2023/01", "2023/02", etc.
                            # O nacionales anuales como "2023".
                            tp = str(obs.get("TIME_PERIOD", ""))
                            # Si es trimestral o tiene slash, nos quedamos con el año
                            anio = tp.split("/")[0] if "/" in tp else tp
                            if len(anio) != 4: continue
                            
                            val = float(obs.get("OBS_VALUE", 0) or 0)
                            
                            if anio not in by_year:
                                by_year[anio] = {"anio": anio, "total": 0.0, "primario": 0.0, "secundario": 0.0, "terciario": 0.0}
                            
                            by_year[anio][key] = val
            except Exception:
                continue

        # Filtrar solo años con datos significativos (Total > 0 o suma sectores > 0)
        res = []
        for anio in sorted(by_year.keys()):
            d = by_year[anio]
            if d["total"] > 0 or (d["primario"] + d["secundario"] + d["terciario"]) > 0:
                res.append(d)
        return res
    except Exception:
        return []


# ── 10. get_municipios_por_estado ────────────────────────────
def get_municipios_por_estado(estado_nombre: str):
    try:
        from services.db import get_municipios_from_db
        return get_municipios_from_db(estado_nombre)
    except Exception:
        return []


# ── 11. get_localidades ─────────────────────────────────────
def get_localidades(estado: str, municipio: str):
    try:
        from services.db import get_localidades_from_db
        return get_localidades_from_db(estado, municipio)
    except Exception:
        return []


# ── 12. get_distribucion_poblacion_localidad ─────────────────
def get_distribucion_poblacion_localidad(estado: str, municipio: str, localidad: str):
    try:
        from services.db import get_distribucion_poblacion_localidad_from_db
        return get_distribucion_poblacion_localidad_from_db(estado, municipio, localidad)
    except Exception:
        return None


# ── 13. get_distribucion_poblacion_municipal ─────────────────
def get_distribucion_poblacion_municipal(estado: str, municipio: str):
    try:
        from services.db import get_distribucion_poblacion_municipal_from_db
        return get_distribucion_poblacion_municipal_from_db(estado, municipio)
    except Exception:
        return None


# ── 14. get_aeropuertos_por_estado ───────────────────────────
def get_aeropuertos_por_estado(codigo: str):
    try:
        from services.db import get_aeropuertos_estatal_from_db
        return get_aeropuertos_estatal_from_db(codigo)
    except Exception:
        return []


# ── 15. get_exportaciones_por_estado y ETL: API DataMéxico ───
# Mapeo slug DataMéxico -> código INEGI (2 dígitos)
def _state_slug_to_codigo(slug: str) -> str | None:
    if not slug:
        return None
    slug = str(slug).strip().lower().replace(" ", "-").replace("_", "-")
    for name, codigo in ESTADO_NOMBRE_TO_CODIGO.items():
        n = _normalizar_estado(name).replace(" ", "-")
        if n == slug or slug in n or n in slug:
            return codigo
    return None


def _get_exportaciones_por_estado_from_api() -> list[dict]:
    """
    Obtiene exportaciones por estado desde API DataMéxico (sin token).
    Retorna lista con estado_codigo, estado_slug, year, trade_value para guardar en exportaciones_estatal.
    """
    url = "https://api.datamexico.org/tesseract/data.jsonrecords?cube=economy_foreign_trade_ent&drilldowns=Year,State&measures=Trade+Value&parents=false&sparse=false"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return []
        body = r.json()
        raw = body.get("data", [])
        if not raw:
            return []
        result = []
        for row in raw:
            year = row.get("Year")
            state_slug = str(row.get("State", "")).strip().lower().replace(" ", "-")
            trade_value = row.get("Trade Value", 0)
            try:
                year = int(year)
            except (TypeError, ValueError):
                continue
            codigo = _state_slug_to_codigo(state_slug)
            if not codigo:
                continue
            result.append({
                "estado_codigo": codigo,
                "state_slug": state_slug,
                "year": year,
                "trade_value": float(trade_value) if trade_value is not None else 0,
            })
        return result
    except Exception:
        return []


def get_exportaciones_por_estado():
    """Intenta BD, luego DataMéxico, luego CSV local."""
    # 1. Intentar PostgreSQL
    try:
        from services.db import get_exportaciones_estatal_from_db
        data = get_exportaciones_estatal_from_db()
        if data:
            return data
    except Exception:
        pass

    # 2. Fallback API DataMéxico (timeout más largo)
    api_data = _get_exportaciones_por_estado_from_api()
    if api_data:
        return [
            {"year": r["year"], "state_slug": r["state_slug"], "estado_codigo": r["estado_codigo"], "trade_value": r["trade_value"]}
            for r in api_data
        ]

    # 3. Fallback: CSV procesados
    try:
        import os, csv
        path = os.path.join("data", "process", "exportaciones_estatal.csv")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                return [dict(row) for row in reader]
    except Exception:
        pass
    return []


# ── 16. get_proyeccion_poblacional_municipal ────────────────
def get_proyeccion_poblacional_municipal(estado: str, municipio: str):
    try:
        from services.db import get_proyeccion_poblacional_municipal_from_db
        return get_proyeccion_poblacional_municipal_from_db(estado, municipio)
    except Exception:
        return None


# ── 17. get_actividad_hotelera_estatal ──────────────────────
def get_actividad_hotelera_estatal(estado: str, anio=None):
    """Retorna (data_dict, error_str, years_list)."""
    try:
        from services.db import get_actividad_hotelera_estatal_from_db
        codigo = ESTADO_NOMBRE_TO_CODIGO.get(estado)
        if not codigo:
            norm = _normalizar_estado(estado)
            for name, c in ESTADO_NOMBRE_TO_CODIGO.items():
                if _normalizar_estado(name) == norm:
                    codigo = c
                    break
        if not codigo:
            return None, f"Estado no encontrado: {estado}", []
        result = get_actividad_hotelera_estatal_from_db(codigo, anio=anio)
        # DB returns (data_dict_or_None, years_list)
        if isinstance(result, tuple):
            data, years = result
        else:
            data, years = result, []
        if data is None:
            return None, f"No hay datos de actividad hotelera para {estado}", years
        return data, None, years
    except Exception as e:
        return None, str(e), []


# ── 18. get_crecimiento_historico_localidad ──────────────────
def get_crecimiento_historico_localidad(estado: str, municipio: str, localidad: str):
    try:
        from services.db import get_crecimiento_historico_localidad_from_db
        return get_crecimiento_historico_localidad_from_db(estado, municipio, localidad)
    except Exception:
        return []


# ── 19. get_mapa_carretero_estatal ───────────────────────────
def get_mapa_carretero_estatal(estado: str):
    """
    Retorna (bytes_png | None, error_str | None).
    Fuente: SICT Datos Viales. Basado en estado_conectividad.ipynb.
    """
    try:
        # PRIMERO: Intentar obtener de la base de datos (ETL procesado)
        from services.db import get_mapa_carretero_from_db
        img_db = get_mapa_carretero_from_db(estado)
        if img_db:
            return img_db, None

        import requests
        import fitz  # PyMuPDF
        import io

        # Mapeo oficial SCT de estado_conectividad.ipynb
        STATES_MAP_SCT = {
            "Aguascalientes": ("01", "AGUASCALIENTES"), "Baja California": ("02", "BAJA_CALIFORNIA"),
            "Baja California Sur": ("03", "BAJA_CALIFORNIA_SUR"), "Campeche": ("04", "CAMPECHE"),
            "Coahuila": ("05", "COAHUILA"), "Colima": ("06", "COLIMA"), "Chiapas": ("07", "CHIAPAS"),
            "Chihuahua": ("08", "CHIHUAHUA"), "Ciudad de México": ("09", "CIUDAD_DE_MEXICO"),
            "Durango": ("10", "DURANGO"), "Guanajuato": ("11", "GUANAJUATO"), "Guerrero": ("12", "GUERRERO"),
            "Hidalgo": ("13", "HIDALGO"), "Jalisco": ("14", "JALISCO"), "México": ("15", "MEXICO"),
            "Michoacán": ("16", "MICHOACAN"), "Morelos": ("17", "MORELOS"), "Nayarit": ("18", "NAYARIT"),
            "Nuevo León": ("19", "NUEVO_LEON"), "Oaxaca": ("20", "OAXACA"), "Puebla": ("21", "PUEBLA"),
            "Querétaro": ("22", "QUERETARO"), "Quintana Roo": ("23", "QUINTANA_ROO"),
            "San Luis Potosí": ("24", "SAN_LUIS_POTOSI"), "Sinaloa": ("25", "SINALOA"),
            "Sonora": ("26", "SONORA"), "Tabasco": ("27", "TABASCO"), "Tamaulipas": ("28", "TAMAULIPAS"),
            "Tlaxcala": ("29", "TLAXCALA"), "Veracruz": ("30", "VERACRUZ"), "Yucatán": ("31", "YUCATAN"),
            "Zacatecas": ("32", "ZACATECAS")
        }

        # Normalizar para buscar en el mapa (quitar " de Ignacio de la Llave", etc si es necesario)
        # Pero podemos intentar coincidencia parcial o exacta
        target = estado.strip()
        found = None
        for k, v in STATES_MAP_SCT.items():
            if k.lower() in target.lower() or target.lower() in k.lower():
                found = v
                break
        
        if not found:
            return None, f"Estado '{estado}' no mapeado para SCT"

        code, name_api = found
        # URL oficial SICT Datos Viales
        url = f"https://micrs.sct.gob.mx/images/DireccionesGrales/DGST/Datos-Viales-2016/{code}_{name_api}.pdf"
        
        resp = requests.get(url, timeout=20)
        if resp.status_code != 200:
            return None, f"Error SCT ({resp.status_code}) al descargar PDF para {estado}"
        
        # Convertir PDF a PNG usando fitz
        doc = fitz.open(stream=resp.content, filetype="pdf")
        if doc.page_count == 0:
            return None, "El PDF de la SCT está vacío"
        
        page = doc.load_page(0) # Primera página
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2)) # Zoom 2x para calidad
        img_bytes = pix.tobytes("png")
        doc.close()

        return img_bytes, None

    except ImportError:
        return None, "Librería PyMuPDF (fitz) no instalada"
    except Exception as e:
        return None, f"Error procesando mapa carretero: {str(e)}"


# ── 20. process_llegada_turistas_from_upload ─────────────────
def process_llegada_turistas_from_upload(filepath: str):
    """Procesa archivo Excel de llegada de turistas. Retorna (data_by_estado, error)."""
    try:
        import pandas as pd
        df = pd.read_excel(filepath)
        if df.empty:
            return None, "Archivo vacío"
        data_by_estado = {}
        for _, row in df.iterrows():
            estado = str(row.get("Estado", row.get("estado", ""))).strip()
            if not estado:
                continue
            if estado not in data_by_estado:
                data_by_estado[estado] = []
            data_by_estado[estado].append(row.to_dict())
        return data_by_estado, None
    except Exception as e:
        return None, str(e)


# ── 21. CETM actividad hotelera: carga y procesamiento Excel 6_2 (Vista05, Vista06a, Vista09a) ──
CETM_SHEETS_HOTELERIA = ("Vista05", "Vista06a", "Vista09a")
CETM_SHEETS_TURISTAS = ("Vista07a", "Vista07b", "Vista07c")

def _load_cetm_excel_sheets(filepath: str, sheets=None):
    """Carga hojas específicas del Excel CETM. Retorna dict nombre_hoja -> DataFrame (header=None)."""
    try:
        import pandas as pd
        engine = "openpyxl" if str(filepath).lower().endswith(".xlsx") else None
        xls = pd.ExcelFile(filepath, engine=engine)
        out = {}
        target_sheets = sheets if sheets else (CETM_SHEETS_HOTELERIA + CETM_SHEETS_TURISTAS)
        for sh in target_sheets:
            if sh in xls.sheet_names:
                out[sh] = pd.read_excel(xls, sh, header=None)
        return out
    except Exception:
        return {}

def _process_actividad_hotelera_dfs(dfs: dict):
    """
    A partir de los DataFrames de Vista05 (disponibles), Vista06a (ocupados), Vista09a (porc_ocupacion),
    retorna {estado_codigo: {anio: {disponibles: [12], ocupados: [12], porc_ocupacion: [12]}}}.
    """
    import pandas as pd
    if not dfs:
        return {}
    NAME_TO_CODE = ESTADO_NOMBRE_TO_CODIGO
    # Normalizar nombre para búsqueda
    def norm(s):
        s = (s or "").strip().lower()
        return "".join(c for c in _unicodedata.normalize("NFD", s) if _unicodedata.category(c) != "Mn")
    estado_norms = {norm(n): (n, c) for n, c in NAME_TO_CODE.items()}

    # Usar Vista06a como referencia (ocupados) - suele tener header en fila 12
    df_ref = dfs.get("Vista06a")
    if df_ref is None:
        df_ref = dfs.get("Vista05")
    if df_ref is None and dfs:
        df_ref = list(dfs.values())[0]

    if df_ref is None or (hasattr(df_ref, "empty") and df_ref.empty):
        return {}

    # Detectar columna de estados: primera columna con muchos nombres de estado
    best_col_idx = 0
    best_count = 0
    for col_idx in range(min(5, df_ref.shape[1])):
        try:
            vals = df_ref.iloc[:, col_idx].dropna().astype(str).str.strip()
            matches = sum(1 for v in vals if norm(v) in estado_norms or any(norm(v).startswith(k) or k.startswith(norm(v)) for k in estado_norms))
            if matches > best_count:
                best_count = matches
                best_col_idx = col_idx
        except Exception:
            continue

    # Filas de datos suelen empezar después del header (ej. fila 13)
    header_row = 12
    data_start = header_row + 1
    result = {}

    # Detectar cuáles son las columnas de los 12 meses (evitar columnas de "Total")
    # Buscamos en la fila de meses (12) la columna que diga "[12]" o "Dic"
    last_month_idx = -1
    for c_idx in range(df_ref.shape[1] - 1, -1, -1):
        month_label = str(df_ref.iloc[12, c_idx]).lower()
        if "[12]" in month_label or "dic" in month_label:
            last_month_idx = c_idx
            break
    
    if last_month_idx == -1:
        # Fallback si no encontramos etiquetas claras, evitamos los últimos 2 (Total y Total General)
        month_indices = range(df_ref.shape[1] - 14, df_ref.shape[1] - 2)
    else:
        month_indices = range(last_month_idx - 11, last_month_idx + 1)

    for row_idx in range(data_start, len(df_ref)):
        cell = df_ref.iloc[row_idx, best_col_idx]
        estado_nombre = str(cell).strip() if pd.notna(cell) else ""
        if not estado_nombre or len(estado_nombre) < 3:
            continue
        n = norm(estado_nombre)
        codigo = None
        for k, (nombre, c) in estado_norms.items():
            if n == k or n.startswith(k) or k.startswith(n):
                codigo = c
                break
        if not codigo:
            continue

        disponibles = []
        ocupados = []
        porc_ocupacion = []

        df_occ = dfs.get("Vista06a")
        df_disp = dfs.get("Vista05")
        df_pct = dfs.get("Vista09a")
        
        # Procesar ocupados
        if df_occ is not None and row_idx < len(df_occ):
            for c in month_indices:
                v = _scalar_from_series(df_occ.iloc[row_idx, c])
                val = float(pd.to_numeric(v, errors="coerce") or 0)
                ocupados.append(val if not pd.isna(val) else 0)
        
        # Procesar disponibles
        if df_disp is not None and row_idx < len(df_disp):
            for c in month_indices:
                v = _scalar_from_series(df_disp.iloc[row_idx, c])
                val = float(pd.to_numeric(v, errors="coerce") or 0)
                disponibles.append(val if not pd.isna(val) else 0)
        
        # Procesar porcentaje
        if df_pct is not None and row_idx < len(df_pct):
            for c in month_indices:
                v = _scalar_from_series(df_pct.iloc[row_idx, c])
                val = float(pd.to_numeric(v, errors="coerce") or 0)
                # Si el porcentaje está en formato 0.50, dejarlo así. Si está en 50.0, normalizar?
                # Según inspección, parece 0.465, así que está bien.
                porc_ocupacion.append(val if not pd.isna(val) else 0)

        # Rellenar si faltan datos
        if len(ocupados) < 12: ocupados = (ocupados + [0] * 12)[:12]
        if len(disponibles) < 12: disponibles = (disponibles + [0] * 12)[:12]
        if len(porc_ocupacion) < 12: porc_ocupacion = (porc_ocupacion + [0] * 12)[:12]

        # Intentar detectar el año del bloque seleccionado
        anio = 2024
        try:
            # Buscar en la fila del año (11) para el primer mes del bloque
            first_idx = list(month_indices)[0]
            year_val = df_ref.iloc[11, first_idx]
            if pd.isna(year_val):
                # Buscar hacia atrás el primer valor no nulo
                for back_idx in range(first_idx, 1, -1):
                    yv = df_ref.iloc[11, back_idx]
                    if pd.notna(yv):
                        year_val = yv
                        break
            
            anio_clean = int(pd.to_numeric(year_val, errors="coerce") or 2024)
            if 1990 < anio_clean < 2100:
                anio = anio_clean
        except Exception:
            pass

        if codigo not in result:
            result[codigo] = {}
        result[codigo][anio] = {
            "disponibles": disponibles,
            "ocupados": ocupados,
            "porc_ocupacion": porc_ocupacion,
        }
    return result


def _scalar_from_series(val):
    """Convierte valor de una celda/Series de pandas a escalar para evitar 'truth value of DataFrame is ambiguous'."""
    if val is None:
        return None
    try:
        import pandas as pd
        if isinstance(val, pd.Series):
            return val.iloc[0] if len(val) > 0 else None
        if hasattr(val, "item") and callable(getattr(val, "item", None)):
            return val.item()
    except Exception:
        pass
    return val


def process_actividad_hotelera_from_upload(filepath: str):
    """
    Procesa archivo Excel de CETM actividad hotelera (6_2.xlsx con Vista05, Vista06a, Vista09a).
    Retorna (data_by_estado, error).
    """
    try:
        import pandas as pd
        dfs = _load_cetm_excel_sheets(filepath, sheets=CETM_SHEETS_HOTELERIA)
        if dfs:
            data_by_estado = _process_actividad_hotelera_dfs(dfs)
            if data_by_estado:
                return data_by_estado, None
        return None, "No se reconoció la estructura CETM (Vista05/Vista06a/Vista09a)"
    except Exception as e:
        return None, str(e)


def process_llegada_turistas_cetm(filepath: str):
    """
    Procesa archivo Excel de CETM para Llegada de Turistas (6_2.xlsx con Vista07a).
    Retorna (data_by_estado, error) donde data_by_estado es {estado_codigo: {anio: valor_total}}.
    """
    try:
        import pandas as pd
        dfs = _load_cetm_excel_sheets(filepath, sheets=["Vista07a"])
        df = dfs.get("Vista07a")
        if df is None or df.empty:
            return None, "No se encontró la hoja Vista07a en el archivo"

        # Normalizar nombre para búsqueda
        def norm(s):
            s = (s or "").strip().lower()
            return "".join(c for c in _unicodedata.normalize("NFD", s) if _unicodedata.category(c) != "Mn")
        estado_norms = {norm(n): (n, c) for n, c in ESTADO_NOMBRE_TO_CODIGO.items()}

        # Detectar columnas de años. En Vista07a, la fila 11 suele tener años únicos o "Total XXXX"
        # Pero nos interesa el total anual de cada año.
        # Buscamos columnas que digan "Total 2024", "Total 2023", etc.
        year_cols = {}
        for c_idx in range(df.shape[1]):
            val = str(df.iloc[11, c_idx]).strip()
            if "Total" in val:
                try:
                    anio = int("".join(filter(str.isdigit, val)))
                    if 1990 < anio < 2100:
                        year_cols[anio] = c_idx
                except Exception:
                    continue

        if not year_cols:
            # Fallback a buscar simplemente números de 4 dígitos en fila 11
            for c_idx in range(df.shape[1]):
                val = str(df.iloc[11, c_idx]).strip()
                if val.isdigit() and len(val) == 4:
                    anio = int(val)
                    year_cols[anio] = c_idx

        # Detectar columna de estados
        best_col_idx = 1 # Columna B suele ser estados
        
        result = {}
        for row_idx in range(13, len(df)):
            cell = df.iloc[row_idx, best_col_idx]
            estado_nombre = str(cell).strip() if pd.notna(cell) else ""
            if not estado_nombre or len(estado_nombre) < 3:
                continue
            
            n = norm(estado_nombre)
            codigo = None
            for k, (nombre, c) in estado_norms.items():
                if n == k or n.startswith(k) or k.startswith(n):
                    codigo = c
                    break
            
            if not codigo:
                continue
            
            if codigo not in result:
                result[codigo] = {}
            
            for anio, c_idx in year_cols.items():
                v = df.iloc[row_idx, c_idx]
                val = float(pd.to_numeric(v, errors="coerce") or 0)
                if not pd.isna(val) and val > 0:
                    result[codigo][anio] = int(val)
        
        return result, None

    except Exception as e:
        import traceback
        print(f"Error en procesar turistas CETM: {e}\n{traceback.format_exc()}")
        return None, str(e)


def load_cetm_actividad_hotelera_todos_estados():
    """
    Intenta cargar actividad hotelera desde archivo local (CETM_LOCAL_XLSX) o desde URL del Compendio.
    Retorna {estado_codigo: {anio: {disponibles, ocupados, porc_ocupacion}}} o {}.
    """
    local_path = os.getenv("CETM_LOCAL_XLSX", "").strip()
    if local_path and os.path.isfile(local_path):
        data, _ = process_actividad_hotelera_from_upload(local_path)
        return data or {}
    # Opcional: URL de descarga del Compendio (ej. CETM año)
    url = os.getenv("CETM_EXCEL_URL", "").strip()
    if url:
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200 and len(resp.content) > 1000:
                import tempfile
                with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                    tmp.write(resp.content)
                    tmp.flush()
                    try:
                        data, _ = process_actividad_hotelera_from_upload(tmp.name)
                        return data or {}
                    finally:
                        try:
                            os.unlink(tmp.name)
                        except Exception:
                            pass
        except Exception:
            pass
    return {}


def process_aeropuertos_estatal_dgac(filepath: str):
    """
    Procesa archivo Excel de DGAC (CUADRO_DGAC) o AFAC (Producto Aeropuertos) para Aeropuertos Estatales.
    Retorna (data_list, error) donde data_list es [{estado_codigo, aeropuerto, grupo, anio, operaciones}, ...].
    """
    try:
        import pandas as pd
        import zipfile
        import io

        # Mapeo de Nombres de Aeropuerto a Códigos de Estado
        AIRPORT_TO_STATE_MAP = {
            "AICM": "09", "CIUDAD DE MÉXICO": "09", "CIUDAD DE MEXICO": "09",
            "AIFA": "15", "SANTA LUCÍA": "15", "SANTA LUCIA": "15",
            "ACAPULCO": "12", "ZIHUATANEJO": "12",
            "AGUASCALIENTES": "01",
            "TIJUANA": "02", "MEXICALI": "02",
            "LA PAZ": "03", "SAN JOSE DEL CABO": "03", "LORETO": "03",
            "CAMPECHE": "04", "CD. DEL CARMEN": "04",
            "CHIHUAHUA": "08", "CD. JUAREZ": "08", "CREEL": "08",
            "TORREON": "05",
            "COLIMA": "06", "MANZANILLO": "06",
            "TOLUCA": "15",
            "BAJIO": "11", "GUANAJUATO": "11", "LEÓN": "11", "LEON": "11",
            "GUADALAJARA": "14", "PUERTO VALLARTA": "14",
            "MORELIA": "16", "URUAPAN": "16",
            "CUERNAVACA": "17",
            "TEPIC": "18",
            "MONTERREY": "19", "DEL NORTE": "19",
            "OAXACA": "20", "HUATULCO": "20", "PUERTO ESCONDIDO": "20", "IXTEPEC": "20",
            "PUEBLA": "21",
            "QUERETARO": "22",
            "CANCUN": "23", "COZUMEL": "23", "CHETUMAL": "23", "TULÚM": "23", "TULUM": "23",
            "SAN LUIS POTOSI": "24",
            "CULIACAN": "25", "MAZATLAN": "25", "LOS MOCHIS": "25",
            "HERMOSILLO": "26", "CD. OBREGÓN": "26", "CD. OBREGON": "26", "GUAYMAS": "26", "NOGALES": "26", "PUERTO PEÑASCO": "26",
            "VILLAHERMOSA": "27",
            "TAMPICO": "28", "REYNOSA": "28", "MATAMOROS": "28", "NUEVO LAREDO": "28", "CD. VICTORIA": "28",
            "VERACRUZ": "30", "MINATITLAN": "30", "POZA RICA": "30",
            "MERIDA": "31", "CHICHÉN ITZÁ": "31", "CHICHEN ITZA": "31",
            "ZACATECAS": "32",
            "TUXTLA GUTIERREZ": "07", "TAPACHULA": "07", "PALENQUE": "07"
        }

        def norm(s):
            s = (s or "").strip().lower()
            return "".join(c for c in _unicodedata.normalize("NFD", s) if _unicodedata.category(c) != "Mn")

        # Manejar zip o xlsx
        if str(filepath).lower().endswith(".zip"):
            with zipfile.ZipFile(filepath, 'r') as z:
                files = [f for f in z.namelist() if f.lower().endswith(('.xlsx', '.xls'))]
                if not files:
                    return None, "No se encontró archivo Excel dentro del ZIP"
                with z.open(files[0]) as f:
                    xls = pd.ExcelFile(f.read())
        else:
            xls = pd.ExcelFile(filepath)

        out = []
        name_to_code = ESTADO_NOMBRE_TO_CODIGO
        
        # 1. Intentar formato Flat (tradicional de DGAC)
        for sheet_name in xls.sheet_names:
            try:
                df_raw = pd.read_excel(xls, sheet_name=sheet_name, nrows=10, header=None)
                h_row = -1
                for i, row in df_raw.iterrows():
                    row_vals = row.astype(str).str.lower().values
                    if any("aeropuerto" in str(v) for v in row_vals) and any("estado" in str(v) or "entidad" in str(v) for v in row_vals):
                        h_row = i
                        break
                if h_row == -1: continue
                
                df = pd.read_excel(xls, sheet_name=sheet_name, header=h_row)
                col_aero, col_estado, col_anio, col_oper = None, None, None, None
                for c in df.columns:
                    sc = str(c).strip().lower()
                    if "aeropuerto" in sc: col_aero = c
                    elif "estado" in sc or "entidad" in sc: col_estado = c
                    elif "ano" in sc or "anio" in sc or "year" in sc: col_anio = c
                    elif "operacion" in sc or "movimiento" in sc: col_oper = c
                
                if col_aero and col_estado and col_oper:
                    for _, r in df.iterrows():
                        aero = str(r.get(col_aero, "")).strip()
                        est_nom = str(r.get(col_estado, "")).strip()
                        if not aero or not est_nom or aero.lower() == "nan" or "total" in aero.lower():
                            continue
                        codigo = name_to_code.get(est_nom)
                        if not codigo:
                            n_est = norm(est_nom)
                            for nm, cc in name_to_code.items():
                                if norm(nm) == n_est:
                                    codigo = cc
                                    break
                        if not codigo: continue
                        anio = 2024
                        if col_anio:
                            try: anio = int(pd.to_numeric(r.get(col_anio), errors="coerce") or 2024)
                            except: pass
                        try:
                            oper = int(pd.to_numeric(r.get(col_oper), errors="coerce") or 0)
                            if oper > 0:
                                out.append({"estado_codigo": codigo, "aeropuerto": aero[:200], "grupo": None, "anio": anio, "operaciones": oper})
                        except: pass
            except: continue

        # 2. Si no hubo éxito, intentar formato Tabla Dinámica de AFAC (Producto Aeropuertos)
        if not out and "TD Prod Aptos" in xls.sheet_names:
            df = pd.read_excel(xls, "TD Prod Aptos", header=None)
            if not df.empty and len(df) > 4:
                # Buscar fila de años (etiquetas de columna o directamente una fila con años >= 2006)
                years_row_idx = -1
                for i in range(min(15, len(df))):
                    row_vals = df.iloc[i].to_list()
                    # Contar cuántos valores parecen ser años
                    year_count = 0
                    for val in row_vals:
                        try:
                            y = int(pd.to_numeric(val, errors="coerce"))
                            if 2000 <= y <= 2100: year_count += 1
                        except: pass
                    
                    if year_count >= 5:
                        years_row_idx = i
                        break
                
                if years_row_idx != -1:
                    year_cols = {}
                    for col_idx, val in enumerate(df.iloc[years_row_idx]):
                        try:
                            y = int(pd.to_numeric(val, errors="coerce"))
                            if 1990 <= y <= 2100:
                                year_cols[col_idx] = y
                        except: pass
                    
                    if year_cols:
                        for row_idx in range(years_row_idx + 1, len(df)):
                            label = str(df.iloc[row_idx, 0]).strip()
                            if not label or label.lower() == "nan" or "total" in label.lower() or label.lower() == "etiquetas de fila":
                                continue
                            
                            # Mapear nombre/etiqueta a estado de forma más robusta
                            state_code = None
                            clean_label = label.upper().replace("/", " ").replace(".", " ").replace("(", " ").replace(")", " ")
                            label_words = set(clean_label.split())
                            
                            for k, v in AIRPORT_TO_STATE_MAP.items():
                                k_words = set(k.upper().replace(".", " ").split())
                                if k_words.issubset(label_words) or label_words.issubset(k_words):
                                    state_code = v
                                    break
                            
                            if state_code:
                                for col_idx, year in year_cols.items():
                                    try:
                                        val = df.iloc[row_idx, col_idx]
                                        oper = int(pd.to_numeric(val, errors="coerce") or 0)
                                        if oper > 0:
                                            out.append({
                                                "estado_codigo": state_code,
                                                "aeropuerto": label[:100],
                                                "anio": year,
                                                "operaciones": oper
                                            })
                                    except: pass
        
        if not out:
            return None, "No se extrajeron datos de aeropuertos del archivo."
            
        return out, None

    except Exception as e:
        return None, str(e)


# ── 18. Municipios y Distribución Poblacional (INEGI ITER) ───

def fetch_and_process_iter_municipal():
    """
    Descarga y procesa el Censo de Población 2020 (ITER) de INEGI para obtener la distribución municipal.
    URL: https://www.inegi.org.mx/contenidos/programas/ccpv/2020/datosabiertos/iter/iter_00_cpv2020_csv.zip
    Retorna lista de dicts con datos de población y pirámide.
    """
    import requests
    import zipfile
    import io
    import pandas as pd
    import numpy as np

    url = "https://www.inegi.org.mx/contenidos/programas/ccpv/2020/datosabiertos/iter/iter_00_cpv2020_csv.zip"
    try:
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        
        # El archivo principal está en conjunto_de_datos/
        csv_file = [f for f in z.namelist() if "conjunto_de_datos" in f and f.endswith(".csv")][0]
        
        # Columnas necesarias: Identificación, Totales y Pirámide (5 años)
        base_cols = ["ENTIDAD", "NOM_ENT", "MUN", "NOM_MUN", "LOC", "POBTOT", "POBFEM", "POBMAS"]
        age_groups_source = [
            "P_0A4_F", "P_0A4_M", "P_5A9_F", "P_5A9_M", "P_10A14_F", "P_10A14_M",
            "P_15A19_F", "P_15A19_M", "P_20A24_F", "P_20A24_M", "P_25A29_F", "P_25A29_M",
            "P_30A34_F", "P_30A34_M", "P_35A39_F", "P_35A39_M", "P_40A44_F", "P_40A44_M",
            "P_45A49_F", "P_45A49_M", "P_50A54_F", "P_50A54_M", "P_55A59_F", "P_55A59_M",
            "P_60A64_F", "P_60A64_M", "P_65A69_F", "P_65A69_M", "P_70A74_F", "P_70A74_M",
            "P_75A79_F", "P_75A79_M", "P_80A84_F", "P_80A84_M", "P_85YMAS_F", "P_85YMAS_M"
        ]
        all_cols = base_cols + age_groups_source
        
        # Leer por trozos para no saturar memoria
        with z.open(csv_file) as f:
            chunks = pd.read_csv(f, usecols=all_cols, chunksize=100000, encoding="utf-8", dtype={"ENTIDAD": str, "MUN": str, "LOC": str})
            filtered_dfs = []
            for chunk in chunks:
                # Filtrar: ENTIDAD != 00 (Nacional), MUN != 000 (Estado Total), LOC == 0000 o 0 (Municipal Total)
                mask = (chunk["ENTIDAD"] != "00") & (chunk["MUN"] != "000") & (chunk["LOC"].astype(str).isin(["0", "0000"]))
                filtered_dfs.append(chunk[mask])
            
            df = pd.concat(filtered_dfs)
        
        # Limpieza de datos (INEGI usa '*' para valores confidenciales < 3)
        def clean_val(v):
            if str(v).strip() == '*': return 0
            try: return int(float(v))
            except: return 0

        results = []
        for _, row in df.iterrows():
            item = {
                "estado_codigo": row["ENTIDAD"].zfill(2),
                "estado_nombre": row["NOM_ENT"],
                "municipio_codigo": row["MUN"].zfill(3),
                "municipio_nombre": row["NOM_MUN"],
                "POBTOT": clean_val(row["POBTOT"]),
                "POBFEM": clean_val(row["POBFEM"]),
                "POBMAS": clean_val(row["POBMAS"]),
            }
            # Agregar pirámide (mapeando a las claves que espera el frontend)
            for col in age_groups_source:
                target_col = col.replace("85YMAS", "85A999")
                item[target_col] = clean_val(row[col])
            
            results.append(item)
            
        return results
    except Exception as e:
        print(f"Error en fetch_and_process_iter_municipal: {e}")
        return []


def fetch_municipios_catalog():
    """
    Obtiene el catálogo de municipios desde el archivo ITER 2020.
    """
    import requests
    import zipfile
    import io
    import pandas as pd
    import unicodedata

    def normalize_str(s: str) -> str:
        if not s: return ""
        nfkd = unicodedata.normalize("NFD", str(s).lower().strip())
        return "".join(c for c in nfkd if unicodedata.category(c) != "Mn")

    url = "https://www.inegi.org.mx/contenidos/programas/ccpv/2020/datosabiertos/iter/iter_00_cpv2020_csv.zip"
    try:
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        csv_file = [f for f in z.namelist() if "conjunto_de_datos" in f and f.endswith(".csv")][0]
        
        with z.open(csv_file) as f:
            # Solo necesitamos identificación
            cols = ["ENTIDAD", "NOM_ENT", "MUN", "NOM_MUN", "LOC"]
            chunks = pd.read_csv(f, usecols=cols, chunksize=100000, encoding="utf-8", dtype=str)
            filtered_dfs = []
            for chunk in chunks:
                mask = (chunk["ENTIDAD"] != "00") & (chunk["MUN"] != "000") & (chunk["LOC"].astype(str).isin(["0", "0000"]))
                filtered_dfs.append(chunk[mask])
            df = pd.concat(filtered_dfs)
        
        results = []
        for _, row in df.iterrows():
            muni_nombre = str(row["NOM_MUN"]).strip()
            results.append({
                "estado_codigo": row["ENTIDAD"].zfill(2),
                "estado_nombre": row["NOM_ENT"],
                "codigo": row["MUN"].zfill(3),
                "nombre": muni_nombre,
                "nombre_normalizado": normalize_str(muni_nombre)
            })
        
        # Agregar un campo virtual para el preview de Streamlit
        # (El UI de config.py espera mostrar algo en la tabla)
        if results:
            summary_df = pd.DataFrame(results).groupby("estado_nombre")["codigo"].count().reset_index()
            summary_df.columns = ["estado_nombre", "total_municipios"]
            # Pero la función save_fn espera la lista completa de municipios.
            # Para el preview en Streamlit, si devolvemos la lista completa es mucha data.
            # Sin embargo, el preview_fn de config.py suele devolver lo que se va a guardar.
            return results
        return []
    except Exception as e:
        print(f"Error en fetch_municipios_catalog: {e}")
        return []


def fetch_and_process_conapo_municipal():
    """
    Procesa proyecciones municipales de CONAPO.
    URL: https://www.datos.gob.mx/dataset/f2b9b220-3ef7-4e3a-bde6-87e1dac78c6a/resource/3c3092be-583e-4490-8c23-67ef9a64b198/download/pobproy_quinq1.csv
    """
    import pandas as pd
    import requests
    import io

    url = "https://www.datos.gob.mx/dataset/f2b9b220-3ef7-4e3a-bde6-87e1dac78c6a/resource/3c3092be-583e-4490-8c23-67ef9a64b198/download/pobproy_quinq1.csv"
    try:
        r = requests.get(url, timeout=45)
        if r.status_code == 200:
            # Reintentar decodificar mojibake si es necesario
            try:
                content = r.content.decode("utf-8")
            except:
                content = r.content.decode("latin1")
            
            df = pd.read_csv(io.StringIO(content))
            # Columnas observadas: CLAVE, MUNICIPIO, ENTIDAD, ANIO (o ANO), SEXO, POB (o POB_TOTAL)
            df.columns = [c.upper() for c in df.columns]
            
            results = []
            for _, row in df.iterrows():
                # CLAVE suele ser 5 dígitos (ENTIDAD + MUN)
                clave = str(row.get("CLAVE", row.get("CVE_MUN", ""))).zfill(5)
                if len(clave) != 5: continue
                
                estado_codigo = clave[:2]
                municipio_codigo = clave[2:]
                
                anio_val = row.get("ANO", row.get("ANIO", row.get("AO", 0)))
                pob_val = row.get("POB_TOTAL", row.get("POB", row.get("POBLACION", 0)))
                
                results.append({
                    "estado_codigo": estado_codigo,
                    "estado_nombre": str(row.get("NOM_ENT", row.get("ENTIDAD", ""))),
                    "municipio_codigo": municipio_codigo,
                    "municipio_nombre": str(row.get("NOM_MUN", row.get("MUNICIPIO", ""))),
                    "anio": int(anio_val) if anio_val else 0,
                    "sexo": str(row.get("SEXO", "")),
                    "poblacion": int(pob_val) if pob_val else 0
                })
            return results
        else:
            return []
    except Exception as e:
        print(f"Error en fetch_and_process_conapo_municipal: {e}")
        return []


# ── 19. Localidades (INEGI ITER 2005-2020) ───────────────────

def fetch_localidades_catalog():
    """
    Obtiene el catálogo de localidades desde el archivo ITER 2020.
    """
    import requests
    import zipfile
    import io
    import pandas as pd

    url = "https://www.inegi.org.mx/contenidos/programas/ccpv/2020/datosabiertos/iter/iter_00_cpv2020_csv.zip"
    try:
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        csv_file = [f for f in z.namelist() if "conjunto_de_datos" in f and f.endswith(".csv")][0]
        
        with z.open(csv_file) as f:
            # Seleccionar solo identificadores
            cols = ["ENTIDAD", "NOM_ENT", "MUN", "NOM_MUN", "LOC", "NOM_LOC"]
            chunks = pd.read_csv(f, usecols=cols, chunksize=100000, encoding="utf-8", dtype=str)
            filtered_dfs = []
            for chunk in chunks:
                # Filtrar solo localidades (evitar totales municipales y estatales)
                # LOC 0000 es municipal, 9998 es rural disperso, 9999 es no especificado
                mask = ~chunk["LOC"].astype(str).isin(["0", "0000", "9998", "9999"])
                filtered_dfs.append(chunk[mask])
            df = pd.concat(filtered_dfs)
        
        results = []
        for _, row in df.iterrows():
            results.append({
                "estado_codigo": row["ENTIDAD"].zfill(2),
                "estado_nombre": row["NOM_ENT"],
                "municipio_codigo": row["MUN"].zfill(3),
                "municipio_nombre": row["NOM_MUN"],
                "loc_codigo": row["LOC"].zfill(4),
                "localidad_nombre": row["NOM_LOC"]
            })
        return results
    except Exception as e:
        print(f"Error en fetch_localidades_catalog: {e}")
        return []


def fetch_and_process_iter_localidad():
    """
    Descarga y procesa el Censo de Población 2020 (ITER) de INEGI para obtener la distribución por localidad.
    """
    import requests
    import zipfile
    import io
    import pandas as pd

    url = "https://www.inegi.org.mx/contenidos/programas/ccpv/2020/datosabiertos/iter/iter_00_cpv2020_csv.zip"
    try:
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        csv_file = [f for f in z.namelist() if "conjunto_de_datos" in f and f.endswith(".csv")][0]
        
        base_cols = ["ENTIDAD", "NOM_ENT", "MUN", "NOM_MUN", "LOC", "NOM_LOC", "POBTOT", "POBFEM", "POBMAS"]
        age_groups_source = [
            "P_0A4_F", "P_0A4_M", "P_5A9_F", "P_5A9_M", "P_10A14_F", "P_10A14_M",
            "P_15A19_F", "P_15A19_M", "P_20A24_F", "P_20A24_M", "P_25A29_F", "P_25A29_M",
            "P_30A34_F", "P_30A34_M", "P_35A39_F", "P_35A39_M", "P_40A44_F", "P_40A44_M",
            "P_45A49_F", "P_45A49_M", "P_50A54_F", "P_50A54_M", "P_55A59_F", "P_55A59_M",
            "P_60A64_F", "P_60A64_M", "P_65A69_F", "P_65A69_M", "P_70A74_F", "P_70A74_M",
            "P_75A79_F", "P_75A79_M", "P_80A84_F", "P_80A84_M", "P_85YMAS_F", "P_85YMAS_M"
        ]
        all_cols = base_cols + age_groups_source
        
        with z.open(csv_file) as f:
            chunks = pd.read_csv(f, usecols=all_cols, chunksize=100000, encoding="utf-8", dtype={"ENTIDAD": str, "MUN": str, "LOC": str})
            filtered_dfs = []
            for chunk in chunks:
                mask = ~chunk["LOC"].astype(str).isin(["0", "0000", "9998", "9999"])
                filtered_dfs.append(chunk[mask])
            df = pd.concat(filtered_dfs)
        
        def clean_val(v):
            if str(v).strip() == '*': return 0
            try: return int(float(v))
            except: return 0

        results = []
        for _, row in df.iterrows():
            item = {
                "estado_codigo": row["ENTIDAD"].zfill(2),
                "estado_nombre": row["NOM_ENT"],
                "municipio_codigo": row["MUN"].zfill(3),
                "municipio_nombre": row["NOM_MUN"],
                "loc_codigo": row["LOC"].zfill(4),
                "localidad_nombre": row["NOM_LOC"],
                "POBTOT": clean_val(row["POBTOT"]),
                "POBFEM": clean_val(row["POBFEM"]),
                "POBMAS": clean_val(row["POBMAS"]),
            }
            # Pirámide (mapeando keys)
            piramide = {col.replace("85YMAS", "85A999"): clean_val(row[col]) for col in age_groups_source}
            item.update(piramide) # Unir para que sea un dict plano
            results.append(item)
            
        return results
    except Exception as e:
        print(f"Error en fetch_and_process_iter_localidad: {e}")
        return []


def fetch_and_process_crecimiento_historico_localidad():
    """
    Descarga y procesa censos 2005, 2010 y 2020 para obtener crecimiento histórico por localidad.
    """
    import requests
    import zipfile
    import io
    import pandas as pd

    urls = {
        2005: "https://www.inegi.org.mx/contenidos/programas/ccpv/2005/datosabiertos/cpv2005_iter_00_csv.zip",
        2010: "https://www.inegi.org.mx/contenidos/programas/ccpv/2010/datosabiertos/iter_nal_2010_csv.zip",
        2020: "https://www.inegi.org.mx/contenidos/programas/ccpv/2020/datosabiertos/iter/iter_00_cpv2020_csv.zip"
    }
    
    combined_data = []

    for year, url in urls.items():
        try:
            print(f"Descargando ITER {year}...")
            resp = requests.get(url, timeout=180)
            resp.raise_for_status()
            z = zipfile.ZipFile(io.BytesIO(resp.content))
            
            # Encontrar el CSV de datos
            if year == 2020:
                csv_file = [f for f in z.namelist() if "conjunto_de_datos" in f and f.endswith(".csv")][0]
            elif year == 2010:
                # El ITER 2010 Nacional suele tener el CSV en la raíz o subcarpeta conj_de_datos
                csv_file = [f for f in z.namelist() if f.endswith(".csv") and "iter_00" in f.lower() and "datos" in f.lower()]
                if not csv_file:
                    csv_file = [f for f in z.namelist() if f.endswith(".csv") and "iter" in f.lower() and "catalogos" not in f.lower()][0]
                else:
                    csv_file = csv_file[0]
            else: # 2005
                csv_file = [f for f in z.namelist() if f.endswith(".csv") and "cpv2005" in f.lower() and "iter" in f.lower()][0]
            
            with z.open(csv_file) as f:
                # Detección dinámica de nombres de columna
                # Usar latin-1 como fallback ya que ITER antiguos suelen usarlo
                try:
                    df_header = pd.read_csv(f, encoding="utf-8", dtype=str, nrows=0)
                except:
                    f.seek(0)
                    df_header = pd.read_csv(f, encoding="latin-1", dtype=str, nrows=0)
                
                actual_cols = df_header.columns.tolist()
                
                # Función auxiliar para buscar columna ignorando mayúsculas
                def find_col(possible_names, cols):
                    for name in possible_names:
                        for c in cols:
                            if c.upper().strip() == name.upper(): return c
                    return None

                # Mapeo flexible
                map_rules = {
                    "ENTIDAD": ["ENTIDAD", "EDO", "CVE_ENT"],
                    "MUN": ["MUN", "MUNICIPIO", "CVE_MUN"],
                    "LOC": ["LOC", "LOCALIDAD", "CVE_LOC"],
                    "NOM_LOC": ["NOM_LOC", "LOCALIDAD_NOM"],
                    "NOM_ENT": ["NOM_ENT", "ENTIDAD_NOM"],
                    "NOM_MUN": ["NOM_MUN", "MUNICIPIO_NOM"],
                    "POBTOT": ["POBTOT", "POBLACION_TOTAL", "P_TOTAL"],
                    "POBFEM": ["POBFEM", "POBLACION_FEMENINA", "P_FEM"],
                    "POBMAS": ["POBMAS", "POBLACION_MASCULINA", "P_MAS"]
                }
                
                target_cols = {target: find_col(options, actual_cols) for target, options in map_rules.items()}
                
                # Columnas críticas
                critical = ["ENTIDAD", "MUN", "LOC", "POBTOT"]
                if not all(target_cols[c] for c in critical):
                    print(f"Faltan columnas críticas para {year}: {[c for c in critical if not target_cols[c]]}")
                    continue

                f.seek(0)
                # Seleccionar solo las columnas que encontramos
                use_cols = [v for v in target_cols.values() if v]
                chunks = pd.read_csv(f, usecols=use_cols, chunksize=50000, encoding="latin-1" if "latin-1" in locals() else "utf-8", dtype=str)
                
                count_year = 0
                for chunk in chunks:
                    # Renombrar a nombres estándar
                    chunk.rename(columns={v: k for k, v in target_cols.items() if v}, inplace=True)
                    
                    # Filtrar localidades reales
                    mask = ~chunk["LOC"].astype(str).isin(["0", "0000", "9998", "9999", "Total"])
                    # Asegurar que ENTIDAD y MUN no sean totales
                    if "ENTIDAD" in chunk.columns:
                        mask = mask & (~chunk["ENTIDAD"].astype(str).isin(["0", "00", "000"]))
                    if "MUN" in chunk.columns:
                        mask = mask & (~chunk["MUN"].astype(str).isin(["0", "00", "000"]))
                        
                    df_clean = chunk[mask].copy()
                    
                    def clean_v(v):
                        try:
                            # Manejar '*', NaNs y nulos
                            vn = pd.to_numeric(v, errors="coerce")
                            if pd.isna(vn): return 0
                            return int(vn)
                        except:
                            return 0

                    for _, row in df_clean.iterrows():
                        combined_data.append({
                            "estado_codigo": str(row.get("ENTIDAD", "")).zfill(2),
                            "estado_nombre": str(row.get("NOM_ENT", "")),
                            "municipio_codigo": str(row.get("MUN", "")).zfill(3),
                            "municipio_nombre": str(row.get("NOM_MUN", "")),
                            "loc_codigo": str(row.get("LOC", "")).zfill(4),
                            "localidad_nombre": str(row.get("NOM_LOC", "")),
                            "anio": year,
                            "poblacion": clean_v(row.get("POBTOT", 0)),
                            "hombres": clean_v(row.get("POBMAS", 0)),
                            "mujeres": clean_v(row.get("POBFEM", 0))
                        })
                        count_year += 1
                print(f"Procesados {count_year} registros para el año {year}")
                
        except Exception as e:
            print(f"Error procesando {year}: {e}")
            import traceback
            traceback.print_exc()
            
            
    return combined_data

