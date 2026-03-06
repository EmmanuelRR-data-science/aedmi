# Fuentes de datos del proyecto EDMI-APP-VPS

Listado de fuentes de datos en uso, con método de acceso (archivo descargable, API con/sin token, web scraping).

---

## 1. API **con** token

| Fuente | Indicadores / Uso | Método de acceso | Variable de entorno |
|--------|--------------------|------------------|----------------------|
| **Banxico (SIE)** | Tipo de cambio FIX (SF43718), Inflación INPC (SP1) | API REST (`SieAPIRest`). Header `Bmx-Token` o query `token`. | `BANXICO_TOKEN` |
| **INEGI (BIE/BISE)** | PIB, crecimiento poblacional, estructura por edad, PEA, sector de actividad, distribución por sexo, PIB trimestral, balanza de visitantes (entradas/salidas) | API REST (`app/api/indicadores/desarrolladores/jsonxml`). Token en URL. | `INEGI_TOKEN` |
| **GROQ** | Respuestas de IA (chat/analista). No es fuente de indicadores. | API (cliente Groq). | `GROQ_API_KEY` |

---

## 2. API **sin** token

| Fuente | URL / Origen | Indicadores / Uso |
|--------|--------------|--------------------|
| **Economía / DataMéxico (Tesseract)** | `https://www.economia.gob.mx/apidatamexico/tesseract/cubes/...` | Balanza comercial por producto (`inegi_foreign_trade_product`), Anuncios de inversión combinados, Anuncios de inversión base |
| **DataMéxico (api.datamexico.org)** | `https://api.datamexico.org/tesseract/...` | Exportaciones por estado (`economy_foreign_trade_ent`) |
| **Banco Mundial (World Bank)** | `https://api.worldbank.org/v2/country/MX/indicator/...` | PIB histórico (total y per capita). Indicador turismo mundial: `ST.INT.RCPT.CD` |
| **FMI (IMF DataMapper)** | `https://www.imf.org/external/datamapper/api/v1` | Proyección PIB (WEO): NGDPD, NGDPDPC para México |
| **datos.gob.mx (CKAN)** | `https://www.datos.gob.mx/api/3/action/package_show?id=inversion_extranjera_directa` + recurso CSV descubierto | IED por sectores, IED flujo por entidad, IED por país de origen |
| **open.er-api.com** | `https://open.er-api.com/v6/latest/USD` | Tipo de cambio USD/MXN (fallback cuando no hay Banxico) |

---

## 3. Archivo descargable (CSV / XLSX / ZIP)

| Fuente | Formato | URL o ubicación | Uso |
|--------|---------|------------------|-----|
| **CONAPO (DataMx GitHub)** | CSV | `https://raw.githubusercontent.com/DataMx/conapo/master/datos/proyecciones/entidades/proyecciones_entidades.csv` (o `CONAPO_CSV_URL`) | Proyecciones de población por entidad (2025–2030). Se descarga a `data/process/proyecciones_conapo.csv`. |
| **DataTur / ATDT (ocupación hotelera)** | CSV | `https://repodatos.atdt.gob.mx/s_turismo/ocupacion_hotelera/Base70centros.csv` | Actividad hotelera nacional (agregado anual y por categoría). |
| **DataTur / SECTUR (DGAC)** | ZIP (contiene XLSX) | `https://datatur.sectur.gob.mx/Documentos%20compartidos/CUADRO_DGAC.zip` | Participación mercado aéreo (nacional/internacional) y operaciones por aeropuerto/estado. Se descarga ZIP y se extrae/lee el Excel. |
| **CETM SECTUR (Actividad hotelera estatal)** | XLSX | Archivo local (`CETM_LOCAL_XLSX`) o descarga desde página del Compendio (CETM). No hay URL fija pública en código. | Actividad hotelera por estado y mes (6_2.xlsx). El ETL usa archivo local o subida vía API. |

---

## 4. Archivos locales (CSV / Excel) – respaldo o insumo

| Archivo / patrón | Ubicación típica | Uso |
|-------------------|-------------------|-----|
| CSV de respaldo varios indicadores | `data/process/` | Fallback cuando API o BD fallan: `crecimiento_poblacional_nacional.csv`, `pib_historico_banco_mundo.csv`, `pob_sector_actividad.csv`, `pea_inegi.csv`, `distribucion_sexo_inegi.csv`, `estructura_poblacional_inegi.csv`, `inflacion_nacional.csv`, `tipo_cambio_banxico_*.csv`, `balanza_visitantes_inegi.csv`, `pib_proyeccion_fmi.csv`, `ied_flujo.csv`, `ied_sectores.csv`, `ied_paises.csv`, `exportaciones_estatal.csv`, `proyecciones_conapo.csv`. |
| Excel CETM | Variable (`CETM_LOCAL_XLSX` o subida) | Actividad hotelera estatal (6_2.xlsx). |

---

## 5. Web scraping

| Fuente | URL | Método | Uso |
|--------|-----|--------|-----|
| **Observatur Yucatán** | `https://www.observaturyucatan.org.mx/indicadores` | HTTP + BeautifulSoup (tablas con clase `rw_mid_poocupada`, atributos `data-yr`, `data-mnth`, `data-vl`) | Población ocupada en turismo (Mérida/Yucatán). Usado en ETL vía `_scrape_poblacion_ocupada_observatur`. |

---

## Resumen por método

- **API con token:** Banxico, INEGI, GROQ (solo IA).
- **API sin token:** Economía/DataMéxico, api.datamexico.org, Banco Mundial, FMI, datos.gob.mx (CKAN), open.er-api.com.
- **Archivo descargable (CSV/XLSX/ZIP):** CONAPO (CSV), DataTur Base70centros (CSV), CUADRO_DGAC (ZIP→XLSX), CETM (XLSX local o descarga).
- **Archivos locales:** Múltiples CSV en `data/process/` como respaldo; Excel CETM como insumo.
- **Web scraping:** Observatur (BeautifulSoup).
