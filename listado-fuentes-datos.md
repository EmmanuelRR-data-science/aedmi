# Listado de Fuentes de Datos

Este documento describe todas las fuentes de datos externas y locales que alimentan el dashboard de indicadores económicos.

## 1. Organismos Nacionales (APIs y Portales)

### INEGI (API de Indicadores)
*   **Descripción:** Principal fuente de indicadores macroeconómicos y demográficos de México.
*   **Método de Consulta:** API REST (JSON/XML).
*   **URL Base:** `https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR/{id}/es/00/true/{fuente}/2.0/{token}?type=json`
*   **Indicadores Consultados:**
    *   `733671`: PIB Nacional (Anual/Trimestral).
    *   `628194`: Inflación (INPC).
    *   `601614`: Población Económicamente Activa (PEA).
    *   `289242`: Población Ocupada Total.
    *   `1002000002` / `1002000003`: Distribución por Sexo (Hombres/Mujeres).
    *   `6207067158`: ITAEE Estatal (Indicador Trimestral de la Actividad Económica Estatal).
    *   `6207123161`: Balanza de Visitantes (Entradas/Salidas).

### Banco de México (API SIE)
*   **Descripción:** Fuente oficial para el tipo de cambio y política monetaria.
*   **Método de Consulta:** API REST SIE.
*   **URL Base:** `https://www.banxico.org.mx/SieAPIRest/service/v1/series/{series}/datos`
*   **Indicadores Consultados:**
    *   `SF43718`: Tipo de Cambio FIX (MXN/USD).
    *   `SF43783`: Tipo de Cambio Promedio Mensual.
    *   `SP1`: Inflación Mensual.

### DataMéxico (Secretaría de Economía)
*   **Descripción:** Integración de datos económicos, comercio exterior e inversión.
*   **Método de Consulta:** API Tesseract (JSON).
*   **URLs:**
    *   **Anuncios de Inversión:** `https://www.economia.gob.mx/apidatamexico/tesseract/cubes/Anuncios_Inversion_Combinados_General/aggregate.jsonrecords`
    *   **Balanza Comercial por Producto:** `https://www.economia.gob.mx/apidatamexico/tesseract/cubes/inegi_foreign_trade_product/aggregate.jsonrecords`
    *   **Exportaciones por Estado:** `https://api.datamexico.org/tesseract/data.jsonrecords?cube=economy_foreign_trade_ent&drilldowns=Year,State&measures=Trade+Value`

### DataTur (SECTUR) / DGAC (AFAC)
*   **Descripción:** Estadísticas de turismo y transporte aéreo.
*   **Método de Consulta:** Descarga directa de archivos CSV/ZIP y procesamiento local.
*   **URLs:**
    *   **Ocupación Hotelera:** `https://repodatos.atdt.gob.mx/s_turismo/ocupacion_hotelera/Base70centros.csv` (Delimitado por Tabulado).
    *   **Participación de Mercado Aéreo:** `https://datatur.sectur.gob.mx/Documentos%20compartidos/CUADRO_DGAC.zip` (Archivo Excel contenido en el ZIP).

## 2. Organismos Internacionales

### Banco Mundial (World Bank API)
*   **Descripción:** Datos globales de PIB y turismo internacional.
*   **Método de Consulta:** API V2 (JSON).
*   **URL Base:** `https://api.worldbank.org/v2/country/MX/indicator/{indicador}?format=json`
*   **Indicadores Consultados:**
    *   `NY.GDP.MKTP.CD`: PIB Total (USD).
    *   `NY.GDP.PCAP.CD`: PIB Per Cápita (USD).
    *   `ST.INT.RCPT.CD`: Ranking Turismo Mundial (Ingresos).

### Fondo Monetario Internacional (IMF DataMapper)
*   **Descripción:** Proyecciones económicas a mediano plazo.
*   **Método de Consulta:** API DataMapper.
*   **URL Base:** `https://www.imf.org/external/datamapper/api/v1/{indicador}/MEX`
*   **Indicadores Consultados:**
    *   `NGDPD`: Proyección PIB Total (Billions USD).
    *   `NGDPDPC`: Proyección PIB Per Cápita (USD).

## 3. Otras Fuentes Gubernamentales

### CONAPO
*   **Descripción:** Proyecciones de población por entidad federativa.
*   **Método de Consulta:** Descarga de dataset oficial.
*   **URL:** `https://raw.githubusercontent.com/DataMx/conapo/master/datos/proyecciones/entidades/proyecciones_entidades.csv`

### SICT (Secretaría de Infraestructura, Comunicaciones y Transportes)
*   **Descripción:** Mapas de conectividad carretera estatal.
*   **Método de Consulta:** Raspado de imágenes desde PDFs oficiales de "Datos Viales".
*   **URL Base:** `https://micrs.sct.gob.mx/images/DireccionesGrales/DGST/Datos-Viales-2016/{codigo}_{nombre_estado}.pdf`

## 4. Archivos Locales y Base de Datos (data/process)

El sistema utiliza archivos CSV/JSON locales como respaldo (fallback) y para datos que requieren curación manual:
*   `pib_estatal_consolidado.csv`: Datos históricos de PIB por estado.
*   `demografia_estatal_{id}.json`: Perfiles demográficos detallados por entidad (Censo 2020).
*   `poblacion_total_merida.json` / `merida_indicators_manual.sql`: Datos específicos de la ciudad de Mérida/Monterrey inyectados vía SQL inicial.
*   `ied_historico_entidad_sector.csv`: Datos de Inversión Extranjera Directa históricos para Nuevo León y Monterrey.

---
*Nota: Todos los tokens de acceso (INEGI, Banxico) deben configurarse en el archivo `.env` del servidor.*
