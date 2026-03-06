-- Esquema para KPIs nacionales - Aplicación Estudios de Mercado
-- Se ejecuta al iniciar el contenedor PostgreSQL

CREATE TABLE IF NOT EXISTS kpis_nacional (
    indicator VARCHAR(50) PRIMARY KEY,
    value NUMERIC,
    date VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_log (
    id SERIAL PRIMARY KEY,
    run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20),
    indicators_updated INTEGER DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS crecimiento_poblacional_nacional (
    id SERIAL PRIMARY KEY,
    year INT,
    value BIGINT
);

CREATE TABLE IF NOT EXISTS estructura_poblacional_inegi (
    id SERIAL PRIMARY KEY,
    year INT,
    pob_0_14 BIGINT,
    pob_15_64 BIGINT,
    pob_65_plus BIGINT
);

CREATE TABLE IF NOT EXISTS distribucion_sexo_inegi (
    id SERIAL PRIMARY KEY,
    year INT,
    male BIGINT,
    female BIGINT
);

CREATE TABLE IF NOT EXISTS pea_inegi (
    id SERIAL PRIMARY KEY,
    anio INT,
    trimestre INT,
    valor BIGINT
);

CREATE TABLE IF NOT EXISTS pob_sector_actividad (
    id SERIAL PRIMARY KEY,
    sector VARCHAR(100),
    valor BIGINT,
    pct DECIMAL(10, 4),
    es_residual BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS pib_nacional (
    id SERIAL PRIMARY KEY,
    fecha DATE,
    anio INT,
    trimestre INT,
    pib_total_millones DECIMAL(20, 4),
    pib_per_capita DECIMAL(20, 4)
);

-- Tipo de cambio (Banxico SF43718 FIX) - pq-estudios-mercado-vps
CREATE TABLE IF NOT EXISTS tipo_cambio_banxico_diario (
    id SERIAL PRIMARY KEY,
    fecha DATE,
    tc DECIMAL(10, 4),
    dato DECIMAL(10, 4)
);

CREATE TABLE IF NOT EXISTS tipo_cambio_banxico_mensual (
    id SERIAL PRIMARY KEY,
    fecha DATE,
    tc_prom_mes DECIMAL(10, 4)
);

-- Inflación nacional (Banxico INPC SP1) - inflacion_nacional.ipynb
CREATE TABLE IF NOT EXISTS inflacion_nacional (
    anio INT,
    mes INT,
    inflacion DECIMAL(10, 4),
    texto_fecha VARCHAR(50),
    PRIMARY KEY (anio, mes)
);

-- IED Flujo por Entidad (últimos 4 trimestres - inversion_extranjera_ied.ipynb)
CREATE TABLE IF NOT EXISTS ied_flujo_entidad (
    id SERIAL PRIMARY KEY,
    entidad VARCHAR(100),
    mdd_4t DECIMAL(20, 4),
    rank INT,
    periodo VARCHAR(20)
);

-- Ranking Turismo Mundial (Banco Mundial WDI - pq-estudios-mercado-vps)
CREATE TABLE IF NOT EXISTS ranking_turismo_wb (
    id SERIAL PRIMARY KEY,
    iso VARCHAR(10),
    country VARCHAR(120),
    year INT,
    val DECIMAL(20, 2)
);

-- IED por País de Origen (Secretaría de Economía - pq-estudios-mercado-vps)
CREATE TABLE IF NOT EXISTS ied_paises (
    id SERIAL PRIMARY KEY,
    pais VARCHAR(120),
    monto_mdd DECIMAL(20, 4),
    periodo VARCHAR(20)
);

-- IED por Sector Económico (Secretaría de Economía - pq-estudios-mercado-vps)
CREATE TABLE IF NOT EXISTS ied_sectores (
    id SERIAL PRIMARY KEY,
    sector VARCHAR(100),
    monto_mdd DECIMAL(20, 4),
    periodo VARCHAR(20)
);

-- Balanza de Visitantes (INEGI BISE - pq-estudios-mercado-vps)
CREATE TABLE IF NOT EXISTS balanza_visitantes_inegi (
    id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    entradas DECIMAL(20, 2),
    salidas DECIMAL(20, 2),
    balance DECIMAL(20, 2)
);

-- Balanza Comercial por Producto (Economía/DataMéxico - inegi_foreign_trade_product). ETL carga; API lee.
CREATE TABLE IF NOT EXISTS balanza_comercial_producto (
    id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    flow_id INT NOT NULL,
    product VARCHAR(200) NOT NULL,
    trade_value DECIMAL(24, 2) NOT NULL,
    UNIQUE(year, flow_id, product)
);
CREATE INDEX IF NOT EXISTS idx_bcp_year ON balanza_comercial_producto (year);
CREATE INDEX IF NOT EXISTS idx_bcp_flow ON balanza_comercial_producto (flow_id);

-- Producto Aeropuertos Nacional (Excel producto-aeropuertos 2006-2025). ETL carga; API lee.
CREATE TABLE IF NOT EXISTS producto_aeropuertos_nacional (
    id SERIAL PRIMARY KEY,
    anio INT NOT NULL,
    aeropuerto VARCHAR(250) NOT NULL,
    operaciones BIGINT NOT NULL,
    UNIQUE(anio, aeropuerto)
);
CREATE INDEX IF NOT EXISTS idx_producto_aeropuertos_nacional_anio ON producto_aeropuertos_nacional (anio);

-- Participación Mercado Aéreo (AFAC/DataTur - pq-estudios-mercado-vps)
CREATE TABLE IF NOT EXISTS participacion_mercado_aereo (
    id SERIAL PRIMARY KEY,
    aerolinea VARCHAR(150),
    participacion DECIMAL(10, 6)
);

CREATE TABLE IF NOT EXISTS participacion_internacional_region (
    id SERIAL PRIMARY KEY,
    region VARCHAR(150),
    pasajeros DECIMAL(20, 2)
);

-- Anuncios de Inversión Combinados (DataMéxico - pq-estudios-mercado-vps)
CREATE TABLE IF NOT EXISTS anuncios_inversion_combinados (
    id SERIAL PRIMARY KEY,
    anio INT NOT NULL,
    num_anuncios INT DEFAULT 0,
    monto_inversion DECIMAL(24, 2),
    state VARCHAR(100)
);

-- Anuncios de Inversión Base (DataMéxico - pq-estudios-mercado-vps)
CREATE TABLE IF NOT EXISTS anuncios_inversion_base (
    id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    country VARCHAR(200),
    state VARCHAR(100),
    ia_sector VARCHAR(200),
    monto_inversion DECIMAL(24, 2)
);

-- Proyección PIB (FMI WEO) - pib_proyeccion.ipynb
CREATE TABLE IF NOT EXISTS pib_proyeccion_fmi (
    anio INT PRIMARY KEY,
    pib_total_mxn_billones DECIMAL(20, 4),
    pib_total_usd_billones DECIMAL(20, 4),
    pib_per_capita_mxn DECIMAL(20, 4),
    pib_per_capita_usd DECIMAL(20, 4)
);

-- Demografía estatal (INEGI - estado_crecimiento_hist.ipynb). ETL escribe; API lee para las 3 gráficas.
CREATE TABLE IF NOT EXISTS demografia_estatal_crecimiento (
    estado_codigo VARCHAR(2) NOT NULL,
    anio INT NOT NULL,
    valor BIGINT,
    crecimiento_pct DECIMAL(10, 2),
    PRIMARY KEY (estado_codigo, anio)
);
CREATE TABLE IF NOT EXISTS demografia_estatal_genero (
    estado_codigo VARCHAR(2) NOT NULL,
    anio INT NOT NULL,
    hombres BIGINT,
    mujeres BIGINT,
    PRIMARY KEY (estado_codigo, anio)
);
CREATE TABLE IF NOT EXISTS demografia_estatal_edad (
    estado_codigo VARCHAR(2) NOT NULL,
    anio INT NOT NULL,
    g_0_19 BIGINT,
    g_20_64 BIGINT,
    g_65_plus BIGINT,
    no_especificado BIGINT,
    PRIMARY KEY (estado_codigo, anio)
);

-- Proyecciones de Población (CONAPO - estado_proyeccion.ipynb). Años 2025-2030 por estado.
CREATE TABLE IF NOT EXISTS proyecciones_conapo (
    estado_codigo VARCHAR(2) NOT NULL,
    anio INT NOT NULL,
    total BIGINT,
    hombres BIGINT,
    mujeres BIGINT,
    PRIMARY KEY (estado_codigo, anio)
);

-- Actividad Económica ITAEE por estado (estado_pib_sectores.ipynb). Último año disponible por sector.
CREATE TABLE IF NOT EXISTS itaee_estatal (
    estado_codigo VARCHAR(2) NOT NULL,
    anio VARCHAR(10) NOT NULL,
    sector VARCHAR(20) NOT NULL,
    valor DECIMAL(20, 4),
    PRIMARY KEY (estado_codigo, anio, sector)
);

-- Actividad Hotelera por estado (CETM SECTUR - estado_turismo_llegadas.ipynb). Por año y 12 meses.
CREATE TABLE IF NOT EXISTS actividad_hotelera_estatal (
    estado_codigo VARCHAR(2) NOT NULL,
    anio INT NOT NULL DEFAULT 2024,
    mes_num INT NOT NULL,
    disponibles DECIMAL(14, 2),
    ocupados DECIMAL(14, 2),
    porc_ocupacion DECIMAL(6, 2),
    PRIMARY KEY (estado_codigo, anio, mes_num)
);
CREATE INDEX IF NOT EXISTS idx_actividad_hotelera_estatal_anio ON actividad_hotelera_estatal (estado_codigo, anio);

-- Actividad Hotelera nacional (DataTur - Base70centros.csv). Agregado anual.
CREATE TABLE IF NOT EXISTS actividad_hotelera_nacional (
    anio INT PRIMARY KEY,
    cuartos_disponibles_pd DECIMAL(20, 2),
    cuartos_ocupados_pd DECIMAL(20, 2),
    porc_ocupacion DECIMAL(6, 2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Actividad Hotelera nacional por categoría (DataTur - Base70centros.csv). Último año por categoría.
CREATE TABLE IF NOT EXISTS actividad_hotelera_nacional_por_categoria (
    anio INT NOT NULL,
    categoria VARCHAR(80) NOT NULL,
    cuartos_disponibles_pd DECIMAL(20, 2),
    cuartos_ocupados_pd DECIMAL(20, 2),
    porc_ocupacion DECIMAL(6, 2),
    PRIMARY KEY (anio, categoria)
);
CREATE INDEX IF NOT EXISTS idx_ah_nacional_cat_anio ON actividad_hotelera_nacional_por_categoria (anio);

-- Exportaciones por Estado (INEGI vía DataMéxico)
CREATE TABLE IF NOT EXISTS exportaciones_estatal (
    id SERIAL PRIMARY KEY,
    estado_codigo VARCHAR(2) NOT NULL,
    estado_slug VARCHAR(100) NOT NULL,
    anio INT NOT NULL,
    trade_value NUMERIC(15, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(estado_codigo, anio)
);
CREATE INDEX IF NOT EXISTS idx_exportaciones_estatal_anio ON exportaciones_estatal (estado_codigo, anio);
CREATE INDEX IF NOT EXISTS idx_exportaciones_estatal_slug ON exportaciones_estatal (estado_slug, anio);

-- Aeropuertos por Estado (DGAC)
CREATE TABLE IF NOT EXISTS aeropuertos_estatal (
    id SERIAL PRIMARY KEY,
    estado_codigo VARCHAR(2) NOT NULL,
    aeropuerto VARCHAR(200) NOT NULL,
    grupo VARCHAR(50),
    anio INT NOT NULL,
    operaciones INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(estado_codigo, aeropuerto, anio)
);
CREATE INDEX IF NOT EXISTS idx_aeropuertos_estatal_anio ON aeropuertos_estatal (estado_codigo, anio);
CREATE INDEX IF NOT EXISTS idx_aeropuertos_estatal_aeropuerto ON aeropuertos_estatal (aeropuerto, anio);

-- Municipios (Catálogo INEGI)
CREATE TABLE IF NOT EXISTS municipios (
    id SERIAL PRIMARY KEY,
    estado_codigo VARCHAR(2) NOT NULL,
    estado_nombre VARCHAR(100) NOT NULL,
    municipio_codigo VARCHAR(3) NOT NULL,
    municipio_nombre VARCHAR(255) NOT NULL,
    municipio_nombre_normalizado VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(estado_codigo, municipio_codigo)
);
CREATE INDEX IF NOT EXISTS idx_municipios_estado ON municipios (estado_codigo);
CREATE INDEX IF NOT EXISTS idx_municipios_nombre_normalizado ON municipios (municipio_nombre_normalizado);

-- Distribución de Población Municipal (Censo 2020)
CREATE TABLE IF NOT EXISTS distribucion_poblacion_municipal (
    id SERIAL PRIMARY KEY,
    estado_codigo VARCHAR(2) NOT NULL,
    estado_nombre VARCHAR(100) NOT NULL,
    municipio_codigo VARCHAR(3) NOT NULL,
    municipio_nombre VARCHAR(255) NOT NULL,
    pobtot INT,
    pobfem INT,
    pobmas INT,
    data_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(estado_codigo, municipio_codigo)
);
CREATE INDEX IF NOT EXISTS idx_distribucion_poblacion_municipal_estado ON distribucion_poblacion_municipal (estado_codigo, municipio_codigo);

-- Proyección Poblacional Municipal (CONAPO)
CREATE TABLE IF NOT EXISTS proyeccion_poblacional_municipal (
    id SERIAL PRIMARY KEY,
    estado_codigo VARCHAR(2) NOT NULL,
    estado_nombre VARCHAR(100) NOT NULL,
    municipio_codigo VARCHAR(3) NOT NULL,
    municipio_nombre VARCHAR(255) NOT NULL,
    anio INT NOT NULL,
    sexo VARCHAR(20) NOT NULL,
    poblacion INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(estado_codigo, municipio_codigo, anio, sexo)
);
CREATE INDEX IF NOT EXISTS idx_proyeccion_poblacional_municipal_estado ON proyeccion_poblacional_municipal (estado_codigo, municipio_codigo, anio);

-- Localidades (INEGI Censo 2020, LOC != 0, 9998, 9999)
CREATE TABLE IF NOT EXISTS localidades (
    id SERIAL PRIMARY KEY,
    estado_codigo VARCHAR(2) NOT NULL,
    estado_nombre VARCHAR(100) NOT NULL,
    municipio_codigo VARCHAR(3) NOT NULL,
    municipio_nombre VARCHAR(255) NOT NULL,
    loc_codigo VARCHAR(10) NOT NULL,
    localidad_nombre VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(estado_codigo, municipio_codigo, loc_codigo)
);
CREATE INDEX IF NOT EXISTS idx_localidades_estado ON localidades (estado_codigo);
CREATE INDEX IF NOT EXISTS idx_localidades_estado_mun ON localidades (estado_codigo, municipio_codigo);

-- Distribución de Población por Localidad (Censo 2020)
CREATE TABLE IF NOT EXISTS distribucion_poblacion_localidad (
    id SERIAL PRIMARY KEY,
    estado_codigo VARCHAR(2) NOT NULL,
    estado_nombre VARCHAR(100) NOT NULL,
    municipio_codigo VARCHAR(3) NOT NULL,
    municipio_nombre VARCHAR(255) NOT NULL,
    loc_codigo VARCHAR(10) NOT NULL,
    localidad_nombre VARCHAR(255) NOT NULL,
    pobtot INT,
    pobfem INT,
    pobmas INT,
    data_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(estado_codigo, municipio_codigo, loc_codigo)
);
CREATE INDEX IF NOT EXISTS idx_distribucion_poblacion_localidad_lookup ON distribucion_poblacion_localidad (estado_codigo, municipio_codigo, loc_codigo);

-- Crecimiento histórico por localidad (2005, 2010, 2020 - INEGI)
CREATE TABLE IF NOT EXISTS crecimiento_historico_localidad (
    id SERIAL PRIMARY KEY,
    estado_codigo VARCHAR(2) NOT NULL,
    estado_nombre VARCHAR(100) NOT NULL,
    municipio_codigo VARCHAR(3) NOT NULL,
    municipio_nombre VARCHAR(255) NOT NULL,
    loc_codigo VARCHAR(10) NOT NULL,
    localidad_nombre VARCHAR(255) NOT NULL,
    anio INT NOT NULL,
    poblacion INT NOT NULL DEFAULT 0,
    hombres INT NOT NULL DEFAULT 0,
    mujeres INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(estado_codigo, municipio_codigo, loc_codigo, anio)
);
CREATE INDEX IF NOT EXISTS idx_crecimiento_historico_localidad_lookup ON crecimiento_historico_localidad (estado_codigo, municipio_codigo, loc_codigo);

-- Crecimiento histórico por municipio (2005, 2010, 2020 - INEGI ITER LOC=0)
CREATE TABLE IF NOT EXISTS crecimiento_historico_municipal (
    id SERIAL PRIMARY KEY,
    estado_codigo VARCHAR(2) NOT NULL,
    estado_nombre VARCHAR(100) NOT NULL,
    municipio_codigo VARCHAR(3) NOT NULL,
    municipio_nombre VARCHAR(255) NOT NULL,
    anio INT NOT NULL,
    poblacion INT NOT NULL DEFAULT 0,
    hombres INT NOT NULL DEFAULT 0,
    mujeres INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(estado_codigo, municipio_codigo, anio)
);
CREATE INDEX IF NOT EXISTS idx_crecimiento_historico_municipal_lookup ON crecimiento_historico_municipal (estado_codigo, municipio_codigo);

-- Ciudades fijas (menú Ciudades): slug, nombre, estado, municipio. Si es_entidad_completa=1 se agrega todo el estado (ej. CDMX).
CREATE TABLE IF NOT EXISTS ciudades (
    id SERIAL PRIMARY KEY,
    slug VARCHAR(50) NOT NULL UNIQUE,
    nombre VARCHAR(100) NOT NULL,
    estado_codigo VARCHAR(2) NOT NULL,
    estado_nombre VARCHAR(100) NOT NULL,
    municipio_codigo VARCHAR(3),
    municipio_nombre VARCHAR(255),
    es_entidad_completa BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_ciudades_slug ON ciudades (slug);
-- Migración: tablas pib_estatal y estado_info_general
-- Para análisis geo-económico
-- Aplicar con: psql -h localhost -U postgres -d dash_db -f etl/migrations/002_geo_economico.sql

CREATE TABLE IF NOT EXISTS estado_info_general (
    estado VARCHAR(100) PRIMARY KEY,
    poblacion BIGINT,
    extension_km2 BIGINT
);

CREATE TABLE IF NOT EXISTS pib_estatal (
    id SERIAL PRIMARY KEY,
    estado VARCHAR(100),
    anio INT,
    pib_actual DECIMAL(20, 2),
    pib_anterior DECIMAL(20, 2),
    variacion_pct DECIMAL(10, 4),
    UNIQUE(estado, anio),
    FOREIGN KEY(estado) REFERENCES estado_info_general(estado) ON DELETE CASCADE
);
