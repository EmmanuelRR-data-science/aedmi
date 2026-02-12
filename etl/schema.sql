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
