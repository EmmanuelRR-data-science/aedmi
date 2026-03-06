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
