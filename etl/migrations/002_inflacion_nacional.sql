-- Migración: tabla inflacion_nacional
-- Ejecutar si la BD ya existía antes de agregar esta tabla:
--   psql -h localhost -U postgres -d dash_db -f etl/migrations/002_inflacion_nacional.sql

CREATE TABLE IF NOT EXISTS inflacion_nacional (
    anio INT,
    mes INT,
    inflacion DECIMAL(10, 4),
    texto_fecha VARCHAR(50),
    PRIMARY KEY (anio, mes)
);
