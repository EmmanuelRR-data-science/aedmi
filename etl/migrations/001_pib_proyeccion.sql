-- Migración: tabla pib_proyeccion_fmi
-- Ejecutar si la BD ya existía antes de agregar esta tabla:
--   psql -h localhost -U postgres -d dash_db -f etl/migrations/001_pib_proyeccion.sql

CREATE TABLE IF NOT EXISTS pib_proyeccion_fmi (
    anio INT PRIMARY KEY,
    pib_total_mxn_billones DECIMAL(20, 4),
    pib_total_usd_billones DECIMAL(20, 4),
    pib_per_capita_mxn DECIMAL(20, 4),
    pib_per_capita_usd DECIMAL(20, 4)
);
