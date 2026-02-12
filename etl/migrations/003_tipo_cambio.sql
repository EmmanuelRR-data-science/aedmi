-- Migración: tablas tipo de cambio (pq-estudios-mercado-vps)
-- Ejecutar si la BD ya existía: psql -h localhost -U postgres -d dash_db -f etl/migrations/003_tipo_cambio.sql

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
