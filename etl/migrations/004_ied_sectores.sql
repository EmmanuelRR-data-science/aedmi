-- Migración: tabla ied_sectores para IED por Sector Económico
CREATE TABLE IF NOT EXISTS ied_sectores (
    id SERIAL PRIMARY KEY,
    sector VARCHAR(100),
    monto_mdd DECIMAL(20, 4),
    periodo VARCHAR(20)
);
