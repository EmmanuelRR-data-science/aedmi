-- Migración: tabla ied_flujo_entidad para IED flujo por entidad (últimos 4 trimestres)
CREATE TABLE IF NOT EXISTS ied_flujo_entidad (
    id SERIAL PRIMARY KEY,
    entidad VARCHAR(100),
    mdd_4t DECIMAL(20, 4),
    rank INT,
    periodo VARCHAR(20)
);
