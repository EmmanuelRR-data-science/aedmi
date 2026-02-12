-- Migración: tabla ied_paises para IED por País de Origen
CREATE TABLE IF NOT EXISTS ied_paises (
    id SERIAL PRIMARY KEY,
    pais VARCHAR(120),
    monto_mdd DECIMAL(20, 4),
    periodo VARCHAR(20)
);
