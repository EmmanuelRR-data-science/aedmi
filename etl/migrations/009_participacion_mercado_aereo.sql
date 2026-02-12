-- Migración: tablas para Participación Mercado Aéreo (AFAC/DataTur)
-- Nacional: Aerolinea, Participacion (porcentaje decimal)
-- Internacional: Region, Pasajeros
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
