-- Migración: tabla anuncios_inversion_base (DataMéxico)
CREATE TABLE IF NOT EXISTS anuncios_inversion_base (
    id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    country VARCHAR(200),
    state VARCHAR(100),
    ia_sector VARCHAR(200),
    monto_inversion DECIMAL(24, 2)
);
