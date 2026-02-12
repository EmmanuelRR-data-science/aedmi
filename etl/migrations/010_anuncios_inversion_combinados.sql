-- Migración: tabla anuncios_inversion_combinados (DataMéxico)
CREATE TABLE IF NOT EXISTS anuncios_inversion_combinados (
    id SERIAL PRIMARY KEY,
    anio INT NOT NULL,
    num_anuncios INT DEFAULT 0,
    monto_inversion DECIMAL(24, 2),
    state VARCHAR(100)
);
