-- Producto Aeropuertos Nacional (Excel producto-aeropuertos 2006-2025). ETL carga; API lee.
CREATE TABLE IF NOT EXISTS producto_aeropuertos_nacional (
    id SERIAL PRIMARY KEY,
    anio INT NOT NULL,
    aeropuerto VARCHAR(250) NOT NULL,
    operaciones BIGINT NOT NULL,
    UNIQUE(anio, aeropuerto)
);
CREATE INDEX IF NOT EXISTS idx_producto_aeropuertos_nacional_anio ON producto_aeropuertos_nacional (anio);
