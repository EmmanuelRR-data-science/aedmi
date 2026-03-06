CREATE TABLE IF NOT EXISTS poblacion_ocupada_turismo_merida (
    id SERIAL PRIMARY KEY,
    anio INTEGER NOT NULL,
    trimestre INTEGER NOT NULL,
    poblacion_ocupada INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(anio, trimestre)
);

CREATE INDEX IF NOT EXISTS idx_poblacion_ocupada_turismo_lookup ON poblacion_ocupada_turismo_merida (anio, trimestre);
