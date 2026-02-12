-- Migración: tabla ranking_turismo_wb para Ranking Turismo Mundial
CREATE TABLE IF NOT EXISTS ranking_turismo_wb (
    id SERIAL PRIMARY KEY,
    iso VARCHAR(10),
    country VARCHAR(120),
    year INT,
    val DECIMAL(20, 2)
);
