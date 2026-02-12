-- Migración: tabla balanza_visitantes_inegi para Balanza de Visitantes (INEGI BISE)
-- Replica proc_tourism_market process_aereo_y_balanza
CREATE TABLE IF NOT EXISTS balanza_visitantes_inegi (
    id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    entradas DECIMAL(20, 2),
    salidas DECIMAL(20, 2),
    balance DECIMAL(20, 2)
);
