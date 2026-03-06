-- Balanza Comercial por Producto (Economía/DataMéxico - inegi_foreign_trade_product). ETL carga; API lee.
CREATE TABLE IF NOT EXISTS balanza_comercial_producto (
    id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    flow_id INT NOT NULL,
    product VARCHAR(200) NOT NULL,
    trade_value DECIMAL(24, 2) NOT NULL,
    UNIQUE(year, flow_id, product)
);
CREATE INDEX IF NOT EXISTS idx_bcp_year ON balanza_comercial_producto (year);
CREATE INDEX IF NOT EXISTS idx_bcp_flow ON balanza_comercial_producto (flow_id);
