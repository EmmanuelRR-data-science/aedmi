-- Script para restaurar tablas y datos de Monterrey localmente
-- (Evita el error 'relation "table" does not exist')

-- 1. IED por Estado y Sector
CREATE TABLE IF NOT EXISTS ied_historico_entidad_sector (
    id SERIAL PRIMARY KEY,
    estado_nombre VARCHAR(100),
    anio INT,
    sector VARCHAR(255),
    monto_mdd DECIMAL(20, 4)
);

-- 2. Ventas Internacionales (Exportaciones/Importaciones)
CREATE TABLE IF NOT EXISTS ventas_internacionales_municipal (
    id SERIAL PRIMARY KEY,
    estado_codigo VARCHAR(2),
    municipio_codigo VARCHAR(3),
    anio INT,
    mes INT,
    flujo VARCHAR(100), -- 'Exportaciones' o 'Importaciones'
    valor_usd DECIMAL(20, 4)
);

-- 3. Llegada de Pasajeros Aeropuerto
CREATE TABLE IF NOT EXISTS llegada_pasajeros_aeropuerto (
    id SERIAL PRIMARY KEY,
    ciudad_slug VARCHAR(50),
    anio INT,
    pasajeros_nacionales BIGINT,
    pasajeros_internacionales BIGINT,
    pasajeros_total BIGINT
);

-- 4. Visitantes Nacionales y Extranjeros
CREATE TABLE IF NOT EXISTS visitantes_nacionales_extranjeros (
    id SERIAL PRIMARY KEY,
    ciudad_slug VARCHAR(50),
    anio INT,
    visitantes_nacionales BIGINT,
    visitantes_extranjeros BIGINT,
    pct_nacionales DECIMAL(10, 4),
    pct_extranjeros DECIMAL(10, 4)
);

-- DATA SEED PARA MONTERREY (MOCK DATA PARA VERIFICACIÓN VISUAL)

-- IED Monterrey (Nuevo León)
INSERT INTO ied_historico_entidad_sector (estado_nombre, anio, sector, monto_mdd) VALUES
('Nuevo León', 2021, 'Industria Manufacturera', 1200.5),
('Nuevo León', 2021, 'Servicios Financieros', 300.2),
('Nuevo León', 2021, 'Transporte', 150.8),
('Nuevo León', 2021, 'Comercio', 100.4),
('Nuevo León', 2021, 'Otros', 50.1),
('Nuevo León', 2022, 'Industria Manufacturera', 1350.5),
('Nuevo León', 2022, 'Servicios Financieros', 320.2),
('Nuevo León', 2022, 'Transporte', 180.8),
('Nuevo León', 2022, 'Comercio', 120.4),
('Nuevo León', 2022, 'Otros', 60.1),
('Nuevo León', 2023, 'Industria Manufacturera', 1500.5),
('Nuevo León', 2023, 'Servicios Financieros', 350.2),
('Nuevo León', 2023, 'Transporte', 200.8),
('Nuevo León', 2023, 'Comercio', 140.4),
('Nuevo León', 2023, 'Otros', 70.1);

-- Ventas Internacionales Monterrey (19, 039)
INSERT INTO ventas_internacionales_municipal (estado_codigo, municipio_codigo, anio, mes, flujo, valor_usd) VALUES
('19', '039', 2023, 1, 'Exportaciones', 500000),
('19', '039', 2023, 1, 'Importaciones', 450000),
('19', '039', 2023, 2, 'Exportaciones', 520000),
('19', '039', 2023, 2, 'Importaciones', 470000),
('19', '039', 2023, 3, 'Exportaciones', 600000),
('19', '039', 2023, 3, 'Importaciones', 500000),
('19', '039', 2024, 1, 'Exportaciones', 550000),
('19', '039', 2024, 1, 'Importaciones', 480000),
('19', '039', 2024, 2, 'Exportaciones', 580000),
('19', '039', 2024, 2, 'Importaciones', 490000);

-- Pasajeros Monterrey
INSERT INTO llegada_pasajeros_aeropuerto (ciudad_slug, anio, pasajeros_nacionales, pasajeros_internacionales, pasajeros_total) VALUES
('monterrey', 2021, 5000000, 1000000, 6000000),
('monterrey', 2022, 6000000, 1500000, 7500000),
('monterrey', 2023, 7500000, 2000000, 9500000);

-- Visitantes Monterrey
INSERT INTO visitantes_nacionales_extranjeros (ciudad_slug, anio, visitantes_nacionales, visitantes_extranjeros, pct_nacionales, pct_extranjeros) VALUES
('monterrey', 2021, 1500000, 200000, 0.88, 0.12),
('monterrey', 2022, 1800000, 300000, 0.86, 0.14),
('monterrey', 2023, 2100000, 450000, 0.82, 0.18);
