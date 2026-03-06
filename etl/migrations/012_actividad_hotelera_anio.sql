-- Añadir año a actividad hotelera estatal para filtrar por año en la gráfica.
ALTER TABLE actividad_hotelera_estatal ADD COLUMN IF NOT EXISTS anio INT;

-- Datos existentes sin año: asumir año más reciente (2024)
UPDATE actividad_hotelera_estatal SET anio = 2024 WHERE anio IS NULL;

ALTER TABLE actividad_hotelera_estatal ALTER COLUMN anio SET NOT NULL;
ALTER TABLE actividad_hotelera_estatal ALTER COLUMN anio SET DEFAULT 2024;

-- Recrear PK para incluir anio
ALTER TABLE actividad_hotelera_estatal DROP CONSTRAINT IF EXISTS actividad_hotelera_estatal_pkey;
ALTER TABLE actividad_hotelera_estatal ADD PRIMARY KEY (estado_codigo, anio, mes_num);

CREATE INDEX IF NOT EXISTS idx_actividad_hotelera_estatal_anio ON actividad_hotelera_estatal (estado_codigo, anio);
