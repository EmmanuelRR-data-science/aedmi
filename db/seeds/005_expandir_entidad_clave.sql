-- Permite claves geográficas más largas (estado + municipio + métrica).
ALTER TABLE IF EXISTS anual.datos ALTER COLUMN entidad_clave TYPE VARCHAR(200);
ALTER TABLE IF EXISTS mensual.datos ALTER COLUMN entidad_clave TYPE VARCHAR(200);
ALTER TABLE IF EXISTS diario.datos ALTER COLUMN entidad_clave TYPE VARCHAR(200);
ALTER TABLE IF EXISTS quinquenal.datos ALTER COLUMN entidad_clave TYPE VARCHAR(200);
ALTER TABLE IF EXISTS public.analisis ALTER COLUMN entidad_clave TYPE VARCHAR(200);
