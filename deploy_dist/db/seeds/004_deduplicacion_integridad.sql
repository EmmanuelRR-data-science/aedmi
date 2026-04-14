-- Deduplicación e integridad para evitar repetidos cuando entidad_clave es NULL.
-- PostgreSQL considera NULL != NULL en constraints UNIQUE, por eso se generaban
-- repetidos al recargar ETL de indicadores nacionales sin entidad_clave.

-- 1) Limpiar duplicados existentes conservando el registro más reciente (id mayor)
WITH d AS (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY indicador_id, nivel_geografico, COALESCE(entidad_clave, ''), periodo
      ORDER BY id DESC
    ) AS rn
  FROM anual.datos
)
DELETE FROM anual.datos t
USING d
WHERE t.ctid = d.ctid AND d.rn > 1;

WITH d AS (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY indicador_id, nivel_geografico, COALESCE(entidad_clave, ''), anio, mes
      ORDER BY id DESC
    ) AS rn
  FROM mensual.datos
)
DELETE FROM mensual.datos t
USING d
WHERE t.ctid = d.ctid AND d.rn > 1;

WITH d AS (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY indicador_id, nivel_geografico, COALESCE(entidad_clave, ''), fecha
      ORDER BY id DESC
    ) AS rn
  FROM diario.datos
)
DELETE FROM diario.datos t
USING d
WHERE t.ctid = d.ctid AND d.rn > 1;

WITH d AS (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY indicador_id, nivel_geografico, COALESCE(entidad_clave, ''), periodo
      ORDER BY id DESC
    ) AS rn
  FROM quinquenal.datos
)
DELETE FROM quinquenal.datos t
USING d
WHERE t.ctid = d.ctid AND d.rn > 1;

-- 2) Índices únicos robustos (con COALESCE) para bloquear repetidos futuros
CREATE UNIQUE INDEX IF NOT EXISTS uq_anual_datos_periodo_entidad_norm
ON anual.datos (indicador_id, nivel_geografico, COALESCE(entidad_clave, ''), periodo);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mensual_datos_periodo_entidad_norm
ON mensual.datos (indicador_id, nivel_geografico, COALESCE(entidad_clave, ''), anio, mes);

CREATE UNIQUE INDEX IF NOT EXISTS uq_diario_datos_fecha_entidad_norm
ON diario.datos (indicador_id, nivel_geografico, COALESCE(entidad_clave, ''), fecha);

CREATE UNIQUE INDEX IF NOT EXISTS uq_quinquenal_datos_periodo_entidad_norm
ON quinquenal.datos (indicador_id, nivel_geografico, COALESCE(entidad_clave, ''), periodo);
