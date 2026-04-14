-- ============================================================
-- AEDMI — DDL inicial de la base de datos
-- Schemas organizados por periodicidad de actualización
-- ============================================================

-- Schemas por periodicidad
CREATE SCHEMA IF NOT EXISTS anual;
CREATE SCHEMA IF NOT EXISTS mensual;
CREATE SCHEMA IF NOT EXISTS diario;
CREATE SCHEMA IF NOT EXISTS quinquenal;

-- ============================================================
-- Schema PUBLIC — Metadatos y catálogos
-- ============================================================

-- Fuentes de datos registradas
CREATE TABLE IF NOT EXISTS public.fuentes_datos (
    id              SERIAL PRIMARY KEY,
    nombre          VARCHAR(200) NOT NULL,
    url_referencia  TEXT,
    periodicidad    VARCHAR(20) NOT NULL
                    CHECK (periodicidad IN ('diario', 'semanal', 'mensual', 'anual', 'quinquenal', 'otra')),
    ultima_carga    TIMESTAMPTZ,
    modulo_etl      VARCHAR(100) NOT NULL,
    estado          VARCHAR(20) NOT NULL DEFAULT 'pendiente'
                    CHECK (estado IN ('pendiente', 'etl_listo', 'api_lista', 'grafica_lista', 'completo')),
    activo          BOOLEAN NOT NULL DEFAULT TRUE,
    notas           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Catálogo de indicadores
CREATE TABLE IF NOT EXISTS public.indicadores (
    id               SERIAL PRIMARY KEY,
    clave            VARCHAR(100) UNIQUE NOT NULL,
    nombre           VARCHAR(300) NOT NULL,
    categoria        VARCHAR(50) NOT NULL
                     CHECK (categoria IN ('demografia', 'economia', 'turismo', 'conectividad_aerea')),
    nivel_geografico VARCHAR(30) NOT NULL
                     CHECK (nivel_geografico IN ('nacional', 'estatal', 'municipal', 'localidad', 'ciudad')),
    unidad           VARCHAR(100),
    fuente_id        INTEGER REFERENCES public.fuentes_datos(id),
    descripcion      TEXT,
    tipo_grafica     VARCHAR(30) DEFAULT 'bar'
                     CHECK (tipo_grafica IN ('bar', 'line', 'pie', 'area', 'scatter')),
    activo           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Análisis IA y revisados por gráfica
CREATE TABLE IF NOT EXISTS public.analisis (
    id                  SERIAL PRIMARY KEY,
    indicador_id        INTEGER NOT NULL REFERENCES public.indicadores(id),
    nivel_geografico    VARCHAR(30) NOT NULL,
    entidad_clave       VARCHAR(50),
    analisis_ia         TEXT,
    analisis_revisado   TEXT,
    ia_generado_at      TIMESTAMPTZ,
    revisado_at         TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (indicador_id, nivel_geografico, entidad_clave)
);

-- Log de ejecuciones ETL
CREATE TABLE IF NOT EXISTS public.etl_logs (
    id                  SERIAL PRIMARY KEY,
    fuente_id           INTEGER REFERENCES public.fuentes_datos(id),
    tipo_ejecucion      VARCHAR(20) NOT NULL
                        CHECK (tipo_ejecucion IN ('programada', 'manual')),
    inicio              TIMESTAMPTZ NOT NULL,
    fin                 TIMESTAMPTZ,
    exitoso             BOOLEAN,
    registros_cargados  INTEGER NOT NULL DEFAULT 0,
    errores             INTEGER NOT NULL DEFAULT 0,
    mensaje             TEXT,
    usuario             VARCHAR(100),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índices en tablas de metadatos
CREATE INDEX IF NOT EXISTS idx_indicadores_categoria ON public.indicadores(categoria);
CREATE INDEX IF NOT EXISTS idx_indicadores_nivel ON public.indicadores(nivel_geografico);
CREATE INDEX IF NOT EXISTS idx_indicadores_fuente ON public.indicadores(fuente_id);
CREATE INDEX IF NOT EXISTS idx_analisis_indicador ON public.analisis(indicador_id);
CREATE INDEX IF NOT EXISTS idx_etl_logs_fuente ON public.etl_logs(fuente_id);
CREATE INDEX IF NOT EXISTS idx_etl_logs_inicio ON public.etl_logs(inicio DESC);

-- ============================================================
-- Schema ANUAL — Indicadores de actualización anual
-- ============================================================
CREATE TABLE IF NOT EXISTS anual.datos (
    id               BIGSERIAL PRIMARY KEY,
    indicador_id     INTEGER NOT NULL REFERENCES public.indicadores(id),
    nivel_geografico VARCHAR(30) NOT NULL,
    entidad_clave    VARCHAR(200),
    valor            NUMERIC(20, 4),
    unidad           VARCHAR(100),
    periodo          INTEGER NOT NULL,
    cargado_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (indicador_id, nivel_geografico, entidad_clave, periodo)
);

CREATE INDEX IF NOT EXISTS idx_anual_indicador ON anual.datos(indicador_id);
CREATE INDEX IF NOT EXISTS idx_anual_nivel ON anual.datos(nivel_geografico);
CREATE INDEX IF NOT EXISTS idx_anual_periodo ON anual.datos(periodo);
CREATE INDEX IF NOT EXISTS idx_anual_entidad ON anual.datos(entidad_clave);
CREATE UNIQUE INDEX IF NOT EXISTS uq_anual_datos_periodo_entidad_norm
ON anual.datos (indicador_id, nivel_geografico, COALESCE(entidad_clave, ''), periodo);

-- ============================================================
-- Schema MENSUAL — Indicadores de actualización mensual
-- ============================================================
CREATE TABLE IF NOT EXISTS mensual.datos (
    id               BIGSERIAL PRIMARY KEY,
    indicador_id     INTEGER NOT NULL REFERENCES public.indicadores(id),
    nivel_geografico VARCHAR(30) NOT NULL,
    entidad_clave    VARCHAR(200),
    valor            NUMERIC(20, 4),
    unidad           VARCHAR(100),
    anio             INTEGER NOT NULL,
    mes              SMALLINT NOT NULL CHECK (mes BETWEEN 1 AND 12),
    cargado_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (indicador_id, nivel_geografico, entidad_clave, anio, mes)
);

CREATE INDEX IF NOT EXISTS idx_mensual_indicador ON mensual.datos(indicador_id);
CREATE INDEX IF NOT EXISTS idx_mensual_nivel ON mensual.datos(nivel_geografico);
CREATE INDEX IF NOT EXISTS idx_mensual_periodo ON mensual.datos(anio, mes);
CREATE INDEX IF NOT EXISTS idx_mensual_entidad ON mensual.datos(entidad_clave);
CREATE UNIQUE INDEX IF NOT EXISTS uq_mensual_datos_periodo_entidad_norm
ON mensual.datos (indicador_id, nivel_geografico, COALESCE(entidad_clave, ''), anio, mes);

-- ============================================================
-- Schema DIARIO — Indicadores de actualización diaria
-- ============================================================
CREATE TABLE IF NOT EXISTS diario.datos (
    id               BIGSERIAL PRIMARY KEY,
    indicador_id     INTEGER NOT NULL REFERENCES public.indicadores(id),
    nivel_geografico VARCHAR(30) NOT NULL,
    entidad_clave    VARCHAR(200),
    valor            NUMERIC(20, 4),
    unidad           VARCHAR(100),
    fecha            DATE NOT NULL,
    cargado_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (indicador_id, nivel_geografico, entidad_clave, fecha)
);

CREATE INDEX IF NOT EXISTS idx_diario_indicador ON diario.datos(indicador_id);
CREATE INDEX IF NOT EXISTS idx_diario_nivel ON diario.datos(nivel_geografico);
CREATE INDEX IF NOT EXISTS idx_diario_fecha ON diario.datos(fecha DESC);
CREATE INDEX IF NOT EXISTS idx_diario_entidad ON diario.datos(entidad_clave);
CREATE UNIQUE INDEX IF NOT EXISTS uq_diario_datos_fecha_entidad_norm
ON diario.datos (indicador_id, nivel_geografico, COALESCE(entidad_clave, ''), fecha);

-- ============================================================
-- Schema QUINQUENAL — Indicadores de actualización quinquenal
-- ============================================================
CREATE TABLE IF NOT EXISTS quinquenal.datos (
    id               BIGSERIAL PRIMARY KEY,
    indicador_id     INTEGER NOT NULL REFERENCES public.indicadores(id),
    nivel_geografico VARCHAR(30) NOT NULL,
    entidad_clave    VARCHAR(200),
    valor            NUMERIC(20, 4),
    unidad           VARCHAR(100),
    periodo          INTEGER NOT NULL,
    cargado_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (indicador_id, nivel_geografico, entidad_clave, periodo)
);

CREATE INDEX IF NOT EXISTS idx_quinquenal_indicador ON quinquenal.datos(indicador_id);
CREATE INDEX IF NOT EXISTS idx_quinquenal_nivel ON quinquenal.datos(nivel_geografico);
CREATE INDEX IF NOT EXISTS idx_quinquenal_periodo ON quinquenal.datos(periodo);
CREATE INDEX IF NOT EXISTS idx_quinquenal_entidad ON quinquenal.datos(entidad_clave);
CREATE UNIQUE INDEX IF NOT EXISTS uq_quinquenal_datos_periodo_entidad_norm
ON quinquenal.datos (indicador_id, nivel_geografico, COALESCE(entidad_clave, ''), periodo);

-- ============================================================
-- Seed inicial: KPIs Nacionales — Banxico
-- ============================================================

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES
    ('INEGI BIE — PIB Trimestral a precios corrientes (MXN)', 'https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR/735879/es/00/false/BIE-BISE/2.0/', 'mensual', 'sources.inegi.pib_trimestral', 'etl_listo'),
    ('Banxico — Tipo de Cambio USD/MXN (FIX)', 'https://www.banxico.org.mx/SieAPIRest/service/v1/series/SF43718/datos', 'diario', 'sources.banxico.tipo_cambio', 'etl_listo'),
    ('Banxico — Inflación INPC (variación anual %)', 'https://www.banxico.org.mx/SieAPIRest/service/v1/series/SP30578/datos', 'mensual', 'sources.banxico.inflacion', 'etl_listo')
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT 'banxico.pib_trimestral_mxn', 'PIB Nacional (MXN)', 'economia', 'nacional', 'Millones de pesos',
    id, 'PIB trimestral a precios corrientes en pesos mexicanos. Banxico SIE SR16734.', 'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.banxico.pib_trimestral'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT 'banxico.pib_trimestral_usd', 'PIB Nacional (USD)', 'economia', 'nacional', 'Millones de dólares',
    id, 'PIB trimestral calculado: PIB MXN / Tipo de cambio FIX promedio. Banxico SIE.', 'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.banxico.pib_trimestral'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT 'banxico.tipo_cambio_usd_mxn', 'Tipo de Cambio USD/MXN', 'economia', 'nacional', 'Pesos por dólar',
    id, 'Tipo de cambio FIX publicado por Banxico. Serie SF43718.', 'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.banxico.tipo_cambio'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT 'banxico.inflacion_inpc_anual', 'Inflación INPC (variación anual)', 'economia', 'nacional', '%',
    id, 'Variación porcentual anual del INPC. Banxico SIE SP1.', 'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.banxico.inflacion'
ON CONFLICT (clave) DO NOTHING;

-- Seed: Población total nacional (INEGI BISE)
INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'INEGI BISE — Población total nacional (censos y conteos)',
    'https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR/1002000001/es/00/false/BISE/2.0/',
    'quinquenal',
    'sources.inegi.poblacion_nacional',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'inegi.poblacion_total_nacional',
    'Población Total Nacional',
    'demografia',
    'nacional',
    'Personas',
    id,
    'Población total de México según censos y conteos de población y vivienda. INEGI BISE serie 1002000001.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.inegi.poblacion_nacional'
ON CONFLICT (clave) DO NOTHING;

-- Seed: Grupos de edad nacional (INEGI Censos)
INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES ('INEGI — Censos y Conteos de Población (grupos de edad)',
    'https://www.inegi.org.mx/programas/ccpv/2020/',
    'quinquenal', 'sources.inegi.grupos_edad_nacional', 'etl_listo')
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT 'inegi.grupos_edad_nacional', 'Distribución de la Población por Grupos de Edad',
    'demografia', 'nacional', 'Personas', id,
    'Distribución de la población nacional en grupos de edad: 0-14, 15-64 y 65+ años. INEGI Censos de Población y Vivienda.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.inegi.grupos_edad_nacional'
ON CONFLICT (clave) DO NOTHING;

-- Seed: Población por sexo nacional (INEGI BISE)
INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES ('INEGI BISE — Población total por sexo (series 1002000002/1002000003)',
    'https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR/1002000002,1002000003/es/00/false/BISE/2.0/',
    'quinquenal', 'sources.inegi.poblacion_sexo_nacional', 'etl_listo')
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT 'inegi.poblacion_sexo_nacional', 'Distribución de la Población por Sexo',
    'demografia', 'nacional', 'Personas', id,
    'Distribución de la población nacional entre hombres y mujeres. INEGI BISE series 1002000002 y 1002000003.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.inegi.poblacion_sexo_nacional'
ON CONFLICT (clave) DO NOTHING;

-- Seed: Llegada de turistas por estado (Datatur/SECTUR)
INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Datatur/SECTUR — Llegada de turistas por entidad federativa',
    'https://www.datatur.sectur.gob.mx/SitePages/Visitantes.aspx',
    'anual',
    'sources.sectur.llegada_turistas_estatal',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.llegada_turistas_estatal',
    'Llegada de Turistas por Estado',
    'turismo',
    'estatal',
    'Personas',
    id,
    'Total de turistas por año para cada estado de México (últimos 5 años). Fuente: Datatur / SECTUR.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.llegada_turistas_estatal'
ON CONFLICT (clave) DO NOTHING;

-- Seed: Exportaciones por estado (DataMéxico / SE)
INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'SE/DataMéxico — Exportaciones por estado',
    'https://www.economia.gob.mx/datamexico/',
    'anual',
    'sources.se.exportaciones_estatal',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'se.exportaciones_estatal',
    'Exportaciones por Estado',
    'economia',
    'estatal',
    'Millones USD',
    id,
    'Evolución anual de exportaciones por entidad federativa, incluyendo participación nacional y ranking.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.se.exportaciones_estatal'
ON CONFLICT (clave) DO NOTHING;

-- Seed: Aeropuertos por estado (DGAC / AFAC)
INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'DGAC/AFAC — Aeropuertos por estado',
    'https://www.gob.mx/afac',
    'anual',
    'sources.afac.aeropuertos_estatal',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'afac.aeropuertos_estatal',
    'Aeropuertos por Estado',
    'conectividad_aerea',
    'estatal',
    'Operaciones',
    id,
    'Operaciones anuales por aeropuerto dentro de cada estado, incluyendo ranking de composición.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.afac.aeropuertos_estatal'
ON CONFLICT (clave) DO NOTHING;

-- Seed: Población municipal por sexo (CONAPO)
INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'CONAPO — Población municipal por sexo',
    'https://www.gob.mx/conapo',
    'anual',
    'sources.conapo.municipios_poblacion',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'conapo.municipios_poblacion',
    'Población municipal por sexo',
    'demografia',
    'municipal',
    'Personas',
    id,
    'Población municipal total, hombres y mujeres para años censales y proyección reciente.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.conapo.municipios_poblacion'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'conapo.municipios_piramide_edad',
    'Distribución de la población municipal',
    'demografia',
    'municipal',
    'Personas',
    id,
    'Distribución de población municipal por grupos de edad y sexo.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.conapo.municipios_poblacion'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'CONAPO — Pirámide municipal por edad y sexo',
    'https://www.gob.mx/conapo',
    'anual',
    'sources.conapo.municipios_piramide_edad',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

UPDATE public.indicadores
SET fuente_id = (
    SELECT id FROM public.fuentes_datos WHERE modulo_etl = 'sources.conapo.municipios_piramide_edad' LIMIT 1
)
WHERE clave = 'conapo.municipios_piramide_edad';

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'CONAPO — Proyección poblacional municipal',
    'https://www.gob.mx/conapo',
    'anual',
    'sources.conapo.municipios_proyeccion',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'conapo.municipios_proyeccion',
    'Proyección poblacional municipal',
    'demografia',
    'municipal',
    'Personas',
    id,
    'Proyecciones a 5 años de población total, hombres y mujeres para el municipio seleccionado.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.conapo.municipios_proyeccion'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'CONAPO — Población por localidad',
    'https://www.gob.mx/conapo',
    'anual',
    'sources.conapo.localidades_poblacion',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'conapo.localidades_poblacion',
    'Población por localidad',
    'demografia',
    'localidad',
    'Personas',
    id,
    'Población de localidades (total, mujeres y hombres) para la localidad seleccionada.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.conapo.localidades_poblacion'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'CONAPO — Pirámide de población por localidad',
    'https://www.gob.mx/conapo',
    'anual',
    'sources.conapo.localidades_piramide_edad',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'conapo.localidades_piramide_edad',
    'Distribución de la población por localidad',
    'demografia',
    'localidad',
    'Personas',
    id,
    'Distribución por grupos de edad y sexo para la localidad seleccionada.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.conapo.localidades_piramide_edad'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Observatur Yucatán / ENOE-INEGI — Ocupación en restaurantes y hoteles (Mérida)',
    'https://www.observaturyucatan.org.mx/indicadores',
    'anual',
    'sources.inegi.ocupacion_restaurantes_hoteles_yucatan',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'inegi.ocupacion_restaurantes_hoteles_merida',
    'Población ocupada en restaurantes y hoteles (Mérida)',
    'economia',
    'ciudad',
    'Personas',
    id,
    'Población ocupada en servicios de alojamiento y restaurantes para Mérida, Yucatán.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.inegi.ocupacion_restaurantes_hoteles_yucatan'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'DataMéxico — Comercio internacional de Mérida',
    'https://www.economia.gob.mx/datamexico/es/profile/geo/merida-993101',
    'anual',
    'sources.se.comercio_internacional_merida',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'se.comercio_internacional_merida',
    'Comercio internacional de Mérida',
    'economia',
    'ciudad',
    'Millones USD',
    id,
    'Importaciones y exportaciones anuales de Mérida.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.se.comercio_internacional_merida'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'AFAC — Conectividad aérea de Mérida',
    'https://www.gob.mx/afac/acciones-y-programas/estadisticas-280404',
    'anual',
    'sources.afac.conectividad_aerea_merida',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'afac.conectividad_aerea_merida',
    'Conectividad aérea de Mérida',
    'conectividad_aerea',
    'ciudad',
    'Operaciones',
    id,
    'Total anual de operaciones aeroportuarias en Mérida.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.afac.conectividad_aerea_merida'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'datos.gob.mx/AFAC — Conexiones aéreas hacia Mérida',
    'https://www.datos.gob.mx/dataset/movimiento_operacional_aicm',
    'anual',
    'sources.afac.conexiones_aereas_merida',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'afac.conexiones_aereas_merida',
    'Conexiones aéreas hacia Mérida',
    'conectividad_aerea',
    'ciudad',
    'Vuelos',
    id,
    'Vuelos de llegada a Mérida por aeropuerto de origen.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.afac.conexiones_aereas_merida'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'datos.gob.mx — Oferta de servicios turísticos en Mérida',
    'https://www.datos.gob.mx/dataset/servicios_turisticos/resource/e1fcea3e-773f-4b2a-a692-c13edbe00b8f',
    'anual',
    'sources.sectur.oferta_servicios_turisticos_merida',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.oferta_servicios_turisticos_merida',
    'Oferta de servicios turísticos en Mérida',
    'turismo',
    'ciudad',
    'Mixto',
    id,
    'Servicios turísticos otorgados y ventas obtenidas por año.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.oferta_servicios_turisticos_merida'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Observatur Yucatán — Ocupación hotelera promedio en Mérida',
    'https://www.observaturyucatan.org.mx/indicadores',
    'anual',
    'sources.sectur.ocupacion_hotelera_promedio_merida',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.ocupacion_hotelera_promedio_merida',
    'Ocupación hotelera promedio en Mérida',
    'turismo',
    'ciudad',
    'Porcentaje',
    id,
    'Porcentaje promedio anual de habitaciones ocupadas en establecimientos de hospedaje.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.ocupacion_hotelera_promedio_merida'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Observatur Yucatán — Llegada de visitantes con pernocta en Mérida',
    'https://www.observaturyucatan.org.mx/indicadores',
    'anual',
    'sources.sectur.llegada_visitantes_pernocta_merida',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.llegada_visitantes_pernocta_merida',
    'Llegada de visitantes con pernocta en Mérida',
    'turismo',
    'ciudad',
    'Visitantes',
    id,
    'Visitantes con pernocta clasificados en nacionales e internacionales.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.llegada_visitantes_pernocta_merida'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Observatur Yucatán — Gasto promedio diario del visitante con pernocta en Mérida',
    'https://www.observaturyucatan.org.mx/indicadores',
    'anual',
    'sources.sectur.gasto_promedio_diario_pernocta_merida',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.gasto_promedio_diario_pernocta_merida',
    'Gasto promedio diario del visitante con pernocta en Mérida',
    'turismo',
    'ciudad',
    'MXN',
    id,
    'Gasto monetario promedio diario de visitantes con pernocta.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.gasto_promedio_diario_pernocta_merida'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Observatur Yucatán — Derrama económica estimada de visitantes con pernocta en Mérida',
    'https://www.observaturyucatan.org.mx/indicadores',
    'anual',
    'sources.sectur.derrama_economica_pernocta_merida',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.derrama_economica_pernocta_merida',
    'Derrama económica estimada de visitantes con pernocta en Mérida',
    'turismo',
    'ciudad',
    'Millones MXN',
    id,
    'Derrama económica anual estimada asociada a visitantes con pernocta.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.derrama_economica_pernocta_merida'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Observatur Yucatán — Ingreso hotelero en Mérida',
    'https://www.observaturyucatan.org.mx/indicadores',
    'anual',
    'sources.sectur.ingreso_hotelero_merida',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.ingreso_hotelero_merida',
    'Ingreso hotelero en Mérida',
    'turismo',
    'ciudad',
    'Millones MXN',
    id,
    'Ingreso hotelero anual estimado en establecimientos de hospedaje.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.ingreso_hotelero_merida'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Observatur Yucatán — Establecimientos de servicios turísticos en Yucatán',
    'https://www.observaturyucatan.org.mx/indicadores',
    'anual',
    'sources.sectur.establecimientos_servicios_turisticos_yucatan',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.establecimientos_servicios_turisticos_yucatan',
    'Establecimientos de servicios turísticos en Yucatán',
    'turismo',
    'ciudad',
    'Establecimientos',
    id,
    'Número anual estimado de establecimientos de servicios turísticos registrados en el estado.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.establecimientos_servicios_turisticos_yucatan'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'SE/datos.gob.mx — Comercio internacional Monterrey (proxy IED)',
    'https://www.datos.gob.mx/dataset/inversion_extranjera_directa',
    'anual',
    'sources.se.comercio_internacional_monterrey',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'se.comercio_internacional_monterrey',
    'Comercio internacional de Monterrey',
    'economia',
    'ciudad',
    'Millones USD',
    id,
    'Flujos anuales estimados de inversión/comercio internacional para Monterrey/Nuevo León.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.se.comercio_internacional_monterrey'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'SE/datos.gob.mx — Exportaciones e importaciones Monterrey',
    'https://www.datos.gob.mx/dataset/inversion_extranjera_directa',
    'anual',
    'sources.se.exportaciones_importaciones_monterrey',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'se.exportaciones_importaciones_monterrey',
    'Exportaciones e importaciones de Monterrey',
    'economia',
    'ciudad',
    'Millones USD',
    id,
    'Serie anual de exportaciones e importaciones para Monterrey/Nuevo León.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.se.exportaciones_importaciones_monterrey'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'OMA/AFAC — Llegadas de pasajeros Monterrey',
    'https://www.oma.aero/',
    'anual',
    'sources.afac.llegadas_pasajeros_monterrey',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'afac.llegadas_pasajeros_monterrey',
    'Llegadas de pasajeros de Monterrey',
    'conectividad_aerea',
    'ciudad',
    'Pasajeros',
    id,
    'Llegadas anuales de pasajeros nacionales e internacionales en Monterrey.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.afac.llegadas_pasajeros_monterrey'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'SECTUR/Datatur — Visitantes nacionales y extranjeros Monterrey',
    'https://datatur.sectur.gob.mx/',
    'anual',
    'sources.sectur.visitantes_nacionales_extranjeros_monterrey',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.visitantes_nacionales_extranjeros_monterrey',
    'Visitantes nacionales y extranjeros en Monterrey',
    'turismo',
    'ciudad',
    'Visitantes',
    id,
    'Visitantes anuales en Monterrey, desagregados en nacionales y extranjeros.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.visitantes_nacionales_extranjeros_monterrey'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'SECTUR/Datatur — Ocupación hotelera Monterrey',
    'https://datatur.sectur.gob.mx/',
    'anual',
    'sources.sectur.ocupacion_hotelera_monterrey',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.ocupacion_hotelera_monterrey',
    'Ocupación hotelera en Monterrey',
    'turismo',
    'ciudad',
    'Mixto',
    id,
    'Ocupación hotelera anual y cuartos disponibles/ocupados en Monterrey.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.ocupacion_hotelera_monterrey'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'INEGI — PEA Querétaro (ENOE 15 años y más)',
    'https://www.inegi.org.mx/programas/enoe/15ymas/#datos_abiertos',
    'anual',
    'sources.inegi.pea_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'inegi.pea_queretaro',
    'Población económicamente activa (Querétaro)',
    'economia',
    'ciudad',
    'Miles de personas',
    id,
    'PEA 15 años y más para la ZM/city Querétaro (serie referencial ENOE).',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.inegi.pea_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'INEGI — Tasa de participación laboral Querétaro (ENOE 15 años y más)',
    'https://www.inegi.org.mx/programas/enoe/15ymas/#tabulados',
    'anual',
    'sources.inegi.tasa_participacion_laboral_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'inegi.tasa_participacion_laboral_queretaro',
    'Tasa de participación laboral (Querétaro)',
    'economia',
    'ciudad',
    'Porcentaje',
    id,
    'Tasa de participación laboral de la población de 15 años y más para Querétaro (ENOE).',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.inegi.tasa_participacion_laboral_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'INEGI — Tasa de desocupación Querétaro (ENOE 15 años y más)',
    'https://www.inegi.org.mx/programas/enoe/15ymas/#tabulados',
    'anual',
    'sources.inegi.tasa_desocupacion_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'inegi.tasa_desocupacion_queretaro',
    'Tasa de desocupación (Querétaro)',
    'economia',
    'ciudad',
    'Porcentaje',
    id,
    'Tasa de desocupación de la PEA para Querétaro (ENOE).',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.inegi.tasa_desocupacion_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'INEGI — Composición sectorial del empleo Querétaro (ENOE 15 años y más)',
    'https://www.inegi.org.mx/programas/enoe/15ymas/#tabulados',
    'anual',
    'sources.inegi.composicion_sectorial_empleo_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'inegi.composicion_sectorial_empleo_queretaro',
    'Composición sectorial del empleo (Querétaro)',
    'economia',
    'ciudad',
    'Porcentaje',
    id,
    'Distribución porcentual del empleo en sectores primario, secundario y terciario para Querétaro (ENOE).',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.inegi.composicion_sectorial_empleo_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'INEGI — PIB estatal total Querétaro',
    'https://www.inegi.org.mx/app/tabulados/default.aspx?pr=17&vr=6&in=2&tp=20&wr=1&cno=2',
    'anual',
    'sources.inegi.pib_estatal_total_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'inegi.pib_estatal_total_queretaro',
    'PIB estatal total (Querétaro)',
    'economia',
    'ciudad',
    'Millones de pesos',
    id,
    'PIB estatal total anual de Querétaro a precios corrientes.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.inegi.pib_estatal_total_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'INEGI — PIB per cápita Querétaro',
    'https://www.inegi.org.mx/app/tabulados/default.aspx?pr=17&vr=6&in=2&tp=20&wr=1&cno=2',
    'anual',
    'sources.inegi.pib_per_capita_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'inegi.pib_per_capita_queretaro',
    'PIB per cápita (Querétaro)',
    'economia',
    'ciudad',
    'Pesos por habitante',
    id,
    'PIB per cápita anual de Querétaro a precios corrientes.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.inegi.pib_per_capita_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'INEGI — Distribución del PIB por sector Querétaro',
    'https://www.inegi.org.mx/temas/pib/#tabulados',
    'anual',
    'sources.inegi.pib_sector_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'inegi.pib_sector_queretaro',
    'Distribución del PIB por sector (Querétaro)',
    'economia',
    'ciudad',
    'Millones de pesos',
    id,
    'Distribución anual del PIB de Querétaro por sectores primario, secundario y terciario.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.inegi.pib_sector_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'INEGI — Tasa de crecimiento económico Querétaro',
    'https://www.inegi.org.mx/temas/pib/#tabulados',
    'anual',
    'sources.inegi.tasa_crecimiento_economico_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'inegi.tasa_crecimiento_economico_queretaro',
    'Tasa de crecimiento económico (Querétaro)',
    'economia',
    'ciudad',
    'Porcentaje',
    id,
    'Variación porcentual anual del PIB estatal de Querétaro.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.inegi.tasa_crecimiento_economico_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'SE/datos.gob.mx — Exportaciones estatales Querétaro',
    'https://www.datos.gob.mx/dataset/inversion_extranjera_directa',
    'anual',
    'sources.se.exportaciones_estatales_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'se.exportaciones_estatales_queretaro',
    'Exportaciones estatales (Querétaro)',
    'economia',
    'ciudad',
    'Millones USD',
    id,
    'Exportaciones estatales anuales de Querétaro.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.se.exportaciones_estatales_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'SE/datos.gob.mx — Importaciones estatales Querétaro',
    'https://www.datos.gob.mx/dataset/inversion_extranjera_directa',
    'anual',
    'sources.se.importaciones_estatales_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'se.importaciones_estatales_queretaro',
    'Importaciones estatales (Querétaro)',
    'economia',
    'ciudad',
    'Millones USD',
    id,
    'Importaciones estatales anuales de Querétaro.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.se.importaciones_estatales_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'SE/datos.gob.mx — Comercio internacional neto Querétaro',
    'https://www.datos.gob.mx/dataset/inversion_extranjera_directa',
    'anual',
    'sources.se.comercio_internacional_neto_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'se.comercio_internacional_neto_queretaro',
    'Comercio internacional neto (Querétaro)',
    'economia',
    'ciudad',
    'Millones USD',
    id,
    'Comercio internacional total anual de Querétaro (exportaciones + importaciones).',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.se.comercio_internacional_neto_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'SE/datos.gob.mx — Balance comercial neto Querétaro',
    'https://www.datos.gob.mx/dataset/inversion_extranjera_directa',
    'anual',
    'sources.se.balance_comercial_neto_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'se.balance_comercial_neto_queretaro',
    'Balance comercial neto (Querétaro)',
    'economia',
    'ciudad',
    'Millones USD',
    id,
    'Balance comercial anual de Querétaro (exportaciones - importaciones).',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.se.balance_comercial_neto_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'SE/datos.gob.mx — IED anual Querétaro',
    'https://www.datos.gob.mx/dataset/inversion_extranjera_directa',
    'anual',
    'sources.se.ied_anual_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'se.ied_anual_queretaro',
    'IED anual (Querétaro)',
    'economia',
    'ciudad',
    'Millones USD',
    id,
    'Inversión extranjera directa anual de Querétaro.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.se.ied_anual_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'SE/datos.gob.mx — IED por municipio Querétaro',
    'https://www.datos.gob.mx/dataset/inversion_extranjera_directa',
    'anual',
    'sources.se.ied_municipio_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'se.ied_municipio_queretaro',
    'IED por municipio (Querétaro)',
    'economia',
    'ciudad',
    'Millones USD',
    id,
    'IED anual de Querétaro desagregada por municipio.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.se.ied_municipio_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'INEGI — Llegada total de turistas Querétaro',
    'https://www.inegi.org.mx/temas/turismo/#tabulados',
    'anual',
    'sources.inegi.llegada_total_turistas_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'inegi.llegada_total_turistas_queretaro',
    'Llegada total de turistas (Querétaro)',
    'economia',
    'ciudad',
    'Turistas',
    id,
    'Llegada total anual de turistas en Querétaro.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.inegi.llegada_total_turistas_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Datatur/Sectur — Turismo nacional vs internacional Querétaro',
    'https://datatur.sectur.gob.mx/SitePages/VisitantesInternacionales.aspx',
    'anual',
    'sources.sectur.turismo_nacional_vs_internacional_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.turismo_nacional_vs_internacional_queretaro',
    'Turismo nacional vs internacional (Querétaro)',
    'economia',
    'ciudad',
    'Turistas',
    id,
    'Comparativo anual de turistas nacionales e internacionales en Querétaro.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.turismo_nacional_vs_internacional_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Datatur/Sectur — Derrama económica turística Querétaro',
    'https://datatur.sectur.gob.mx/SitePages/VisitantesInternacionales.aspx',
    'anual',
    'sources.sectur.derrama_economica_turistica_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.derrama_economica_turistica_queretaro',
    'Derrama económica turística (Querétaro)',
    'economia',
    'ciudad',
    'Millones MXN',
    id,
    'Derrama económica turística anual en Querétaro.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.derrama_economica_turistica_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Datatur/Sectur — Ocupación hotelera Querétaro',
    'https://datatur.sectur.gob.mx/SitePages/VisitantesInternacionales.aspx',
    'anual',
    'sources.sectur.ocupacion_hotelera_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.ocupacion_hotelera_queretaro',
    'Ocupación hotelera (Querétaro)',
    'economia',
    'ciudad',
    'Porcentaje',
    id,
    'Porcentaje anual de ocupación hotelera en Querétaro.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.ocupacion_hotelera_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Datatur/Sectur — Crecimiento de habitaciones ocupadas Querétaro',
    'https://datatur.sectur.gob.mx/SitePages/VisitantesInternacionales.aspx',
    'anual',
    'sources.sectur.crecimiento_habitaciones_ocupadas_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'sectur.crecimiento_habitaciones_ocupadas_queretaro',
    'Crecimiento de habitaciones ocupadas (Querétaro)',
    'economia',
    'ciudad',
    'Porcentaje',
    id,
    'Variación porcentual anual de habitaciones ocupadas en Querétaro.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.sectur.crecimiento_habitaciones_ocupadas_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'AIQ — Pasajeros anuales Querétaro',
    'https://aiq.com.mx/estadisticas.php',
    'anual',
    'sources.afac.pasajeros_anuales_aiq_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'afac.pasajeros_anuales_aiq_queretaro',
    'Pasajeros anuales AIQ (Querétaro)',
    'economia',
    'ciudad',
    'Pasajeros',
    id,
    'Pasajeros anuales del Aeropuerto Intercontinental de Querétaro.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.afac.pasajeros_anuales_aiq_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'AIQ — Crecimiento anual de pasajeros Querétaro',
    'https://aiq.com.mx/estadisticas.php',
    'anual',
    'sources.afac.crecimiento_anual_pasajeros_aiq_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'afac.crecimiento_anual_pasajeros_aiq_queretaro',
    'Crecimiento anual de pasajeros (Querétaro)',
    'economia',
    'ciudad',
    'Porcentaje',
    id,
    'Variación porcentual anual de pasajeros del AIQ en Querétaro.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.afac.crecimiento_anual_pasajeros_aiq_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'AIQ — % vuelos nacionales / internacionales Querétaro',
    'https://aiq.com.mx/estadisticas.php',
    'anual',
    'sources.afac.vuelos_nacionales_internacionales_aiq_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'afac.vuelos_nacionales_internacionales_aiq_queretaro',
    '% vuelos nacionales / internacionales (Querétaro)',
    'economia',
    'ciudad',
    'Porcentaje',
    id,
    'Participación anual de vuelos nacionales e internacionales en el AIQ.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.afac.vuelos_nacionales_internacionales_aiq_queretaro'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'AIQ — Histórico de toneladas transportadas Querétaro',
    'https://aiq.com.mx/estadisticas.php',
    'anual',
    'sources.afac.historico_toneladas_transportadas_aiq_queretaro',
    'etl_listo'
)
ON CONFLICT DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'afac.historico_toneladas_transportadas_aiq_queretaro',
    'Histórico de toneladas transportadas (Querétaro)',
    'economia',
    'ciudad',
    'Toneladas',
    id,
    'Toneladas transportadas anuales en el AIQ de Querétaro.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.afac.historico_toneladas_transportadas_aiq_queretaro'
ON CONFLICT (clave) DO NOTHING;
