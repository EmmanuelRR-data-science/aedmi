-- ============================================================
-- Seed: KPIs Nacionales — Banxico
-- Fuentes: PIB trimestral, Tipo de cambio, Inflación (INPC)
-- ============================================================

-- Fuente 1: Banxico — PIB trimestral (MXN)
INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Banxico — PIB Trimestral (MXN)',
    'https://www.banxico.org.mx/SieAPIRest/service/v1/series/SR16734/datos',
    'mensual',
    'sources.banxico.pib_trimestral',
    'pendiente'
)
ON CONFLICT DO NOTHING;

-- Fuente 2: Banxico — Tipo de cambio USD/MXN (FIX)
INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Banxico — Tipo de Cambio USD/MXN (FIX)',
    'https://www.banxico.org.mx/SieAPIRest/service/v1/series/SF43718/datos',
    'diario',
    'sources.banxico.tipo_cambio',
    'pendiente'
)
ON CONFLICT DO NOTHING;

-- Fuente 3: Banxico — Inflación INPC (variación anual)
INSERT INTO public.fuentes_datos (nombre, url_referencia, periodicidad, modulo_etl, estado)
VALUES (
    'Banxico — Inflación INPC (variación anual %)',
    'https://www.banxico.org.mx/SieAPIRest/service/v1/series/SP1/datos',
    'mensual',
    'sources.banxico.inflacion',
    'pendiente'
)
ON CONFLICT DO NOTHING;

-- Indicadores (se insertan después de que las fuentes existan)
INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'banxico.pib_trimestral_mxn',
    'PIB Nacional (MXN)',
    'economia',
    'nacional',
    'Millones de pesos',
    id,
    'Producto Interno Bruto trimestral de México a precios corrientes en pesos mexicanos. Fuente: Banxico SIE serie SR16734.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.banxico.pib_trimestral'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'banxico.tipo_cambio_usd_mxn',
    'Tipo de Cambio USD/MXN',
    'economia',
    'nacional',
    'Pesos por dólar',
    id,
    'Tipo de cambio FIX publicado por Banxico (pesos mexicanos por dólar estadounidense). Fuente: Banxico SIE serie SF43718.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.banxico.tipo_cambio'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'banxico.inflacion_inpc_anual',
    'Inflación INPC (variación anual)',
    'economia',
    'nacional',
    '%',
    id,
    'Variación porcentual anual del Índice Nacional de Precios al Consumidor (INPC). Fuente: Banxico SIE serie SP1.',
    'line'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.banxico.inflacion'
ON CONFLICT (clave) DO NOTHING;

INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, fuente_id, descripcion, tipo_grafica)
SELECT
    'banxico.pib_trimestral_usd',
    'PIB Nacional (USD)',
    'economia',
    'nacional',
    'Millones de dólares',
    id,
    'PIB trimestral calculado: PIB MXN / Tipo de cambio FIX promedio del período. Fuente: Banxico SIE.',
    'bar'
FROM public.fuentes_datos WHERE modulo_etl = 'sources.banxico.pib_trimestral'
ON CONFLICT (clave) DO NOTHING;
