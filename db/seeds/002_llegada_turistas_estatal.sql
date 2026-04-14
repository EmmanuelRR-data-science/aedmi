-- Seed incremental no destructivo para bases ya existentes.
-- Agrega fuente e indicador para la gráfica estatal "Llegada de turistas".

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
