-- indicadores_adicionales.sql
INSERT INTO public.indicadores (clave, nombre, categoria, nivel_geografico, unidad, tipo_grafica) 
VALUES 
    ('inegi.pib_sector', 'PIB por Sector', 'economia', 'nacional', 'Millones de pesos', 'pie'),
    ('se.ied_sector', 'IED por Sector', 'economia', 'nacional', 'Millones de dólares', 'bar'),
    ('inegi.balanza_visitantes', 'Balanza de Visitantes', 'turismo', 'nacional', 'Millones de dólares', 'area'),
    ('inegi.balanza_comercial', 'Balanza Comercial', 'economia', 'nacional', 'Millones de dólares', 'line'),
    ('inegi.pib_trimestral', 'PIB Trimestral', 'economia', 'nacional', 'Millones de pesos', 'line'),
    ('inegi.actividad_hotelera', 'Actividad Hotelera Nacional', 'turismo', 'nacional', 'Porcentaje', 'line'),
    ('afac.mercado_aereo', 'Participación Mercado Aéreo', 'conectividad_aerea', 'nacional', 'Porcentaje', 'pie'),
    ('afac.operaciones_aeroportuarias', 'Operaciones Aeroportuarias', 'conectividad_aerea', 'nacional', 'Operaciones', 'bar')
ON CONFLICT (clave) DO NOTHING;
