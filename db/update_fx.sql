INSERT INTO diario.datos (indicador_id, nivel_geografico, valor, unidad, fecha) 
VALUES (1, 'nacional', 16.85, 'MXN/USD', '2026-04-07') 
ON CONFLICT ON CONSTRAINT uq_diario_datos_fecha_entidad_norm 
DO UPDATE SET valor = EXCLUDED.valor;
