-- Mock data for Localidades verification
DELETE FROM crecimiento_historico_localidad WHERE estado_codigo = '01' AND municipio_codigo = '001' AND loc_codigo = '0001';
DELETE FROM distribucion_poblacion_localidad WHERE estado_codigo = '01' AND municipio_codigo = '001' AND loc_codigo = '0001';
DELETE FROM localidades WHERE estado_codigo = '01' AND municipio_codigo = '001' AND loc_codigo = '0001';

INSERT INTO localidades (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre)
VALUES ('01', 'Aguascalientes', '001', 'Aguascalientes', '0001', 'Aguascalientes');

INSERT INTO distribucion_poblacion_localidad (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre, pobtot, pobfem, pobmas, data_json)
VALUES ('01', 'Aguascalientes', '001', 'Aguascalientes', '0001', 'Aguascalientes', 1000, 520, 480, '{"P_0A4_F": 40, "P_0A4_M": 38, "P_5A9_F": 45, "P_5A9_M": 42, "P_10A14_F": 48, "P_10A14_M": 45}');

INSERT INTO crecimiento_historico_localidad (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre, anio, poblacion, hombres, mujeres)
VALUES 
('01', 'Aguascalientes', '001', 'Aguascalientes', '0001', 'Aguascalientes', 2005, 800, 390, 410),
('01', 'Aguascalientes', '001', 'Aguascalientes', '0001', 'Aguascalientes', 2010, 900, 440, 460),
('01', 'Aguascalientes', '001', 'Aguascalientes', '0001', 'Aguascalientes', 2020, 1000, 480, 520);
