-- Mock data for Ocotepec, Almoloya, Hidalgo
DELETE FROM crecimiento_historico_localidad WHERE estado_codigo = '13' AND municipio_codigo = '004' AND loc_codigo = '0021';
DELETE FROM distribucion_poblacion_localidad WHERE estado_codigo = '13' AND municipio_codigo = '004' AND loc_codigo = '0021';
DELETE FROM localidades WHERE estado_codigo = '13' AND municipio_codigo = '004' AND loc_codigo = '0021';

INSERT INTO localidades (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre)
VALUES ('13', 'Hidalgo', '004', 'Almoloya', '0021', 'Ocotepec');

INSERT INTO distribucion_poblacion_localidad (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre, pobtot, pobfem, pobmas, data_json)
VALUES ('13', 'Hidalgo', '004', 'Almoloya', '0021', 'Ocotepec', 450, 230, 220, '{"P_0A4_F": 20, "P_0A4_M": 18, "P_5A9_F": 25, "P_5A9_M": 22, "P_10A14_F": 28, "P_10A14_M": 25}');

INSERT INTO crecimiento_historico_localidad (estado_codigo, estado_nombre, municipio_codigo, municipio_nombre, loc_codigo, localidad_nombre, anio, poblacion, hombres, mujeres)
VALUES 
('13', 'Hidalgo', '004', 'Almoloya', '0021', 'Ocotepec', 2005, 380, 185, 195),
('13', 'Hidalgo', '004', 'Almoloya', '0021', 'Ocotepec', 2010, 410, 200, 210),
('13', 'Hidalgo', '004', 'Almoloya', '0021', 'Ocotepec', 2020, 450, 220, 230);
