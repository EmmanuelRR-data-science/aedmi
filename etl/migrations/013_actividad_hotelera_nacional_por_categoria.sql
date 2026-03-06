-- Actividad Hotelera nacional por categoría (DataTur Base70centros). Para pestaña "Por Categoría".
CREATE TABLE IF NOT EXISTS actividad_hotelera_nacional_por_categoria (
    anio INT NOT NULL,
    categoria VARCHAR(80) NOT NULL,
    cuartos_disponibles_pd DECIMAL(20, 2),
    cuartos_ocupados_pd DECIMAL(20, 2),
    porc_ocupacion DECIMAL(6, 2),
    PRIMARY KEY (anio, categoria)
);
CREATE INDEX IF NOT EXISTS idx_ah_nacional_cat_anio ON actividad_hotelera_nacional_por_categoria (anio);
