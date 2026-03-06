# ETL UI (Streamlit)

Minisitio para ejecutar el ETL por indicador, hacer preview y cargar datos en **PostgreSQL local**.

## Requisitos

- Python 3.11+
- Dependencias del proyecto (incluye `streamlit` en `pyproject.toml`)
- PostgreSQL local con las tablas creadas (`etl/schema.sql` o migraciones)

## Variables de entorno (PostgreSQL)

Ajuste según su entorno local:

- `POSTGRES_HOST` — por defecto `db` (Docker); en local use `localhost`
- `POSTGRES_PORT` — p. ej. `5432` o `5433`
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`

Opcional para Producto Aeropuertos:

- `PRODUCTO_AEROPUERTOS_XLSX` — ruta al Excel; si no se define, se usa `%USERPROFILE%\Downloads\producto-aeropuertos-2006-2025-nov-29122025.xlsx`

## Cómo ejecutar

Desde la **raíz del proyecto**:

```bash
uv run streamlit run etl_ui/app.py
```

O con el intérprete que use el proyecto:

```bash
python -m streamlit run etl_ui/app.py
```

Se abrirá el navegador en `http://localhost:8501`.

## Uso

1. **Seleccionar indicador** en la barra lateral (Balanza Comercial, Aeropuertos, Balanza Visitantes, Anuncios, Participación Mercado Aéreo).
2. **Preview (Extraer + Transformar)** — ejecuta solo E+T y muestra la tabla en pantalla. No escribe en la BD.
3. Revisar los datos en la vista previa.
4. **Guardar en BD (Load)** — escribe en PostgreSQL los datos del último preview. Debe haber ejecutado antes el Preview para ese mismo indicador.
5. **Programar** — en el acordeón se indican instrucciones para cron o Programador de tareas; si algo falla, revisar los logs de esa ejecución.

## Indicadores configurados (sección Nacional)

Incluye todos los indicadores de la pestaña **Nacional** del dashboard:

| Indicador | Tabla(s) |
|-----------|----------|
| KPIs Nacional (resumen) | `kpis_nacional` |
| Crecimiento poblacional nacional | `crecimiento_poblacional_nacional` |
| Distribución población por edad | `estructura_poblacional_inegi` |
| Distribución población por sexo | `distribucion_sexo_inegi` |
| PEA | `pea_inegi` |
| Población por sector de actividad | `pob_sector_actividad` |
| Inflación nacional | `inflacion_nacional` |
| Tipo de cambio (MXN/USD) | `tipo_cambio_banxico_*` |
| Proyección PIB | `pib_proyeccion_fmi` |
| IED flujo por entidad | `ied_flujo_entidad` |
| IED por país de origen | `ied_paises` |
| IED por sector económico | `ied_sectores` |
| Ranking Turismo Mundial | `ranking_turismo_wb` |
| Balanza de Visitantes | `balanza_visitantes_inegi` |
| Balanza Comercial por Producto | `balanza_comercial_producto` |
| Operaciones Aeroportuarias | `producto_aeropuertos_nacional` |
| Participación Mercado Aéreo | `participacion_mercado_aereo`, `participacion_internacional_region` |
| Actividad hotelera nacional | `actividad_hotelera_nacional`, `actividad_hotelera_nacional_por_categoria` |
| Anuncios de Inversión Combinados | `anuncios_inversion_combinados` |
| Anuncios de Inversión Base | `anuncios_inversion_base` |
