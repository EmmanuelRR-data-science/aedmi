# Esquema de Base de Datos — AEDMI

Documentación completa del esquema PostgreSQL de la Aplicación para Estudios de Mercado (AEDMI).
Actualizar este archivo con cada cambio estructural en la base de datos.

---

## Organización por Schemas

La base de datos usa schemas de PostgreSQL para separar lógicamente los datos según la periodicidad de actualización de su fuente:

| Schema | Periodicidad | Descripción |
|--------|-------------|-------------|
| `public` | — | Metadatos, catálogos y logs del sistema |
| `anual` | Anual | Indicadores que se actualizan una vez al año |
| `mensual` | Mensual | Indicadores que se actualizan cada mes |
| `diario` | Diaria | Indicadores que se actualizan cada día |
| `quinquenal` | Quinquenal | Indicadores que se actualizan cada cinco años (censos) |

---

## Schema `public` — Metadatos y Catálogos

### Tabla `public.fuentes_datos`

Registro de todas las fuentes de datos incorporadas al sistema.

| Columna | Tipo | Restricciones | Descripción |
|---------|------|--------------|-------------|
| `id` | `SERIAL` | PK | Identificador único autoincremental |
| `nombre` | `VARCHAR(200)` | NOT NULL | Nombre descriptivo de la fuente |
| `url_referencia` | `TEXT` | — | URL o referencia de acceso a la fuente |
| `periodicidad` | `VARCHAR(20)` | NOT NULL, CHECK | Frecuencia de actualización: `diario`, `semanal`, `mensual`, `anual`, `quinquenal`, `otra` |
| `ultima_carga` | `TIMESTAMPTZ` | — | Timestamp de la última carga exitosa |
| `modulo_etl` | `VARCHAR(100)` | NOT NULL | Nombre del módulo ETL responsable (ej. `inegi.poblacion`) |
| `estado` | `VARCHAR(20)` | NOT NULL, DEFAULT `pendiente`, CHECK | Estado de avance: `pendiente`, `etl_listo`, `api_lista`, `grafica_lista`, `completo` |
| `activo` | `BOOLEAN` | NOT NULL, DEFAULT `TRUE` | Indica si la fuente está activa |
| `notas` | `TEXT` | — | Notas adicionales sobre la fuente o cambios de estructura |
| `created_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | Timestamp de creación del registro |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | Timestamp de última modificación |

**Índices:** ninguno adicional al PK (tabla de catálogo con bajo volumen).

---

### Tabla `public.indicadores`

Catálogo de todos los indicadores disponibles en el sistema.

| Columna | Tipo | Restricciones | Descripción |
|---------|------|--------------|-------------|
| `id` | `SERIAL` | PK | Identificador único autoincremental |
| `clave` | `VARCHAR(100)` | UNIQUE, NOT NULL | Clave única del indicador (ej. `inegi.poblacion.nacional`) |
| `nombre` | `VARCHAR(300)` | NOT NULL | Nombre completo del indicador |
| `categoria` | `VARCHAR(50)` | NOT NULL, CHECK | Categoría temática: `demografia`, `economia`, `turismo`, `conectividad_aerea` |
| `nivel_geografico` | `VARCHAR(30)` | NOT NULL, CHECK | Nivel de desagregación: `nacional`, `estatal`, `municipal`, `localidad`, `ciudad` |
| `unidad` | `VARCHAR(100)` | — | Unidad de medida (ej. `personas`, `pesos`, `porcentaje`) |
| `fuente_id` | `INTEGER` | FK → `fuentes_datos.id` | Fuente de datos asociada |
| `descripcion` | `TEXT` | — | Descripción detallada del indicador |
| `tipo_grafica` | `VARCHAR(30)` | DEFAULT `bar`, CHECK | Tipo de visualización: `bar`, `line`, `pie`, `area`, `scatter` |
| `activo` | `BOOLEAN` | NOT NULL, DEFAULT `TRUE` | Indica si el indicador está activo |
| `created_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | Timestamp de creación |

**Índices:**
- `idx_indicadores_categoria` — en `categoria` (filtro frecuente)
- `idx_indicadores_nivel` — en `nivel_geografico` (filtro frecuente)
- `idx_indicadores_fuente` — en `fuente_id` (join frecuente)

---

### Tabla `public.analisis`

Almacena los análisis generados por IA y los análisis revisados por el usuario para cada gráfica.

| Columna | Tipo | Restricciones | Descripción |
|---------|------|--------------|-------------|
| `id` | `SERIAL` | PK | Identificador único autoincremental |
| `indicador_id` | `INTEGER` | NOT NULL, FK → `indicadores.id` | Indicador al que pertenece el análisis |
| `nivel_geografico` | `VARCHAR(30)` | NOT NULL | Nivel geográfico de la gráfica analizada |
| `entidad_clave` | `VARCHAR(50)` | — | Clave INEGI de la entidad (estado, municipio, etc.); NULL para nivel nacional |
| `analisis_ia` | `TEXT` | — | Texto del análisis generado por el modelo llama-3.3-70b-versatile |
| `analisis_revisado` | `TEXT` | — | Versión corregida del análisis editada por el usuario |
| `ia_generado_at` | `TIMESTAMPTZ` | — | Timestamp de generación del análisis IA |
| `revisado_at` | `TIMESTAMPTZ` | — | Timestamp de la última revisión del usuario |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | Timestamp de última modificación del registro |

**Restricción UNIQUE:** `(indicador_id, nivel_geografico, entidad_clave)` — garantiza un único registro de análisis por gráfica.

**Índices:**
- `idx_analisis_indicador` — en `indicador_id` (join frecuente)

---

### Tabla `public.etl_logs`

Log de todas las ejecuciones del pipeline ETL, tanto programadas como manuales.

| Columna | Tipo | Restricciones | Descripción |
|---------|------|--------------|-------------|
| `id` | `SERIAL` | PK | Identificador único autoincremental |
| `fuente_id` | `INTEGER` | FK → `fuentes_datos.id` | Fuente de datos procesada |
| `tipo_ejecucion` | `VARCHAR(20)` | NOT NULL, CHECK | Tipo: `programada` o `manual` |
| `inicio` | `TIMESTAMPTZ` | NOT NULL | Timestamp de inicio de la ejecución |
| `fin` | `TIMESTAMPTZ` | — | Timestamp de fin (NULL si aún en curso) |
| `exitoso` | `BOOLEAN` | — | Resultado: `TRUE` éxito, `FALSE` error, NULL en curso |
| `registros_cargados` | `INTEGER` | NOT NULL, DEFAULT `0` | Número de registros insertados exitosamente |
| `errores` | `INTEGER` | NOT NULL, DEFAULT `0` | Número de registros con error |
| `mensaje` | `TEXT` | — | Mensaje descriptivo del resultado o error |
| `usuario` | `VARCHAR(100)` | — | Usuario que disparó la ejecución (solo para ejecuciones manuales) |
| `created_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | Timestamp de creación del registro |

**Índices:**
- `idx_etl_logs_fuente` — en `fuente_id` (filtro frecuente)
- `idx_etl_logs_inicio` — en `inicio DESC` (ordenamiento por fecha)

---

## Schema `anual` — Datos de Actualización Anual

### Tabla `anual.datos`

Almacena series de tiempo anuales para indicadores con periodicidad anual.

| Columna | Tipo | Restricciones | Descripción |
|---------|------|--------------|-------------|
| `id` | `BIGSERIAL` | PK | Identificador único autoincremental |
| `indicador_id` | `INTEGER` | NOT NULL, FK → `public.indicadores.id` | Indicador al que pertenece el dato |
| `nivel_geografico` | `VARCHAR(30)` | NOT NULL | Nivel de desagregación geográfica |
| `entidad_clave` | `VARCHAR(50)` | — | Clave INEGI de la entidad geográfica |
| `valor` | `NUMERIC(20, 4)` | — | Valor numérico del indicador |
| `unidad` | `VARCHAR(100)` | — | Unidad de medida del valor |
| `periodo` | `INTEGER` | NOT NULL | Año de referencia (ej. `2024`) |
| `cargado_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | Timestamp de carga del registro |

**Restricción UNIQUE:** `(indicador_id, nivel_geografico, entidad_clave, periodo)` — garantiza idempotencia en cargas ETL.

**Índices:**
- `idx_anual_indicador` — en `indicador_id`
- `idx_anual_nivel` — en `nivel_geografico`
- `idx_anual_periodo` — en `periodo`
- `idx_anual_entidad` — en `entidad_clave`

---

## Schema `mensual` — Datos de Actualización Mensual

### Tabla `mensual.datos`

Almacena series de tiempo mensuales para indicadores con periodicidad mensual.

| Columna | Tipo | Restricciones | Descripción |
|---------|------|--------------|-------------|
| `id` | `BIGSERIAL` | PK | Identificador único autoincremental |
| `indicador_id` | `INTEGER` | NOT NULL, FK → `public.indicadores.id` | Indicador al que pertenece el dato |
| `nivel_geografico` | `VARCHAR(30)` | NOT NULL | Nivel de desagregación geográfica |
| `entidad_clave` | `VARCHAR(50)` | — | Clave INEGI de la entidad geográfica |
| `valor` | `NUMERIC(20, 4)` | — | Valor numérico del indicador |
| `unidad` | `VARCHAR(100)` | — | Unidad de medida del valor |
| `anio` | `INTEGER` | NOT NULL | Año de referencia |
| `mes` | `SMALLINT` | NOT NULL, CHECK (1–12) | Mes de referencia (1 = enero, 12 = diciembre) |
| `cargado_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | Timestamp de carga del registro |

**Restricción UNIQUE:** `(indicador_id, nivel_geografico, entidad_clave, anio, mes)` — garantiza idempotencia.

**Índices:**
- `idx_mensual_indicador` — en `indicador_id`
- `idx_mensual_nivel` — en `nivel_geografico`
- `idx_mensual_periodo` — en `(anio, mes)` compuesto
- `idx_mensual_entidad` — en `entidad_clave`

---

## Schema `diario` — Datos de Actualización Diaria

### Tabla `diario.datos`

Almacena series de tiempo diarias para indicadores con periodicidad diaria.

| Columna | Tipo | Restricciones | Descripción |
|---------|------|--------------|-------------|
| `id` | `BIGSERIAL` | PK | Identificador único autoincremental |
| `indicador_id` | `INTEGER` | NOT NULL, FK → `public.indicadores.id` | Indicador al que pertenece el dato |
| `nivel_geografico` | `VARCHAR(30)` | NOT NULL | Nivel de desagregación geográfica |
| `entidad_clave` | `VARCHAR(50)` | — | Clave INEGI de la entidad geográfica |
| `valor` | `NUMERIC(20, 4)` | — | Valor numérico del indicador |
| `unidad` | `VARCHAR(100)` | — | Unidad de medida del valor |
| `fecha` | `DATE` | NOT NULL | Fecha de referencia del dato |
| `cargado_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | Timestamp de carga del registro |

**Restricción UNIQUE:** `(indicador_id, nivel_geografico, entidad_clave, fecha)` — garantiza idempotencia.

**Índices:**
- `idx_diario_indicador` — en `indicador_id`
- `idx_diario_nivel` — en `nivel_geografico`
- `idx_diario_fecha` — en `fecha DESC` (consultas por rango de fechas recientes)
- `idx_diario_entidad` — en `entidad_clave`

---

## Schema `quinquenal` — Datos de Actualización Quinquenal

### Tabla `quinquenal.datos`

Almacena datos de indicadores con periodicidad quinquenal (censos, conteos).

| Columna | Tipo | Restricciones | Descripción |
|---------|------|--------------|-------------|
| `id` | `BIGSERIAL` | PK | Identificador único autoincremental |
| `indicador_id` | `INTEGER` | NOT NULL, FK → `public.indicadores.id` | Indicador al que pertenece el dato |
| `nivel_geografico` | `VARCHAR(30)` | NOT NULL | Nivel de desagregación geográfica |
| `entidad_clave` | `VARCHAR(50)` | — | Clave INEGI de la entidad geográfica |
| `valor` | `NUMERIC(20, 4)` | — | Valor numérico del indicador |
| `unidad` | `VARCHAR(100)` | — | Unidad de medida del valor |
| `periodo` | `INTEGER` | NOT NULL | Año del censo o conteo (ej. `2020`) |
| `cargado_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | Timestamp de carga del registro |

**Restricción UNIQUE:** `(indicador_id, nivel_geografico, entidad_clave, periodo)` — garantiza idempotencia.

**Índices:**
- `idx_quinquenal_indicador` — en `indicador_id`
- `idx_quinquenal_nivel` — en `nivel_geografico`
- `idx_quinquenal_periodo` — en `periodo`
- `idx_quinquenal_entidad` — en `entidad_clave`

---

## Diagrama de Relaciones

```
public.fuentes_datos (1) ──< public.indicadores (1) ──< public.analisis
                                      │
                                      ├──< anual.datos
                                      ├──< mensual.datos
                                      ├──< diario.datos
                                      └──< quinquenal.datos

public.fuentes_datos (1) ──< public.etl_logs
```

---

## Notas de Mantenimiento

- Actualizar este archivo con cada cambio estructural (nuevas tablas, columnas, índices).
- Al incorporar una nueva fuente de datos, agregar su tabla en el schema correspondiente a su periodicidad.
- Los campos `entidad_clave` usan claves INEGI estándar (ej. `09` para CDMX, `09001` para Álvaro Obregón).
- La restricción UNIQUE en todas las tablas de datos garantiza idempotencia en el ETL (`INSERT ... ON CONFLICT DO NOTHING`).
