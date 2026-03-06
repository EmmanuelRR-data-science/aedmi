# Análisis: datos en PostgreSQL para la ciudad de Mérida (Yucatán)

## Resumen ejecutivo

Con los datos actuales en PostgreSQL **sí se puede obtener** para el **municipio de Mérida** (Yucatán):

| Indicador | ¿Disponible? | Fuente en BD |
|-----------|--------------|--------------|
| **Población total** | **Sí** | `distribucion_poblacion_municipal` (Censo 2020) |
| **Distribución por sexo** | **Sí** | Misma tabla: `pobtot`, `pobfem`, `pobmas` |
| **Distribución por edad** | **Sí** | Misma tabla: campo `data_json` con grupos (P_0A4, P_5A9, …, P_85+) |
| **Crecimiento poblacional anual (histórico)** | **No** a nivel municipio | No existe tabla con años 2005/2010/2020 por municipio |

Para la **localidad** “Mérida” (cabecera): la búsqueda por nombre no encontró filas; puede deberse al nombre exacto en INEGI o a la muestra de crecimiento histórico (solo 6 registros en `crecimiento_historico_localidad`). Los indicadores del **municipio** cubren todo el municipio, incluida la ciudad de Mérida.

---

## Detalle por tabla

### 1. Municipio Mérida (estado Yucatán, código 31)

- **Tabla `municipios`**  
  El catálogo incluye al municipio Mérida (estado Yucatán).

- **Tabla `distribucion_poblacion_municipal`**  
  Hay un registro para Mérida, Yucatán:
  - **Población total (`pobtot`)**: 995,129
  - **Mujeres (`pobfem`)**: 515,760
  - **Hombres (`pobmas`)**: 479,369
  - **Distribución por edad**: en `data_json` (grupos por sexo y edad: P_0A4, P_5A9, …, P_85YMAS).

Con esto se puede obtener:
- Población total.
- Distribución por sexo (hombres/mujeres).
- Distribución por edad (usando los grupos en `data_json`).

- **Tabla `proyeccion_poblacional_municipal`**  
  Para Mérida no hay registros (0 filas). No hay proyección CONAPO cargada para este municipio.

- **Crecimiento poblacional anual histórico**  
  No hay tabla en el esquema que guarde años 2005, 2010 y 2020 **por municipio**. Solo existe `crecimiento_historico_localidad` (por localidad), no equivalente municipal.

### 2. Localidad “Mérida” (cabecera)

- **Tablas `localidades` y `distribucion_poblacion_localidad`**  
  La consulta por estado Yucatán, municipio Mérida y nombre de localidad “Mérida” no devolvió filas. Es posible que en INEGI la cabecera tenga otro nombre o variante (acento, mayúsculas, etc.).

- **Tabla `crecimiento_historico_localidad`**  
  En toda la BD hay solo 6 registros (muestra pequeña del ETL). No se encontraron registros para una localidad “Mérida” en el municipio Mérida.

---

## Conclusión

- **Población total, distribución por sexo y distribución por edad** para la **ciudad de Mérida** se pueden obtener usando el **municipio de Mérida** en:
  - `distribucion_poblacion_municipal`  
  Con eso se cubren los tres indicadores (total, sexo y edad desde `data_json`).

- **Crecimiento poblacional anual (histórico)** para Mérida **no** está disponible en PostgreSQL:
  - No hay tabla de crecimiento histórico por **municipio** (solo por localidad).
  - Para la **localidad** Mérida no hay filas en `crecimiento_historico_localidad` con los criterios usados.

Recomendación: usar el municipio Mérida (Yucatán) para reportar población total, distribución por sexo y por edad; para crecimiento anual histórico sería necesario incorporar otra fuente o una tabla de histórico municipal.
