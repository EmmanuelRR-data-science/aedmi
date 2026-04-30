# SPEC_DRIVEN_CONTRACT — Exportación de lote: PPTX, Excel nativo y Gamma

**Proyecto:** AEDMI — Market Study App  
**Estado:** Borrador para aprobación (no implementar lógica de aplicación hasta firma explícita de este contrato).  
**Trazabilidad:** Amplía `.kiro/specs/market-study-app/requisitos-export-pptx-seleccion.md` y la cola existente (`PresentationQueueContext`, `POST /export/presentacion/lote`).

---

## 1. Historias de usuario

1. **HU-1** — Como analista, quiero en **cada gráfica exportable** los mismos tres controles que el resto del dashboard: **+ Cola**, **↓ XLSX** (Excel nativo de la vista actual) y **↓ PNG**, además de en la **barra de cola** poder descargar **solo PPTX**, **solo Excel**, **ZIP (PPTX+Excel)** o **solo Gamma** para el lote completo.

2. **HU-2** — Como analista, quiero una opción explícita de **combinación PPTX + Excel** (p. ej. un ZIP con ambos archivos) cuando necesite presentación e hoja de cálculo alineados al mismo lote y orden.

3. **HU-3** — Como analista, quiero que el archivo **Excel** use **datos en celdas** y **gráficos nativos de Excel** referenciando esas celdas (no imágenes PNG incrustadas como sustituto del gráfico).

4. **HU-4** — Como revisor SDD, quiero que el contrato de API y los tipos del cliente estén **cerrados** antes de implementar, para evitar deriva entre UI, payload y archivos generados.

---

## 2. Glosario

| Término | Definición |
|--------|------------|
| **Cola** | Lista ordenada de ítems exportables (misma semántica que hoy: `PresentationQueueItem`). |
| **Lote** | Conjunto de ítems de la cola enviado en una sola petición. |
| **`datos_serie`** | Lista de puntos `{ periodo, valor, entidad_clave?, unidad? }` alineada a `DatoSeriePresentacionItem` en API. |

---

## 3. Decisiones técnicas **Closed**

| ID | Decisión | Valor cerrado |
|----|-----------|----------------|
| **D-01** | Ubicación de botones | **Closed:** En **cada tarjeta de gráfica** con exportación, el usuario ve **+ Cola**, **↓ XLSX** y **↓ PNG** (tercer botón = **XLSX** junto a Cola y PNG). En **`PresentationQueueBar`** existen acciones de **lote**: PPTX, Excel, ZIP (PPTX+Excel) y Gamma. |
| **D-02** | Combinación PPTX + Excel | **ZIP** (`application/zip`) que contiene exactamente dos miembros: `{slug}.pptx` y `{slug}.xlsx`, con el mismo `slug` derivado del título de presentación (reglas ASCII existentes alineadas al lote PPTX). |
| **D-03** | Petición HTTP para lote con Excel | **Un solo endpoint** de lote reutilizado: `POST /export/presentacion/lote` con cuerpo extendido (ver §5). **No** crear endpoints paralelos salvo que en implementación se justifique caché/CDN (fuera de MVP). |
| **D-04** | Discriminador de salida (lote binario) | Campo **`modo_salida`** en `POST /export/presentacion/lote`. Valores: `pptx` \| `xlsx` \| `zip_pptx_xlsx`. Gamma sigue en **`POST /export/presentacion/gamma`** (mismo cuerpo de ítems; `modo_salida` no aplica a esa ruta). |
| **D-05** | Gamma vs binarios | **Gamma:** `POST /export/presentacion/gamma`. **Binarios locales:** `POST /export/presentacion/lote` con `modo_salida` ∈ {`pptx`, `xlsx`, `zip_pptx_xlsx`}. |
| **D-06** | Excel sin `datos_serie` | Si algún ítem del lote **no** trae `datos_serie` o la lista está vacía, el servidor responde **422** con detalle que indique **índices o `grafica_id`** afectados. No se genera XLSX parcial “silencioso”. |
| **D-07** | Tipo de gráfico en Excel | Cada ítem incluye **`excel_chart_kind`**: `column` \| `line` \| `pie` \| `none`. Por defecto **`column`**. `none` = solo tabla de datos + texto de análisis en la hoja, sin objeto chart. |
| **D-08** | Mapeo `excel_chart_kind` desde el dashboard | **Closed:** el valor lo envía el **cliente** según el tipo de visualización del componente (p. ej. alinear `chartType` de `ChartWrapper` o convención por componente Recharts). El servidor **no** infiere desde PNG. |
| **D-09** | Series y `entidad_clave` | **MVP Closed:** tabla con columnas **Período**, **Valor** y, si algún punto trae `entidad_clave` no nulo, columna **Entidad**. Para el eje de categorías del gráfico (salvo `pie`/`none`), si existe columna Entidad con valores no vacíos, usar etiqueta compuesta **`{periodo} — {entidad_clave}`** en una columna auxiliar “Categoría” como fuente del eje X; si no, usar solo **Período**. |
| **D-10** | Límite de ítems | Reutilizar **`MAX_PRESENTACION_LOTE` = 30** para todos los modos. |
| **D-11** | Autenticación | Misma política que hoy: rutas bajo usuario autenticado. |

---

## 4. Contrato de dominio (tipos lógicos)

### 4.1 `ModoSalidaLote` (API y front)

```text
"pptx" | "xlsx" | "zip_pptx_xlsx"
```

### 4.2 `ExcelChartKind`

```text
"column" | "line" | "pie" | "none"
```

### 4.3 Ítem de lote (extensión del ítem actual)

Campos **obligatorios** existentes sin cambio semántico: `grafica_id`, `titulo`, `nivel_geografico`, `entidad_clave`, `imagen_grafica_png_base64` (opcional según ya definido), `subtitulo_contexto`, `texto_analisis`, `leyenda_fuente`, `datos_serie`.

**Nuevo campo obligatorio con default en serialización:**

- `excel_chart_kind`: `ExcelChartKind`, default `column`.

**Request de lote:**

- `titulo_presentacion`: string (existente).
- `items`: array (existente), cada elemento con `excel_chart_kind`.
- `modo_salida`: `ModoSalidaLote` (default **`pptx`** si se omite; ver §11).
- `paleta_hex`: opcional (solo relevante para `gamma`).

---

## 5. Contrato HTTP y respuestas

### 5.1 `POST /export/presentacion/lote`

| `modo_salida` | `Content-Type` | Cuerpo |
|---------------|----------------|--------|
| `pptx` | `application/vnd.openxmlformats-officedocument.presentationml.presentation` | Bytes PPTX (igual que hoy). |
| `xlsx` | `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` | Bytes XLSX. |
| `zip_pptx_xlsx` | `application/zip` | ZIP con `.pptx` + `.xlsx`. |

**Gamma** no usa este endpoint; ver §5.3.

**Cabecera:** `Content-Disposition: attachment; filename="<ascii-safe>.pptx|.xlsx|.zip>"`.

### 5.2 Validación

- `items.length` en `[1, MAX_PRESENTACION_LOTE]`.
- Para `modo_salida` ∈ {`xlsx`, `zip_pptx_xlsx`}: todo ítem debe tener `datos_serie` con `len >= 1` (D-06).
- Para `modo_salida` = `pptx`: **sin** requisito nuevo de `datos_serie` (compatibilidad con PPTX solo imagen).

### 5.3 `POST /export/presentacion/gamma`

Sin cambio de contrato obligatorio; el front puede seguir enviando el mismo cuerpo que hoy. El campo `modo_salida` del modelo de lote, si viajara en el JSON, es **ignorado** por esta ruta.

---

## 6. Contrato frontend

### 6.1 `PresentationQueueBar`

Cuando `items.length > 0`: **Descargar PPTX (lote)**, **Descargar Excel (lote)**, **ZIP (PPTX + Excel)**, **Generar en Gamma (lote)**, **Vaciar**.

Estados de carga: `exportingKind: 'pptx' | 'xlsx' | 'zip' | 'gamma' | null`.

### 6.1b Cada gráfica (`ExportChartExcelButton`)

- Componente **`ExportChartExcelButton`** junto a **`AddToPresentationButton`** y al botón **↓ PNG**, con las mismas props de identificación y `datosSerie` que la cola.
- **↓ XLSX** llama a `POST /export/presentacion/lote` con `modo_salida: "xlsx"` y **un solo** ítem; deshabilitado si no hay `datosSerie`.

### 6.2 `PresentationQueueItem`

- Añadir **`excelChartKind: ExcelChartKind`** persistido al añadir desde `AddToPresentationButton` (nueva prop opcional con default `column`).

### 6.3 `AddToPresentationButton`

- Nueva prop opcional: `excelChartKind?: ExcelChartKind` (default `column`).

### 6.4 Descarga

- Reutilizar `apiFetchBlob` con el `Content-Type` recibido para forzar extensión `.pptx` / `.xlsx` / `.zip` según `modo_salida`.

---

## 7. Contrato del artefacto Excel (openpyxl)

- **Un libro** por descarga de lote.
- **Una hoja por ítem**, nombre sanitizado (≤ 31 caracteres), orden = orden de la cola.
- Por hoja: bloque de metadatos (título, subtítulo de contexto), tabla de datos desde `datos_serie`, gráfico nativo según `excel_chart_kind` referenciando el rango de datos, celda(s) de texto para análisis (misma prioridad revisado/IA que PPTX, ya resuelta en cliente o servidor según patrón actual del lote), pie de leyenda de fuente si existe.
- **No** incrustar PNG de la gráfica como sustituto del chart en MVP.

---

## 8. Criterios de aceptación (Gherkin resumido)

- **CA-1** Dado una cola con ≥1 gráfica con `datos_serie`, cuando el usuario pulsa **Descargar Excel (lote)**, entonces recibe un `.xlsx` con una hoja por ítem y gráficos ligados a celdas (salvo `excel_chart_kind: none`).
- **CA-2** Dado una cola donde falta `datos_serie` en algún ítem, cuando el usuario pulsa **Descargar Excel** o **ZIP (PPTX + Excel)**, entonces la API responde **422** con identificación del ítem problemático.
- **CA-3** Dado el mismo lote, cuando el usuario pulsa **Descargar PPTX (lote)**, entonces el comportamiento y el archivo son equivalentes al contrato previo (imágenes + análisis), sin exigir `datos_serie`.
- **CA-4** Dado el mismo lote, cuando el usuario pulsa **ZIP (PPTX + Excel)**, entonces recibe un `.zip` con dos archivos cuyos nombres base coinciden.
- **CA-5** Dado el usuario autenticado, cuando pulsa **Generar en Gamma (lote)**, entonces se mantiene el comportamiento actual de Gamma (URLs / mensajes).
- **CA-6** Dado una gráfica con `datosSerie`, cuando el usuario pulsa **↓ XLSX** en la tarjeta, entonces se descarga un `.xlsx` de un solo ítem con gráfico nativo (según `excel_chart_kind`).

---

## 9. Requisitos no funcionales

- Tamaño de cuerpo y timeouts: alineados a `requisitos-export-pptx-seleccion.md` (RNF-01 a RNF-04).
- No persistir binarios en disco en servidor; generación en memoria.
- Tests: TDD — tests de contrato para generación XLSX y para respuesta ZIP antes o en paralelo al código de producción.

---

## 10. Fuera de alcance (MVP de este contrato)

- Re-fetch de series desde BD cuando falte `datos_serie` en cliente.
- Mapa / AGEB en el mismo lote Excel.
- Edición de estilos de gráfico Excel al detalle (colores de marca); Excel usará tema por defecto.

---

## 11. Compatibilidad hacia atrás

- **Closed:** Las peticiones existentes a `POST /export/presentacion/lote` **sin** `modo_salida` se interpretan como **`modo_salida: "pptx"`** (mismo comportamiento y media type que hoy). Las nuevas implementaciones deben documentar deprecación opcional de omisión del campo tras una versión acordada.

---

## 12. Checklist de aprobación

- [x] Producto aprueba D-01 (botones en barra **y** por gráfica).
- [ ] Producto aprueba D-09 (manejo MVP de `entidad_clave` en Excel).
- [ ] Arquitectura aprueba D-03 y D-11.
- [ ] Contrato congelado en schemas Pydantic y tipos TypeScript antes del primer merge de lógica.

---

*Documento generado como paso formal de /plan. Ajustar IDs y defaults con el comité antes de `/implement`.*
