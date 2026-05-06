# Plantilla PPTX corporativa (exportación desde el dashboard)

La aplicación puede **rellenar una plantilla PowerPoint** con los mismos datos que ya envías al exportar (título, subtítulo, imagen de la gráfica, análisis y leyenda). Los **logos, fondos y estilos** los define el archivo `.pptx`; la app **no** sustituye fuentes ni marca en el master.

**Importante:** la plantilla **no se sube desde el navegador**. Quien opera el servidor coloca el archivo en disco y configura la variable de entorno. Los analistas usan el flujo habitual (**+ Cola**, **Descargar PPTX (lote)**, etc.); si la plantilla está activa, el archivo descargado sigue ese diseño.

### Prueba rápida en el repo

En `api/assets/` está **`plantilla-aedmi-export.pptx`**: ya recortada a **2 diapositivas** y con nombres `AEDMI_*`. Configura `PPTX_TEMPLATE_PATH` (ver `api/assets/README.md`), reinicia la API y exporta desde el dashboard.

---

## Para quien administra la API

### 1. Variable de entorno

En el `.env` de la API (o en el orquestador):

```env
PPTX_TEMPLATE_PATH=C:\ruta\completa\aedmi-plantilla.pptx
```

- En **Linux/VPS:** ruta absoluta accesible por el proceso (p. ej. `/opt/aedmi/plantilla.pptx`).
- Con **Docker Compose** y volumen `./api:/app`:** usa la plantilla ya preparada:

  ```env
  PPTX_TEMPLATE_PATH=/app/assets/plantilla-aedmi-export.pptx
  ```

- Si la variable está vacía, el archivo no existe o no es legible: la API usa la generación **programática** anterior (diseño genérico en código).

### 2. Estructura obligatoria del `.pptx`

Debe tener **exactamente dos diapositivas**, en este orden:

| Índice | Uso |
|--------|-----|
| **0** | Portada |
| **1** | Diapositiva “tipo” (se duplica por cada gráfica del lote) |

En **PowerPoint**, abre **Panel de selección** (*Inicio → Organizar → Panel de selección*) y nombra cada **forma** exactamente así (mayúsculas y guiones bajos):

| Nombre de la forma | Diapositiva | Contenido que inserta la app |
|--------------------|-------------|------------------------------|
| `AEDMI_PORTADA_TITULO` | 0 | Título del lote (*titulo presentación*) o título en exportación de una sola gráfica |
| `AEDMI_TITULO` | 1 | Título del indicador / ítem |
| `AEDMI_SUBTITULO` | 1 | Contexto (p. ej. entidad), puede quedar vacío |
| `AEDMI_IMAGEN` | 1 | **Marcador de imagen** o imagen incrustada donde va el PNG de la gráfica |
| `AEDMI_ANALISIS` | 1 | Texto de análisis (etiqueta revisado/IA + cuerpo) |
| `AEDMI_FUENTE` | 1 | Leyenda de fuente (pie), puede quedar vacío |

Si falta alguno de estos nombres en la plantilla, la API puede responder **500** cuando intente usar modo plantilla.

### 3. Plantilla de ejemplo en segundos

Desde la carpeta `api/` del repositorio:

```bash
uv run python scripts/generar_plantilla_ejemplo_pptx.py
```

Genera `aedmi-plantilla-ejemplo.pptx` (donde ejecutes el comando, por defecto en `api/`). Ábrela en PowerPoint, sustituye fondos y logos corporativos, **conserva los nombres** `AEDMI_*`, guarda y apunta `PPTX_TEMPLATE_PATH` a ese archivo.

---

## Para quien usa el dashboard (analista)

No hay pasos nuevos en la interfaz:

1. Añade gráficas a la **cola** como siempre.
2. Pulsa **Descargar PPTX (lote)** (o exporta una sola gráfica, o el PPTX dentro del ZIP).

Si el administrador configuró una plantilla válida, el PowerPoint descargado **sigue ese layout**. Si no, verás el PPTX con el formato generado por la app (*legacy*).

---

## Referencias técnicas

- Contrato y criterios: `SPEC_DRIVEN_CONTRACT.md` (sección **12** y **12.5**).
- Código: `api/core/presentacion_plantilla.py`, variable `pptx_template_path` en `api/core/config.py`.
