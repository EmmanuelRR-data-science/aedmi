# Activos opcionales de la API

## Plantilla PPTX para exportación (VPS / Docker / local)

| Archivo | Uso |
|---------|-----|
| **`plantilla-aedmi-export.pptx`** | **Usar este** con la API: 2 diapositivas (portada + slide tipo), formas `AEDMI_*`. Generado desde la plantilla larga de ejemplo. |
| `plantilla-estudio-mercado-ejemplo.pptx` | Deck original de referencia (~32 slides). **No** enlaces directamente en `PPTX_TEMPLATE_PATH` (no cumple §12). |

### Regenerar `plantilla-aedmi-export.pptx`

Si actualizas el archivo largo de ejemplo y quieres volver a recortar y renombrar:

```bash
cd api
uv run python scripts/preparar_plantilla_aedmi_export.py
```

### Variable de entorno

- **Docker Compose** (`./api` montado en `/app`):

  ```env
  PPTX_TEMPLATE_PATH=/app/assets/plantilla-aedmi-export.pptx
  ```

- **UV / local** con directorio de trabajo `api/`:

  ```env
  PPTX_TEMPLATE_PATH=assets/plantilla-aedmi-export.pptx
  ```

Tras cambiar `.env`, reinicia el proceso de la API. En el **frontend** no hay pasos extra: cola → Descargar PPTX como siempre.

Detalle de nombres de formas: `docs/guias/plantilla-pptx-corporativa.md` y `SPEC_DRIVEN_CONTRACT.md` §12.
