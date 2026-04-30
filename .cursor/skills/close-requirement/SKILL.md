---
name: close-requirement
description: >-
  Convierte ideas vagas o tareas “en bruto” en requisitos técnicos cerrados mediante
  preguntas estructuradas (forma de solución, salida esperada, casos límite, actor,
  criterios de éxito). No explora ni ideación abierta: fuerza decisiones. Fase SDD
  previa al Arquitecto: ejecutar antes de SPEC_DRIVEN_CONTRACT o /plan. Usar cuando
  el alcance sea ambiguo, falten criterios de aceptación, o el usuario invoque
  cerrar requisito, requisito cerrado, pre-contract, o close-requirement.
---

# Cerrar requisito (close-requirement)

## Cuándo aplicar

- **Antes** de que el Arquitecto genere `SPEC_DRIVEN_CONTRACT.md`, `/plan`, o contrato técnico.
- Entrada vaga: “mejorar el dashboard”, “integrar X”, “como el otro sistema”.
- No es brainstorming: si el usuario pide explorar opciones, redirige brevemente: aquí solo se **cierran** decisiones pendientes.

## Qué NO hacer

- No ampliar el producto con features no pedidas.
- No sustituir la conversación de negocio completa; si faltan datos de negocio, lista **preguntas bloqueantes** y detén el cierre hasta respuesta.

## Flujo (orden fijo)

1. **Reformular en una frase** lo que cree entender; pedir confirmación en una sola pregunta si hace falta.
2. Recorrer las **cinco dimensiones** (siguiente sección): solo preguntas que **obliguen a elegir** (sí/no, A/B, rangos, listas cerradas). Evitar “¿qué te parece mejor?” abierto.
3. Emitir el **artefacto de salida** unificado.
4. Marcar cada decisión como **Cerrado** en el documento (alineado a “no context drift” en specs).

## Las cinco dimensiones (preguntas guía)

### 1. Actor

- ¿Quién dispara la acción y con qué rol/permiso?
- ¿Hay actores secundarios (sistema, batch, admin)?

### 2. Forma de la solución

- ¿Es cambio de UI, API, ETL, infra, datos, integración externa, o combinación?
- ¿Dónde vive el comportamiento (capa / servicio / módulo) a alto nivel?
- ¿Reutiliza patrón existente del repo o es patrón nuevo? (obliga a una opción.)

### 3. Salida esperada

- Artefactos concretos: pantallas, endpoints, eventos, archivos, mensajes de error.
- Formato y campos mínimos obligatorios (lista cerrada o “N/A explícito”).

### 4. Casos límite y exclusiones

- Vacío, timeout, permiso denegado, datos incompletos, volumen máximo esperado.
- Qué queda **fuera de alcance** en esta entrega (explícito).

### 5. Criterios de éxito

- Cómo se valida en prueba (manual o automatizada): checks verificables.
- Definición de “hecho” para esta historia (una lista corta, no filosofía).

## Artefacto de salida (plantilla)

Entregar en markdown (o donde indique el usuario):

```markdown
# Requisito cerrado: [título corto]

## Contexto (1–3 frases)
[Actor + problema + resultado deseado en términos de negocio/operación]

## Alcance
- **Incluye:** …
- **Excluye (esta entrega):** …

## Decisiones cerradas
| Dimensión | Decisión | Estado |
|-----------|----------|--------|
| Actor | … | Cerrado |
| Forma de solución | … | Cerrado |
| Salida esperada | … | Cerrado |
| Casos límite | … | Cerrado |
| Criterios de éxito | … | Cerrado |

## Entregables técnicos (lista verificable)
- …

## Preguntas bloqueantes restantes
- [ ] … (si ninguna: “Ninguna — listo para contrato / Arquitecto”)
```

## Handoff

- Si **no** hay preguntas bloqueantes: indicar que puede procederse al **Arquitecto** y al contrato técnico.
- Si las hay: no avanzar a implementación ni contrato hasta resolverlas.
