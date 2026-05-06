---
name: refine-prompt
description: >-
  Refina y estructura instrucciones (rol, objetivo, contexto, formato, criterios)
  para maximizar precisión y exhaustividad del modelo. Usar al delegar tareas
  entre agentes en SDD, al pedir prompt mejorado, instrucción estructurada,
  brief para IA, o cuando el usuario exprese un requerimiento ambiguo que
  deba cumplirse con alta fidelidad.
---

# Refinar prompt (instrucciones estructuradas)

## Propósito

Convertir un requerimiento básico en instrucciones claras y completas que el modelo pueda ejecutar con resultados precisos y exhaustivos, aplicando mejores prácticas de prompting.

## Cuándo usarlo

- **SDD / agentes**: Antes de pasar una tarea a otro agente o subagente; al definir el mensaje de delegación.
- **Usuario**: Cuando pida “mejorar el prompt”, “estructurar la instrucción”, “brief para la IA” o describa una tarea compleja que exija baja ambigüedad.

## Flujo (ejecutar en orden)

1. **Extraer intención**: Qué debe producirse, para quién y con qué restricciones no negociables.
2. **Rol**: Una línea — qué “persona” o especialización debe asumir el modelo para esta tarea.
3. **Objetivo**: Resultado medible o verificable en una o dos oraciones.
4. **Contexto mínimo indispensable**: Solo datos que cambien la respuesta (proyecto, stack, convenciones, archivos clave). Evitar contexto genérico.
5. **Formato de salida**: Estructura explícita (secciones, listas, JSON, tabla, archivos a tocar). Si aplica, límites de extensión.
6. **Criterios de éxito / no hacer**: Qué debe cumplirse para considerar la tarea cerrada; qué evitar (alucinaciones, alcance extra, formatos prohibidos).
7. **Casos límite** (si aplica): Entradas ambiguas, vacíos, errores esperados.
8. **Ejemplo corto** (opcional): Un micro-ejemplo de entrada/salida esperada cuando la forma importa tanto como el contenido.

Si falta información crítica para cumplir el objetivo, **hacer una sola ronda de preguntas concretas** (máximo 3–5 ítems) en lugar de asumir.

## Plantilla de salida

Al aplicar este skill, entregar al usuario (o al siguiente agente) un bloque listo para copiar, con esta estructura:

```markdown
## Rol
[una línea]

## Objetivo
[resultado verificable]

## Contexto
[solo lo necesario]

## Instrucciones
[pasos o reglas de ejecución, numeradas si ayuda]

## Formato de salida
[estructura exacta]

## Criterios de éxito
- ...
## Evitar
- ...

## Casos límite (si aplica)
- ...

## Ejemplo (si aplica)
Entrada: ...
Salida esperada: ...
```

## Integración SDD

- Tras **cerrar requisitos** (`close-requirement`), usar este skill para **convertir** el requisito cerrado en el prompt operativo del agente que implementa o audita.
- No sustituye el contrato técnico: las instrucciones deben **alinearse** con tipos, alcance y criterios de aceptación ya definidos; este skill solo **empaqueta** y **aclara** la ejecución.

## Anti-patrones

- Prompts largos sin jerarquía (mezclar rol, tono y detalle técnico sin secciones).
- Objetivos vagos (“hazlo bien”, “optimiza”) sin criterio verificable.
- Pedir “exhaustivo” sin delimitar **ámbito** (tema, tiempo, fuentes, archivos).
