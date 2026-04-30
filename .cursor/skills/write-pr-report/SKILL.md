---
name: write-pr-report
description: >-
  Genera descripciones de Pull Request breves (150-300 palabras), estructura fija
  y orientadas a revisor humano; prioriza API, servicios, dominio y tests.
  Usar al cerrar SDD cuando el código ya fue auditado, al pedir texto de PR,
  descripción de merge request, o write-pr-report.
---

# write-pr-report

## Propósito

Generar una descripción de Pull Request (PR) que sea legible y confiable para un revisor humano.

## Momento SDD

Se activa al final del flujo, cuando el código ya ha sido auditado y está listo para ser revisado por un humano.

## Funcionamiento

Sigue reglas estrictas de brevedad (150-300 palabras) y estructura fija (Resumen, Qué cambió, Validación, Notas, Riesgos). Elimina el "ruido" de la IA y se enfoca en el valor aportado en las capas de API, Servicios, Dominio o Tests.

## Instrucciones para el agente

1. **No escribir antes de tener contexto real**: usar diff, commits, tests ejecutados o resumen de auditoría ya hecha; no inventar alcance.
2. **Contar palabras** del cuerpo final (excluyendo títulos de sección si van en línea propia); ajustar hasta quedar entre **150 y 300 palabras**.
3. **Orden obligatorio** de secciones y encabezados exactos (en español):

### Resumen

2-4 oraciones: qué problema cierra o qué capacidad entrega, para quién (rol o flujo). Sin adjetivos vacíos ("robusto", "elegante", "mejorado significativamente").

### Qué cambió

Lista breve o párrafo denso, **priorizando** en este orden lo que tocaste:

- **API** (rutas, contratos, códigos de error, breaking changes)
- **Servicios / aplicación** (orquestación, integraciones)
- **Dominio** (reglas de negocio, invariantes, modelos)
- **Tests** (qué comportamiento queda garantizado)

Omite UI trivial, formato masivo de archivos o refactors cosméticos salvo que cambien comportamiento o contrato.

### Validación

Cómo se comprobó: tests automatizados (comando o suite), prueba manual mínima, o referencia al reporte de auditoría. Si no hubo tests, decirlo en una frase y qué validación manual hiciste.

### Notas

Solo información que el revisor **necesita** para aprobar: flags, variables de entorno, migraciones, orden de despliegue, dependencias entre PRs.

### Riesgos

Riesgos concretos (regresión, compatibilidad, datos, performance) y mitigación en una línea cada uno. Si no hay riesgos relevantes: una frase tipo "Riesgo bajo; sin cambios de contrato público."

## Anti-ruido (prohibido o reducido al mínimo)

- Frases genéricas ("este PR mejora la calidad del código", "refactorización para mantenibilidad" sin hecho atado).
- Listar cada archivo tocado sin valor semántico.
- Repetir el Resumen en Qué cambió.
- Markdown decorativo excesivo (tablas innecesarias, emojis de relleno).
- Superar 300 palabras o quedar por debajo de 150 sin que el usuario pida excepción.

## Plantilla de salida

Copiar estructura; reemplazar contenido:

```markdown
## Resumen

[texto]

## Qué cambió

[texto o lista densa]

## Validación

[texto]

## Notas

[texto]

## Riesgos

[texto]
```

Si el usuario pide el cuerpo en inglés para GitHub/GitLab, traducir **solo** los encabezados a: `Summary` / `What changed` / `Validation` / `Notes` / `Risks`, manteniendo el mismo orden y límites de palabras.
