---
name: conceptual-explain
description: >-
  Entrega explicaciones conceptuales profundas (qué, por qué, dónde) sobre
  comportamiento técnico o patrones de diseño; no sustituye parches rápidos.
  Usar cuando el usuario pida entender arquitectura, DDD, TDD, decisiones de
  diseño, o antes de aprobar contrato o implementación en SDD; términos:
  explicar en profundidad, brecha de habilidades, transferencia de conocimiento,
  por qué esta decisión, contexto del patrón.
---

# Explicación conceptual (transferencia de conocimiento)

## Propósito (texto de requisito)

Cerrar la "brecha de habilidades" del usuario mediante explicaciones conceptuales profundas.

## Funcionamiento (texto de requisito)

No ofrece soluciones rápidas ni parches. Optimiza para la claridad conceptual y la transferencia de conocimiento, explicando el qué, el por qué y el dónde de un comportamiento técnico o patrón de diseño.

## Momento SDD (texto de requisito)

Se usa bajo demanda cuando el usuario necesita entender una decisión de arquitectura (como DDD o TDD) antes de aprobar un contrato o una implementación.

---

## Instrucciones para el agente

1. **Leer el skill primero** cuando el usuario pida comprensión profunda, no entrega inmediata.
2. **Prioridad**: claridad conceptual y transferencia (el usuario debe poder razonar después sin depender de la respuesta literal).
3. **Estructura sugerida** (adaptar al tema; no rellenar secciones vacías):
   - **Qué**: definición o comportamiento observable en términos del dominio/problema.
   - **Por qué**: motivación, trade-offs, costos de no hacerlo así.
   - **Dónde**: en qué capas, archivos o momentos del flujo aplica; límites y anti-ejemplos breves.
4. **Evitar** por defecto: snippets largos como “la solución”; listas de pasos de implementación sin el marco conceptual. Si el usuario también pide código, separar claramente la parte conceptual de la parte operativa.
5. **SDD**: si la pregunta es sobre aprobar contrato o spec, enlazar la explicación a criterios de aceptación, límites del contrato y riesgos de malinterpretar el término.

## Activación

- Explícita: “explícame en profundidad”, “quiero entender el porqué”, “antes de aprobar el contrato…”.
- Implícita: dudas sobre DDD, TDD, capas, límites bounded context, estrategia de pruebas frente a alcance.

## Comprobación breve al cerrar

Una frase que el usuario podría usar para explicárselo a otra persona (ensayo de transferencia).
