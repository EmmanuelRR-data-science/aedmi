---
name: sync
description: >-
  Alinea archivos de documentación (.md) con las decisiones recientes del chat
  (contrato, requisitos, ADRs). Pausa el flujo de implementación activa.
  Usar con /sync, "sincronizar doc", "actualizar el contrato con la charla",
  mantenimiento SDD, o cuando Kiro pida alinear spec tras 2-3 mensajes de ideación.
---

# sync

## Propósito

Alinear los archivos de documentación con las decisiones tomadas durante la conversación.

## Funcionamiento

Pausa la charla de "vibe coding" para actualizar los archivos `.md` (como el `SPEC_DRIVEN_CONTRACT.md` y, si aplica, specs bajo `.kiro/specs/`, `requisitos-*.md`, `schema.md`, README de alcance) basándose en los **últimos acuerdos** alcanzados en el chat. No reescribir documentos enteros por estilo: **fusión mínima** que refleje decisiones nuevas o que contradigan el texto actual.

## Momento SDD

Skill de **mantenimiento** que Kiro (u orquestación) activa **cada 2 o 3 mensajes** de ideación para evitar que el contrato técnico quede desfasado respecto a la conversación.

## Instrucciones para el agente

1. **Leer antes de editar**: localizar en el repo los `.md` que gobiernan el feature (contrato, requisitos, notas de diseño). Si el usuario señala un path, respetarlo.
2. **Extraer acuerdos** del hilo reciente: decisiones explícitas ("cerramos X", "queda Y"), criterios de aceptación, exclusiones, nombres de entidades/API. No inventar acuerdos que no consten en el chat.
3. **Comparar** con el documento: marcar secciones obsoletas, ambiguas o en conflicto con lo acordado.
4. **Actualizar** con ediciones puntuales:
   - Añadir o ajustar subsecciones bajo el mismo estilo y encabezados del doc.
   - Marcar decisiones **Closed** donde el proyecto lo exige (evitar deriva de contexto).
   - Si un acuerdo **revierte** uno anterior, reemplazar o anotar la versión vigente; no dejar dos fuentes de verdad contradictorias.
5. **Resumen al usuario** (breve): qué archivos tocaste y qué decisiones quedaron plasmadas. No mezclar con implementación de código en el mismo turno salvo que el usuario lo pida.
6. **No** usar sync para generar PR, commits o tests; eso son otras skills.

## Lista de comprobación rápida

- [ ] Cada cambio en el `.md` está respaldado por un acuerdo en la conversación.
- [ ] El contrato y los requisitos no se contradicen entre sí tras el sync.
- [ ] Decisiones técnicas relevantes quedan marcadas como cerradas o explícitas.

## Disparadores habituales

- Comando del usuario: `/sync`
- Frases: "sincroniza la documentación", "pon el contrato al día con lo que hablamos", "actualiza el SPEC con lo acordado"
