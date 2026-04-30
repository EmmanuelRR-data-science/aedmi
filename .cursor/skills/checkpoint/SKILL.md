---
name: checkpoint
description: >-
  Valida en tiempo real si el flujo conversacional ("vibe") sigue siendo fiel al
  contrato técnico inicial; guía una verificación rápida tipo QA Auditor para que
  la ideación no rompa tipos o arquitectura cerrados. Usar en refinamiento SDD,
  ante riesgo de deriva de contexto, o cuando el usuario diga checkpoint,
  punto de control, vibe vs contrato, o alinear con contrato.
---

# Checkpoint (SDD)

## Propósito

Validar en tiempo real si el flujo conversacional ("vibe") sigue siendo fiel al contrato técnico inicial.

## Funcionamiento

Invoca al QA Auditor para realizar una verificación rápida y asegurar que la ideación actual no rompa las definiciones de tipos o arquitectura ya "cerradas".

## Momento SDD

Se usa durante la fase de refinamiento para evitar la deriva de contexto técnica.

---

## Qué hace el agente (verificación rápida)

1. **Cargar la fuente de verdad**  
   Leer el contrato vigente: `SPEC_DRIVEN_CONTRACT.md` en la raíz del repo y/o specs bajo `.kiro/specs/**` (incluye `requisitos-*.md` si definen decisiones cerradas). Si no existe contrato escrito, declararlo y limitar el checkpoint a lo explícitamente acordado en el hilo.

2. **Extraer lo “cerrado”**  
   Listar en viñetas: tipos/interfaces bloqueados, capas (p. ej. DDD), endpoints, exclusiones de alcance, y cualquier tabla “Cerrado” del requisito.

3. **Contrastar con la charla actual**  
   Revisar las últimas propuestas del usuario y del asistente: ¿nuevas features, capas o tipos no contemplados? ¿Contradicciones con exclusiones o decisiones marcadas Cerrado?

4. **Emitir salida breve** (plantilla obligatoria)

```markdown
# Checkpoint SDD — [fecha o tema del hilo]

## Alineación con contrato
- **Cumple:** …
- **Riesgo / desviación:** … (si no hay: "Ninguna detectada en este corte")

## Acción recomendada
- [ ] Continuar en vibe sin cambiar contrato
- [ ] Pausar ideación → `/sync` o actualizar contrato antes de código
- [ ] Profundizar → `/audit` (auditoría completa vs contrato)

## Una línea para Kiro
[Resumen en una frase: alineado | deriva leve | deriva fuerte]
```

5. **No sustituir** un `/audit` formal ni un merge de docs: el checkpoint es **corte transversal ligero**. Si la desviación es estructural, indicar explícitamente que proceda `/audit` o el flujo de contrato acordado en el proyecto.

## Qué NO hacer

- No reescribir el contrato entero en este paso (eso es **sync** o Arquitecto).
- No implementar código nuevo “para probar” una idea que contradice tipos o arquitectura cerrados sin avisar la ruptura.

## Handoff

- **Alineado:** seguir refinando o implementar según contrato.
- **Deriva:** cerrar con preguntas mínimas (estilo close-requirement) o derivar a `/sync` / `/plan` según gravedad.
