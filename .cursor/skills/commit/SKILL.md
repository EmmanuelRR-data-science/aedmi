---
name: commit
description: >-
  Inspecciona estado de Git, define alcance de staging, crea commits con mensaje
  derivado del criterio de write-pr-report y abre o actualiza el PR con la CLI de
  GitHub (gh). Usar al pedir commit, push, PR, flujo Git/GitHub profesional,
  staging, gh pr create o actualizar PR.
---

# commit — flujo Git y GitHub profesional

## Propósito

Manejar el flujo de Git y GitHub de manera profesional.

## Cuándo aplicar

Invocar cuando el usuario pida: commit, push, abrir o actualizar PR, revisar qué subir, `gh`, o alinear cambios con GitHub tras editar código.

## Relación con write-pr-report

Antes de redactar el **cuerpo del PR** (`gh pr create` / `gh pr edit`), leer y aplicar [.cursor/skills/write-pr-report/SKILL.md](../write-pr-report/SKILL.md): misma prioridad (API → servicios → dominio → tests), mismas secciones y límites de palabras cuando el entregable sea descripción de PR.

El **mensaje de commit** no es un PR completo: es una **derivación** de ese criterio:

| Elemento write-pr-report | En el commit |
|--------------------------|--------------|
| Resumen / Qué cambió | Primera línea del commit + cuerpo breve (si hace falta) |
| Validación | Una o dos líneas en el cuerpo del commit si aporta contexto |
| Notas / Riesgos | Solo si bloquean revisión o despliegue; si no, omitir del commit |

- Primera línea: [Conventional Commits](https://www.conventionalcommits.org/) — `tipo(ámbito opcional): descripción imperativa breve` (ideal ≤72 caracteres). Tipos habituales: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`, `ci`.
- Cuerpo: párrafos cortos; sin adjetivos vacíos ni listar archivos sin valor semántico.

## Flujo operativo

1. **Inspeccionar**
   - `git status`, rama actual, tracking del remoto.
   - `git diff` (unstaged) y, si aplica, `git diff --cached` (staged).
   - No asumir alcance: basarse en el diff real.

2. **Resolver alcance (staging)**
   - Incluir en el stage solo archivos que formen un **cambio coherente** (un tema por commit).
   - Si hay mezcla de temas, proponer **varios commits** o pedir al usuario qué agrupar.
   - `git add` con rutas explícitas o patches acotados; evitar `git add .` salvo que el contexto lo justifique.

3. **Commit**
   - Redactar mensaje según la tabla de derivación y el diff staged.
   - `git commit` (mensaje multilínea vía editor o `-m` + segundo `-m` para cuerpo).

4. **Push**
   - `git push -u origin <rama>` si la rama es nueva en el remoto; si no, `git push`.
   - No usar `--force` ni `--force-with-lease` salvo petición explícita del usuario o política del repo ya acordada.

5. **PR con GitHub CLI (`gh`)**
   - Comprobar si ya hay PR para la rama: `gh pr view` o `gh pr list --head <rama>`.
   - **Crear**: `gh pr create` con `--title` y `--body` o `--body-file`. El cuerpo sigue **write-pr-report** (150-300 palabras, secciones en español salvo que pidan inglés).
   - **Actualizar**: `gh pr edit <número|url>` para título/body según lo que haya cambiado desde el último push.
   - Si `gh` falla por auth: indicar `gh auth login` (no inventar credenciales).

## Restricciones

- No commitear secretos, `.env` reales ni artefactos generados que el `.gitignore` excluya.
- No inventar comandos ejecutados ni tests pasados: reflejar lo que realmente se corrió o marcar "pendiente".

## Referencia rápida `gh`

```bash
gh pr status
gh pr view
gh pr create --title "..." --body-file pr-body.md
gh pr edit 123 --body-file pr-body.md
```
