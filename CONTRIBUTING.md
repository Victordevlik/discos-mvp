# Convenciones de commits

- Formato: `tipo(alcance)!: resumen`
- Resumen en imperativo, claro y sin punto final, ≤ 72 caracteres
- Cuerpo opcional con el qué y el porqué; pie para referencias y notas
- Usa español en el resumen y el cuerpo

## Tipos permitidos

- feat: nueva funcionalidad
- fix: corrección de bug
- docs: documentación
- style: cambios de formato (sin lógica)
- refactor: reestructuración interna (sin cambiar comportamiento)
- perf: rendimiento
- test: pruebas
- build: cambios de build o dependencias
- ci: integración continua
- chore: tareas menores (limpieza, reubicación, sin lógica)
- revert: revertir un commit anterior

## Alcance (scope)

- Módulo o zona del sistema: staff, waiter, mesas, usuarios, perfil, analytics, panel, catálogo, reportes
- Ejemplo: `feat(staff): menú vertical con submenú Más`

## Reglas

- Línea 1: `tipo(alcance)!: resumen`
- Línea 2: en blanco
- Cuerpo: contexto, motivo y detalles relevantes
- Pie: referencias (Closes #123), coautores y notas
- Breaking change: agrega `!` en el encabezado y un bloque `BREAKING CHANGE: …` en el cuerpo

## Ejemplos

- `fix(waiter): enviar solicitud al seleccionar motivo`
- `feat(staff): botón Analytics como toggle`
- `docs: guía de convenciones de commits`
- `refactor(panel): centrar tarjetas y simplificar estilos`
- `perf(orders): paginar resultados en staff`
- `revert: revertir feat(staff): menú vertical`

## Plantilla de mensaje

- Se provee `.gitmessage.txt` en la raíz del repo para agilizar commits
- Configura tu git local:

```
git config commit.template .gitmessage.txt
```

## Buenas prácticas

- Resume la intención en el encabezado
- Explica en el cuerpo el porqué (no solo el qué)
- Añade cómo validar cuando aplique (pasos o comandos)
- Evita commits mixtos de muchas áreas; prefiere cambios razonables y coherentes

