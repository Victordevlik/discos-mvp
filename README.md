# Discos — MVP funcional

App para discotecas con sesiones nocturnas, acceso por QR, mesas activas y usuarios. Este MVP corre en Node.js sin framework (http nativo) con frontend en vanilla JS.

## Problema que resuelve

- Acceso sencillo por QR cada noche (sessionId + venueId).
- Visualización de mesas activas y personas disponibles.
- Interacción segura: invitaciones para bailar, consumos, llamados a mesero.
- Panel staff para operar la noche y Panel Admin minimal para gestionar créditos por venue.

## Funcionalidades principales (MVP)

- Servidor HTTP sin framework (Node.js nativo).
- Sesiones nocturnas por venue con control de créditos (persistencia en `/data/venues.json`).
- QR de acceso con `venueId` + `sessionId` y validez mientras la sesión esté activa.
- Tiempo real vía SSE (sin WebSockets), con cierre automático de conexiones inactivas.
- Perfil de usuario con selfie validada en backend (JPEG/WebP, ≤500KB).
- Órdenes y consumos, panel staff con analytics y gestión básica.
- Panel Admin aislado para sumar créditos por venue.

## Requisitos

- Node.js 18+ (probado con Node 24).
- Sin dependencias externas obligatorias (no Express, no DB).
- Sistema de archivos accesible para persistencia en `/data`.

## Cómo correr localmente

1. Clonar el repositorio y entrar a la carpeta.
2. Crear `.env` tomando como referencia `.env.example` (no comites `.env`).
3. Ejecutar:

```bash
node server.js
```

4. Abrir en el navegador: `http://localhost:${PORT}/` (por defecto `3000`).

### Panel Staff

- Acceso: `/?staff=1` (requerirá PIN de sesión).

### Panel Admin (créditos por venue)

- Acceso directo: `/admin.html` (requiere `ADMIN_SECRET`).
- Endpoints: `GET /api/admin/venues`, `POST /api/admin/venues/credit`.

## Variables de entorno

- `PORT`: Puerto de escucha (por defecto `3000`).
- `STAFF_PIN`: PIN global opcional para staff.
- `ADMIN_SECRET`: Clave para el Panel Admin.

## Deploy en Railway (sugerido)

- Comando de inicio: `node server.js`
- Variables de entorno: configurar `PORT`, `ADMIN_SECRET`, `STAFF_PIN` según tu entorno.
- Persistencia: el MVP usa archivos en `/data`. En Railway, usa volúmenes o estrategias acordes si deseas persistencia.

## Nota

MVP en evolución. Arquitectura simple y sin refactors complejos por diseño.

