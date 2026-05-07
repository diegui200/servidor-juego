// Trivia Racer — Servidor WebSocket relay
// Deploy gratis en Railway.app, Render.com o Glitch.com
// ─────────────────────────────────────────────────────
// Railway: crea proyecto → "Deploy from GitHub" → sube este archivo + package.json
// Render:  New Web Service → sube repo → Build: npm install | Start: node server.js
// Glitch:  glitch.com → New Project → import desde GitHub o pega este código
// ─────────────────────────────────────────────────────

const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;
const wss  = new WebSocket.Server({ port: PORT });

// rooms[code] = { hostWs, clients: Map<id, ws>, open: bool, ts: number }
const rooms = {};

const ROOM_TTL = 4 * 60 * 60 * 1000; // 4 horas

// Limpiar salas muertas cada 30 minutos
setInterval(() => {
  const now = Date.now();
  for (const [code, room] of Object.entries(rooms)) {
    if (now - room.ts > ROOM_TTL) {
      delete rooms[code];
      console.log(`[CLEAN] Sala ${code} eliminada por TTL`);
    }
  }
}, 30 * 60 * 1000);

wss.on('connection', ws => {
  ws._id   = null;
  ws._room = null;
  ws._role = null; // 'host' | 'client'

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // ── HOST crea sala ─────────────────────────────────
    if (msg.type === 'host-create') {
      const code = msg.code;
      rooms[code] = { hostWs: ws, clients: new Map(), open: true, ts: Date.now() };
      ws._id   = 'host';
      ws._room = code;
      ws._role = 'host';
      ws.send(JSON.stringify({ type: 'host-ready', code }));
      console.log(`[ROOM] Creada: ${code}`);
      return;
    }

    // ── HOST heartbeat ─────────────────────────────────
    if (msg.type === 'host-ping') {
      const room = rooms[msg.code];
      if (room) room.ts = Date.now();
      return;
    }

    // ── CLIENT se une ──────────────────────────────────
    if (msg.type === 'client-join') {
      const { code, clientId } = msg;
      const room = rooms[code];
      if (!room || !room.open) {
        ws.send(JSON.stringify({ type: 'error', reason: 'Sala no encontrada o cerrada' }));
        return;
      }
      room.clients.set(clientId, ws);
      ws._id   = clientId;
      ws._room = code;
      ws._role = 'client';
      // Avisar al host que llegó un cliente nuevo
      safeSend(room.hostWs, { type: 'client-connected', clientId });
      console.log(`[JOIN] ${clientId} → sala ${code}`);
      return;
    }

    // ── Relay: HOST → uno o todos los clientes ─────────
    if (msg.type === 'host-to-client') {
      const room = rooms[ws._room];
      if (!room) return;
      if (msg.to === 'all') {
        // Broadcast a todos los clientes
        room.clients.forEach(cws => safeSend(cws, msg.data));
      } else {
        // A un cliente específico
        const cws = room.clients.get(msg.to);
        if (cws) safeSend(cws, msg.data);
      }
      return;
    }

    // ── Relay: CLIENT → HOST ───────────────────────────
    if (msg.type === 'client-to-host') {
      const room = rooms[ws._room];
      if (!room) return;
      // Añadir el id del remitente al payload
      const payload = Object.assign({}, msg.data, { _from: ws._id });
      safeSend(room.hostWs, payload);
      return;
    }

    // ── HOST cierra sala ───────────────────────────────
    if (msg.type === 'host-close') {
      const room = rooms[ws._room];
      if (!room) return;
      room.open = false;
      room.clients.forEach(cws =>
        safeSend(cws, { type: 'back-to-lobby' })
      );
      delete rooms[ws._room];
      return;
    }
  });

  ws.on('close', () => {
    const room = rooms[ws._room];
    if (!room) return;

    if (ws._role === 'host') {
      // Host se fue — avisarle a los clientes
      room.clients.forEach(cws =>
        safeSend(cws, { type: 'back-to-lobby' })
      );
      delete rooms[ws._room];
      console.log(`[CLOSE] Host cerró sala ${ws._room}`);
    } else if (ws._role === 'client') {
      room.clients.delete(ws._id);
      safeSend(room.hostWs, { type: 'client-disconnected', clientId: ws._id });
      console.log(`[LEAVE] ${ws._id} salió de sala ${ws._room}`);
    }
  });

  ws.on('error', err => console.warn('[WS ERROR]', err.message));
});

function safeSend(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(data)); } catch {}
  }
}

console.log(`🏎  Trivia Racer server corriendo en puerto ${PORT}`);
