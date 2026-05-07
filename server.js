const http      = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Trivia Racer server OK');
});

const wss = new WebSocket.Server({ server });
const rooms = {};
const ROOM_TTL = 4 * 60 * 60 * 1000;

function safeSend(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(data)); } catch(e) {}
  }
}

// Relay raw string sin re-parsear (mucho más rápido para estados de juego)
function safeRelay(ws, raw) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(raw); } catch(e) {}
  }
}

setInterval(() => {
  const now = Date.now();
  for (const [code, room] of Object.entries(rooms)) {
    if (now - room.ts > ROOM_TTL) {
      delete rooms[code];
      console.log(`[CLEAN] Sala ${code} eliminada`);
    }
  }
}, 30 * 60 * 1000);

wss.on('connection', ws => {
  ws._id   = null;
  ws._room = null;
  ws._role = null;

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // HOST crea sala
    if (msg.type === 'host-create') {
      const code = msg.code;
      rooms[code] = { hostWs: ws, clients: new Map(), open: true, ts: Date.now() };
      ws._id = 'host'; ws._room = code; ws._role = 'host';
      safeSend(ws, { type: 'host-ready', code });
      console.log(`[ROOM] Creada: ${code}`);
      return;
    }

    // HOST heartbeat
    if (msg.type === 'host-ping') {
      const room = rooms[msg.code];
      if (room) room.ts = Date.now();
      return;
    }

    // CLIENT se une
    if (msg.type === 'client-join') {
      const { code, clientId } = msg;
      const room = rooms[code];
      if (!room || !room.open) {
        safeSend(ws, { type: 'error', reason: 'Sala no encontrada o cerrada' });
        return;
      }
      room.clients.set(clientId, ws);
      ws._id = clientId; ws._room = code; ws._role = 'client';
      safeSend(room.hostWs, { type: 'client-connected', clientId });
      console.log(`[JOIN] ${clientId} → sala ${code}`);
      return;
    }

    // HOST → clientes: relay
    if (msg.type === 'host-to-client') {
      const room = rooms[ws._room];
      if (!room) return;
      // Para broadcasts de estado de juego, relay raw del data directamente
      const dataStr = JSON.stringify(msg.data);
      if (msg.to === 'all') {
        room.clients.forEach(cws => safeRelay(cws, dataStr));
      } else {
        const cws = room.clients.get(msg.to);
        if (cws) safeRelay(cws, dataStr);
      }
      return;
    }

    // CLIENT → HOST: relay con _from
    if (msg.type === 'client-to-host') {
      const room = rooms[ws._room];
      if (!room) return;
      // Para estados de juego (t:'s'), relay más eficiente
      const d = msg.data;
      if (d && d.t === 's') {
        // Añadir _from inline sin crear objeto nuevo
        safeRelay(room.hostWs, JSON.stringify(
          Object.assign({}, d, { _from: ws._id })
        ));
      } else {
        safeSend(room.hostWs, Object.assign({}, d, { _from: ws._id }));
      }
      return;
    }

    // HOST cierra sala
    if (msg.type === 'host-close') {
      const room = rooms[ws._room];
      if (!room) return;
      room.open = false;
      room.clients.forEach(cws => safeSend(cws, { type: 'back-to-lobby' }));
      delete rooms[ws._room];
      return;
    }
  });

  ws.on('close', () => {
    const room = rooms[ws._room];
    if (!room) return;
    if (ws._role === 'host') {
      room.clients.forEach(cws => safeSend(cws, { type: 'back-to-lobby' }));
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

server.listen(PORT, () => {
  console.log(`🏎  Trivia Racer server corriendo en puerto ${PORT}`);
});
