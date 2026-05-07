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

// safeSend definido PRIMERO antes de todo
function safeSend(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(data)); } catch(e) {}
  }
}

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
  ws._role = null;

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'host-create') {
      const code = msg.code;
      rooms[code] = { hostWs: ws, clients: new Map(), open: true, ts: Date.now() };
      ws._id = 'host'; ws._room = code; ws._role = 'host';
      safeSend(ws, { type: 'host-ready', code });
      console.log(`[ROOM] Creada: ${code}`);
      return;
    }

    if (msg.type === 'host-ping') {
      const room = rooms[msg.code];
      if (room) room.ts = Date.now();
      return;
    }

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

    if (msg.type === 'host-to-client') {
      const room = rooms[ws._room];
      if (!room) return;
      if (msg.to === 'all') {
        room.clients.forEach(cws => safeSend(cws, msg.data));
      } else {
        const cws = room.clients.get(msg.to);
        if (cws) safeSend(cws, msg.data);
      }
      return;
    }

    if (msg.type === 'client-to-host') {
      const room = rooms[ws._room];
      if (!room) return;
      safeSend(room.hostWs, Object.assign({}, msg.data, { _from: ws._id }));
      return;
    }

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
