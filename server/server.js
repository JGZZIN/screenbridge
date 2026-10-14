
'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const url = require('url');

const PORT = Number(process.env.PORT || 8080);

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'content-type': 'text/plain' });
    return res.end('ok');
  }

  res.writeHead(404);
  res.end('not found');
});

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: 1024 * 1024
});

server.on('upgrade', (req, socket, head) => {
  if (url.parse(req.url).pathname !== '/ws') {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, ws => {
    wss.emit('connection', ws, req);
  });
});

const rooms = new Map();

wss.on('connection', ws => {
  ws.room = null;

  ws.on('message', raw => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === 'join') {
      const room = String(msg.room || '').trim().slice(0, 80);

      if (!/^[a-zA-Z0-9_-]{3,80}$/.test(room)) {
        ws.send(JSON.stringify({
          type: 'error',
          message: 'Código de sala inválido.'
        }));
        return;
      }

      if (ws.room) {
        rooms.get(ws.room)?.delete(ws);
      }

      ws.room = room;

      if (!rooms.has(room)) {
        rooms.set(room, new Set());
      }

      const members = rooms.get(room);
      members.add(ws);

      ws.send(JSON.stringify({
        type: 'joined',
        room,
        peers: members.size - 1
      }));

      for (const peer of members) {
        if (peer !== ws && peer.readyState === WebSocket.OPEN) {
          peer.send(JSON.stringify({ type: 'peer-joined' }));
        }
      }

      return;
    }

    if (!ws.room) return;

    if (!['offer', 'answer', 'candidate', 'hangup'].includes(msg.type)) {
      return;
    }

    const message = JSON.stringify(msg);

    for (const peer of rooms.get(ws.room) || []) {
      if (peer !== ws && peer.readyState === WebSocket.OPEN) {
        peer.send(message);
      }
    }
  });

  ws.on('close', () => {
    if (!ws.room) return;

    const members = rooms.get(ws.room);
    members?.delete(ws);

    for (const peer of members || []) {
      if (peer.readyState === WebSocket.OPEN) {
        peer.send(JSON.stringify({ type: 'peer-left' }));
      }
    }

    if (members?.size === 0) {
      rooms.delete(ws.room);
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`ScreenBridge signaling listening on port ${PORT}`);
});
