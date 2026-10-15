
'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const url = require('url');

const PORT = Number(process.env.PORT || 8080);
const MAX_PAYLOAD = 1024 * 1024;

const rooms = new Map();

const ALLOWED_TYPES = new Set([
  'offer',
  'answer',
  'candidate',
  'hangup',
  'device-info',
  'camera-request',
  'camera-stop',
  'screen-request',
  'screen-stop',
  'gallery-list-request',
  'contacts-list-request',
  'file-list-request',
  'permission-status',
  'resource-result',
  'error'
]);

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, {
      'content-type': 'text/plain; charset=utf-8'
    });
    return res.end('ScreenBridge signaling server is running');
  }

  res.writeHead(404, {
    'content-type': 'text/plain; charset=utf-8'
  });
  res.end('Not found');
});

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: MAX_PAYLOAD
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

function sendJson(ws, message) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function removeFromRoom(ws) {
  if (!ws.room) return;

  const roomName = ws.room;
  const members = rooms.get(roomName);

  members?.delete(ws);

  for (const peer of members || []) {
    sendJson(peer, {
      type: 'peer-left',
      role: ws.role
    });
  }

  if (members?.size === 0) {
    rooms.delete(roomName);
  }

  ws.room = null;
  ws.role = null;
}

wss.on('connection', ws => {
  ws.room = null;
  ws.role = null;

  ws.on('message', raw => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      sendJson(ws, {
        type: 'error',
        message: 'Mensagem JSON inválida.'
      });
      return;
    }

    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'join') {
      const room = String(msg.room || '').trim();
      const role = String(msg.role || '');

      if (!/^[a-zA-Z0-9_-]{3,80}$/.test(room)) {
        sendJson(ws, {
          type: 'error',
          message: 'Código de sala inválido.'
        });
        return;
      }

      if (role !== 'android' && role !== 'desktop') {
        sendJson(ws, {
          type: 'error',
          message: 'Tipo de dispositivo inválido.'
        });
        return;
      }

      removeFromRoom(ws);

      ws.room = room;
      ws.role = role;

      if (!rooms.has(room)) {
        rooms.set(room, new Set());
      }

      const members = rooms.get(room);
      members.add(ws);

      sendJson(ws, {
        type: 'joined',
        room,
        role,
        peers: members.size - 1
      });

      for (const peer of members) {
        if (peer === ws) continue;

        sendJson(peer, {
          type: 'peer-joined',
          role
        });

        sendJson(ws, {
          type: 'peer-present',
          role: peer.role
        });
      }

      return;
    }

    if (!ws.room) {
      sendJson(ws, {
        type: 'error',
        message: 'Entre em uma sala antes de enviar mensagens.'
      });
      return;
    }

    if (!ALLOWED_TYPES.has(msg.type)) {
      sendJson(ws, {
        type: 'error',
        message: 'Tipo de mensagem não permitido.'
      });
      return;
    }

    // Encaminha mensagens somente aos participantes da mesma sala.
    // O Android ainda precisa validar as permissões antes de acessar
    // câmera, tela, contatos, galeria ou arquivos.
    const members = rooms.get(ws.room);

    for (const peer of members || []) {
      if (peer !== ws) {
        sendJson(peer, msg);
      }
    }
  });

  ws.on('close', () => {
    removeFromRoom(ws);
  });

  ws.on('error', error => {
    console.error('Erro WebSocket:', error.message);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`ScreenBridge signaling listening on port ${PORT}`);
});
