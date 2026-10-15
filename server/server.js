
'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

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

// Servidor HTTP: página inicial e verificação de saúde.
const server = http.createServer((req, res) => {
  const pathname = new URL(
    req.url || '/',
    'http://localhost'
  ).pathname;

  if (pathname === '/' || pathname === '/health') {
    res.writeHead(200, {
      'content-type': 'text/plain; charset=utf-8',
      'cache-control': 'no-store'
    });

    res.end('ScreenBridge signaling server is running');
    return;
  }

  res.writeHead(404, {
    'content-type': 'text/plain; charset=utf-8'
  });

  res.end('Not found');
});

// WebSocket: usado para trocar mensagens entre os dispositivos.
const wss = new WebSocketServer({
  noServer: true,
  maxPayload: MAX_PAYLOAD
});

// Aceita conexões WebSocket somente no caminho /ws.
server.on('upgrade', (req, socket, head) => {
  let pathname;

  try {
    pathname = new URL(
      req.url || '/',
      'http://localhost'
    ).pathname;
  } catch {
    socket.destroy();
    return;
  }

  if (pathname !== '/ws') {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, ws => {
    wss.emit('connection', ws, req);
  });
});

// Envia JSON somente para conexões abertas.
function sendJson(ws, message) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

// Remove um dispositivo da sala quando desconecta
// ou entra em outra sala.
function removeFromRoom(ws) {
  if (!ws.room) return;

  const roomName = ws.room;
  const members = rooms.get(roomName);

  if (members) {
    members.delete(ws);

    for (const peer of members) {
      sendJson(peer, {
        type: 'peer-left',
        role: ws.role
      });
    }

    if (members.size === 0) {
      rooms.delete(roomName);
    }
  }

  ws.room = null;
  ws.role = null;
}

// Nova conexão WebSocket.
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

    if (!msg || typeof msg.type !== 'string') {
      return;
    }

    // Entrada em uma sala.
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

      // Evita manter a mesma conexão em duas salas.
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

      // Informa os participantes existentes sobre o novo dispositivo.
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

    // Encaminha mensagens somente aos outros participantes
    // da mesma sala. Não executa comandos no dispositivo.
    const members = rooms.get(ws.room);

    if (!members) return;

    for (const peer of members) {
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

// Inicia o servidor na porta fornecida pelo Render.
server.listen(PORT, '0.0.0.0', () => {
  console.log(
    `ScreenBridge signaling listening on port ${PORT}`
  );
});

server.on('error', error => {
  console.error('Erro no servidor HTTP:', error.message);
});
