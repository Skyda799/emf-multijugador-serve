'use strict';

const http = require('http');
const WebSocket = require('ws');
const { WebSocketServer } = WebSocket;

const PORT = Number(process.env.PORT || 8080);
const HOST = '0.0.0.0';
const ROOM_TTL_MS = 30 * 60 * 1000;

const rooms = new Map();
const clients = new Set();

function cleanText(value, max = 80) {
  return String(value ?? '')
    .replace(/[<>\u0000-\u001F]/g, '')
    .trim()
    .slice(0, max);
}

function cleanRoomId(value) {
  return cleanText(value, 24)
    .toUpperCase()
    .replace(/[^A-Z0-9_-]/g, '');
}

function send(ws, message) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  try {
    ws.send(JSON.stringify(message));
  } catch (_) {}
}

function publicRooms() {
  const now = Date.now();
  return [...rooms.values()]
    .filter(room => room.host && now - room.lastSeen < ROOM_TTL_MS)
    .map(room => ({
      id: room.id,
      name: room.name,
      game: room.game,
      players: room.players.size,
      maxPlayers: 2,
      createdAt: room.createdAt
    }));
}

function sendRoomList(ws) {
  send(ws, { t: 'rooms', rooms: publicRooms() });
}

function broadcastRoomList() {
  const message = { t: 'rooms', rooms: publicRooms() };
  for (const ws of clients) {
    if (ws.room === null) send(ws, message);
  }
}

function broadcastToRoom(room, message, except = null) {
  for (const ws of room.players) {
    if (ws !== except) send(ws, message);
  }
}

function removeFromRoom(ws) {
  const room = ws.room;
  if (!room) return;

  room.players.delete(ws);
  ws.room = null;
  room.lastSeen = Date.now();

  if (room.players.size === 0) {
    rooms.delete(room.id);
  } else {
    broadcastToRoom(room, { t: 'player-left' });
  }

  broadcastRoomList();
}

const server = http.createServer((req, res) => {
  const origin = req.headers.origin || '*';
  res.setHeader('Access-Control-Allow-Origin', origin);
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);

  if (url.pathname === '/health') {
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'no-store'
    });
    return res.end(JSON.stringify({
      ok: true,
      service: 'EMF Public Rooms',
      rooms: publicRooms().length
    }));
  }

  if (url.pathname === '/rooms') {
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'no-store'
    });
    return res.end(JSON.stringify({ rooms: publicRooms() }));
  }

  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('EMF Public Rooms Server online');
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  let url;
  try {
    url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  } catch (_) {
    socket.destroy();
    return;
  }

  if (url.pathname !== '/ws') {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, ws => {
    wss.emit('connection', ws, req);
  });
});

wss.on('connection', ws => {
  clients.add(ws);
  ws.room = null;

  send(ws, {
    t: 'welcome',
    server: 'emf-public-rooms',
    version: 1
  });
  sendRoomList(ws);

  ws.on('message', raw => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch (_) {
      return;
    }

    if (!data || typeof data.t !== 'string') return;

    if (data.t === 'list-rooms') {
      sendRoomList(ws);
      return;
    }

    if (data.t === 'create') {
      if (ws.room) {
        send(ws, { t: 'error', code: 'already-in-room' });
        return;
      }

      const id = cleanRoomId(data.room);
      if (!id) {
        send(ws, { t: 'error', code: 'invalid-room' });
        return;
      }

      if (rooms.has(id)) {
        send(ws, { t: 'error', code: 'room-exists' });
        return;
      }

      const room = {
        id,
        name: cleanText(data.name, 40) || 'Sala EMF',
        game: cleanText(data.game, 80) || 'NES',
        host: ws,
        players: new Set([ws]),
        createdAt: Date.now(),
        lastSeen: Date.now()
      };

      rooms.set(id, room);
      ws.room = room;

      send(ws, { t: 'created', room: id, role: 1 });
      broadcastRoomList();
      return;
    }

    if (data.t === 'join') {
      if (ws.room) {
        send(ws, { t: 'error', code: 'already-in-room' });
        return;
      }

      const id = cleanRoomId(data.room);
      const room = rooms.get(id);

      if (!room) {
        send(ws, { t: 'error', code: 'room-not-found' });
        return;
      }

      if (room.players.size >= 2) {
        send(ws, { t: 'error', code: 'room-full' });
        return;
      }

      ws.room = room;
      room.players.add(ws);
      room.lastSeen = Date.now();

      send(ws, {
        t: 'joined',
        room: id,
        role: 2,
        game: room.game
      });

      send(room.host, {
        t: 'player-joined',
        game: room.game
      });

      broadcastRoomList();
      return;
    }

    if (data.t === 'leave') {
      removeFromRoom(ws);
      return;
    }

    if (data.t === 'ping') {
      send(ws, { t: 'pong', at: data.at });
      return;
    }

    // Everything else is part of the existing EMF game protocol.
    // Relay it without changing the payload: input, i2, start, start2,
    // auth-state, ROM transfer messages, ready messages, etc.
    if (ws.room) {
      ws.room.lastSeen = Date.now();
      broadcastToRoom(ws.room, data, ws);
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    removeFromRoom(ws);
  });

  ws.on('error', () => {});
});

setInterval(() => {
  const now = Date.now();

  for (const [id, room] of rooms) {
    if (room.players.size === 0 || now - room.lastSeen > ROOM_TTL_MS) {
      for (const ws of room.players) {
        try { ws.close(); } catch (_) {}
        ws.room = null;
      }
      rooms.delete(id);
    }
  }

  broadcastRoomList();
}, 60_000).unref();

server.listen(PORT, HOST, () => {
  console.log(`EMF Public Rooms Server listening on ${HOST}:${PORT}`);
});
