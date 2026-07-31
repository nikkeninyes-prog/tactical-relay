const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;

// room code → { hostWs, players: Map<playerId, ws> }
const rooms = new Map();

// ws → { roomCode, playerId, isHost }
const clients = new Map();

// เสิร์ฟไฟล์เกม (HTML เดียว) จากโฟลเดอร์เดียวกับ server.js — โหลดเข้า memory ครั้งเดียวตอนบูต
const GAME_FILE = path.join(__dirname, 'index.html');
let gameHtml = null;
try { gameHtml = fs.readFileSync(GAME_FILE); } catch (e) { console.warn('ไม่พบไฟล์เกม:', GAME_FILE); }

const server = http.createServer((req, res) => {
  // Health check endpoint (Railway/Render ใช้ตรวจว่า server ยัง alive)
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', rooms: rooms.size, clients: clients.size }));
    return;
  }
  // เสิร์ฟตัวเกมเองที่ root — ไม่ต้องพึ่ง host แยกแบบ Netlify อีกต่อไป
  if (gameHtml && (req.url === '/' || req.url.startsWith('/?'))) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(gameHtml);
    return;
  }
  res.writeHead(gameHtml ? 404 : 200, {
    'Content-Type': 'text/plain',
    'Access-Control-Allow-Origin': '*'
  });
  res.end(gameHtml ? 'Not found\n' : 'Tactical Shooter Relay Server\n');
});

const wss = new WebSocketServer({ server });

function genPlayerId() {
  return Math.random().toString(36).slice(2, 10);
}

function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(data)); } catch (e) {}
  }
}

function broadcast(room, data, excludeId = null) {
  if (!room) return;
  room.players.forEach((ws, pid) => {
    if (pid !== excludeId) send(ws, data);
  });
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    const { type } = msg;

    // ── สร้างห้อง (host) ─────────────────────────────
    if (type === 'create') {
      const roomCode = msg.roomCode || Math.random().toString(36).slice(2, 8).toUpperCase();
      const playerId = genPlayerId();

      if (rooms.has(roomCode)) {
        send(ws, { type: 'error', reason: 'Room code already exists' });
        return;
      }

      const hostName = (msg.hostName || 'Player').toString().slice(0, 20);
      const room = { hostId: playerId, players: new Map([[playerId, ws]]), hostName, started: false };
      rooms.set(roomCode, room);
      clients.set(ws, { roomCode, playerId, isHost: true });

      send(ws, { type: 'created', roomCode, playerId });
      console.log(`[CREATE] room=${roomCode} host=${playerId}`);
    }

    // ── บอกว่าห้องเริ่มแมทช์แล้ว (ไม่ต้องโชว์ในลิสห้องสาธารณะอีก) ──
    else if (type === 'matchStarted') {
      const info = clients.get(ws);
      if (!info || !info.isHost) return;
      const room = rooms.get(info.roomCode);
      if (room) room.started = true;
    }

    // ── ขอรายชื่อห้องที่เปิดอยู่ (ยังไม่เริ่มแมทช์) ──────────
    else if (type === 'listRooms') {
      const list = [];
      rooms.forEach((room, code) => {
        if (!room.started) list.push({ code, hostName: room.hostName, players: room.players.size });
      });
      send(ws, { type: 'roomList', rooms: list });
    }

    // ── เข้าร่วมห้อง (client) ────────────────────────
    else if (type === 'join') {
      const roomCode = (msg.roomCode || '').toUpperCase();
      const room = rooms.get(roomCode);

      if (!room) {
        send(ws, { type: 'error', reason: 'Room not found' });
        return;
      }
      if (room.players.size >= 10) {
        send(ws, { type: 'error', reason: 'Room is full' });
        return;
      }

      const playerId = genPlayerId();
      room.players.set(playerId, ws);
      clients.set(ws, { roomCode, playerId, isHost: false });

      // แจ้ง client ว่าเข้าได้แล้ว
      send(ws, { type: 'joined', roomCode, playerId, hostId: room.hostId });

      // แจ้ง host ว่ามีคนใหม่
      const hostWs = room.players.get(room.hostId);
      send(hostWs, { type: 'playerJoined', playerId, nickname: msg.nickname || 'Player' });

      console.log(`[JOIN]   room=${roomCode} player=${playerId} total=${room.players.size}`);
    }

    // ── ส่งข้อมูลไปทุกคนในห้อง (broadcast) ──────────
    else if (type === 'broadcast') {
      const info = clients.get(ws);
      if (!info) return;
      const room = rooms.get(info.roomCode);
      if (!room) return;

      // relay ไปทุกคนในห้องยกเว้นตัวเอง
      broadcast(room, {
        type: 'relay',
        from: info.playerId,
        data: msg.data
      }, info.playerId);
    }

    // ── ส่งข้อมูลหา player คนเดียว (direct) ─────────
    else if (type === 'direct') {
      const info = clients.get(ws);
      if (!info) return;
      const room = rooms.get(info.roomCode);
      if (!room) return;

      const targetWs = room.players.get(msg.to);
      if (!targetWs) return;

      send(targetWs, {
        type: 'relay',
        from: info.playerId,
        data: msg.data
      });
    }

    // ── Ping/Pong latency check ───────────────────────
    else if (type === 'ping') {
      send(ws, { type: 'pong', t: msg.t });
    }
  });

  ws.on('close', () => {
    const info = clients.get(ws);
    if (!info) return;

    const { roomCode, playerId, isHost } = info;
    const room = rooms.get(roomCode);
    clients.delete(ws);

    if (!room) return;
    room.players.delete(playerId);

    if (isHost || room.players.size === 0) {
      // Host ออก → ปิดห้อง แจ้งทุกคน
      broadcast(room, { type: 'hostLeft', reason: 'Host disconnected' });
      room.players.forEach((w) => { try { w.close(); } catch {} });
      rooms.delete(roomCode);
      console.log(`[CLOSE]  room=${roomCode} deleted (host left or empty)`);
    } else {
      // Player ทั่วไปออก → แจ้งคนอื่น
      broadcast(room, { type: 'playerLeft', playerId });
      console.log(`[LEAVE]  room=${roomCode} player=${playerId} remaining=${room.players.size}`);
    }
  });

  ws.on('error', (e) => console.warn('ws error:', e.message));
});

// Heartbeat: kick dead connections ทุก 30 วิ
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, () => {
  console.log(`Relay server running on port ${PORT}`);
});
