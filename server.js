// オペレーション・ダンラン 中継サーバー（依存パッケージなし / Node.js 18+）
//   node server.js            → http://localhost:8080 でゲームを配信し、/ws で部屋の通信を中継します
//   PORT=3000 node server.js  → ポート変更
// 部屋の中身（ゲームの進行）はホストの画面が計算し、このサーバーは各プレイヤーの状態を同じ部屋の全員に配るだけです。
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 8080;
const MAX_FRAME = 16 * 1024;        // 1メッセージの上限
const MAX_PRESENCE = 4096;          // プレイヤー1人分の状態の上限（ゲーム側と同じ）
const MAX_ROOM = 8;                 // 1部屋の接続上限（プレイヤー4人＋待機）
const MAX_MSGS_PER_SEC = 80;        // 1接続あたりの送信上限
const ROOM_RE = /^danran-[a-z0-9]{4}$/;
const INDEX = path.join(__dirname, 'index.html');

/* ---------------- HTTP: ゲーム本体の配信 ---------------- */
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/healthz') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok'); return; }
  if (url === '/' || url === '/index.html') {
    fs.readFile(INDEX, (err, buf) => {
      if (err) { res.writeHead(500); res.end('index.html が見つかりません'); return; }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache', 'x-content-type-options': 'nosniff' });
      res.end(buf);
    });
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); res.end('not found');
});

/* ---------------- WebSocket（RFC 6455 の最小実装） ---------------- */
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function frame(op, payload) {
  const p = Buffer.isBuffer(payload) ? payload : Buffer.from(payload || '');
  let h;
  if (p.length < 126) { h = Buffer.from([0x80 | op, p.length]); }
  else if (p.length < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(p.length, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(p.length), 2); }
  return Buffer.concat([h, p]);
}

class Client {
  constructor(sock) {
    this.sock = sock; this.id = crypto.randomBytes(8).toString('hex');
    this.buf = Buffer.alloc(0); this.frag = null; this.rooms = new Set();
    this.alive = true; this.closed = false; this.win = Date.now(); this.count = 0;
    sock.setNoDelay(true);
    sock.on('data', d => this.onData(d));
    sock.on('close', () => this.cleanup());
    sock.on('error', () => this.cleanup());
    this.send({ t: 'you', peer: this.id });
  }
  send(obj) { if (!this.closed && !this.sock.destroyed) this.sock.write(frame(1, JSON.stringify(obj))); }
  close() { if (this.closed) return; try { this.sock.write(frame(8, Buffer.alloc(0))); } catch (e) {} this.sock.end(); this.cleanup(); }
  onData(d) {
    this.alive = true;
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    for (;;) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1], fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; const big = this.buf.readBigUInt64BE(2); if (big > BigInt(MAX_FRAME)) { this.close(); return; } len = Number(big); off = 10; }
      if (len > MAX_FRAME) { this.close(); return; }
      const need = off + (masked ? 4 : 0) + len;
      if (this.buf.length < need) return;
      let payload = this.buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) { const m = this.buf.subarray(off, off + 4); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
      this.buf = this.buf.subarray(need);
      if (op === 0x8) { this.close(); return; }
      if (op === 0x9) { if (!this.sock.destroyed) this.sock.write(frame(0xA, payload)); continue; }
      if (op === 0xA) { this.alive = true; continue; }
      if (op === 0x1 || op === 0x0) {
        if (op === 0x1) this.frag = [payload]; else if (this.frag) this.frag.push(payload); else continue;
        const total = this.frag.reduce((s, b) => s + b.length, 0);
        if (total > MAX_FRAME) { this.close(); return; }
        if (fin) { const text = Buffer.concat(this.frag).toString('utf8'); this.frag = null; this.onMessage(text); }
      }
    }
  }
  onMessage(text) {
    const now = Date.now();
    if (now - this.win > 1000) { this.win = now; this.count = 0; }
    if (++this.count > MAX_MSGS_PER_SEC) return;
    let m; try { m = JSON.parse(text); } catch (e) { return; }
    if (!m || typeof m !== 'object' || typeof m.room !== 'string' || !ROOM_RE.test(m.room)) return;
    if (m.t === 'join') joinRoom(this, m.room, !!m.host);
    else if (m.t === 'leave') leaveRoom(this, m.room);
    else if (m.t === 'p') setPresence(this, m.room, m.presence);
  }
  cleanup() {
    if (this.closed) return; this.closed = true;
    for (const r of [...this.rooms]) leaveRoom(this, r);
    clients.delete(this);
    if (!this.sock.destroyed) this.sock.destroy();
  }
}

/* ---------------- 部屋 ---------------- */
const rooms = new Map();          // name -> Map(clientId -> {client, presence})
const clients = new Set();
function joinRoom(c, name, asHost) {
  let room = rooms.get(name);
  if (asHost && room && room.size) { c.send({ t: 'err', room: name, code: 'taken' }); return; }
  if (!asHost && (!room || !room.size)) { c.send({ t: 'err', room: name, code: 'nohost' }); return; }
  if (room && room.size >= MAX_ROOM) { c.send({ t: 'err', room: name, code: 'full' }); return; }
  if (!room) { room = new Map(); rooms.set(name, room); }
  room.set(c.id, { client: c, presence: {} });
  c.rooms.add(name);
  c.send({ t: 'joined', room: name });
  for (const [id, m] of room) if (id !== c.id) c.send({ t: 'p', room: name, peer: id, presence: m.presence });
}
function leaveRoom(c, name) {
  const room = rooms.get(name); c.rooms.delete(name);
  if (!room || !room.delete(c.id)) return;
  for (const m of room.values()) m.client.send({ t: 'bye', room: name, peer: c.id });
  if (!room.size) rooms.delete(name);
}
function setPresence(c, name, presence) {
  const room = rooms.get(name), me = room && room.get(c.id);
  if (!me || !presence || typeof presence !== 'object' || Array.isArray(presence)) return;
  const text = JSON.stringify(presence);
  if (Buffer.byteLength(text) > MAX_PRESENCE) return;
  me.presence = presence;
  const out = `{"t":"p","room":${JSON.stringify(name)},"peer":"${c.id}","presence":${text}}`;
  for (const [id, m] of room) if (id !== c.id && !m.client.closed && !m.client.sock.destroyed) m.client.sock.write(frame(1, out));
}

server.on('upgrade', (req, sock) => {
  const url = (req.url || '').split('?')[0];
  const key = req.headers['sec-websocket-key'];
  if (url !== '/ws' || !key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { sock.end('HTTP/1.1 400 Bad Request\r\n\r\n'); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  clients.add(new Client(sock));
});

// 切れた接続の掃除と、ホスティング先のアイドル切断を防ぐ ping
setInterval(() => {
  for (const c of clients) {
    if (!c.alive) { c.cleanup(); continue; }
    c.alive = false;
    if (!c.sock.destroyed) c.sock.write(frame(0x9, Buffer.alloc(0)));
  }
}, 25000);

server.listen(PORT, () => {
  console.log(`オペレーション・ダンラン: http://localhost:${PORT} で起動しました（部屋の中継は /ws）`);
});
