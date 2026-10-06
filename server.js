#!/usr/bin/env node
/* ============================================================================
 * 中国象棋服务器 —— serv00 (Node.js) 移植版
 * 由 Cloudflare Worker 版 (index.js) 1:1 移植：
 *   - WebSocketPair          -> 内置 RFC6455 实现（零依赖）
 *   - Durable Object 房间    -> 进程内 Map<roomId, ChessRoom>（逻辑原样保留）
 *   - D1 CHESS_DB            -> JSON 文件存储 data/chess-store.json（原子写入）
 *   - env.ASSETS 静态托管    -> dist/ 静态文件服务
 * 对外协议（WS 事件名、/api 路径与返回结构、静态资源路径）与 Worker 版完全一致，
 * 前端文件零改动即可运行。
 * 运行：PORT=8080 node server.js   （PORT 以 serv00 分配端口为准）
 * ==========================================================================*/
'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// 端口优先级：环境变量 PORT > 同目录 port.txt（serv00 devil 分配的端口）> 8080
let PORT = parseInt(process.env.PORT || '', 10);
if (!PORT || PORT < 1 || PORT > 65535) {
  try {
    const pf = path.join(__dirname, 'port.txt');
    if (fs.existsSync(pf)) {
      const p = parseInt(fs.readFileSync(pf, 'utf8').trim(), 10);
      if (p >= 1 && p <= 65535) PORT = p;
    }
  } catch (e) {}
}
if (!PORT || PORT < 1 || PORT > 65535) PORT = 8080;
const HOST = process.env.HOST || '0.0.0.0';
const DIST_DIR = path.join(__dirname, 'dist');
const DATA_DIR = process.env.XQ_DATA_DIR || path.join(__dirname, 'data');
const STORE_FILE = path.join(DATA_DIR, 'chess-store.json');

/* ==========================================================================
 * JSON 文件存储（等价 D1 的五张表）
 * ========================================================================*/
const store = {
  data: {
    room_state: {},      // roomId -> { state:{...}, updated_at }
    rooms_registry: {},  // id -> { id, red, black, status, watchers, updated_at }
    ai_weights: null,    // { id:1, weights:"<json str>", stats:"<json str>", updated_at }
    online_now: {},      // cid -> ts
    race_report: []      // 最近 2000 条
  },
  _timer: null,
  load() {
    try {
      const raw = fs.readFileSync(STORE_FILE, 'utf8');
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        this.data.room_state = parsed.room_state || {};
        this.data.rooms_registry = parsed.rooms_registry || {};
        this.data.ai_weights = parsed.ai_weights || null;
        this.data.online_now = parsed.online_now || {};
        this.data.race_report = Array.isArray(parsed.race_report) ? parsed.race_report : [];
      }
    } catch (e) { /* 首次运行无文件，空数据启动 */ }
  },
  scheduleSave() {
    if (this._timer) return;
    this._timer = setTimeout(() => { this._timer = null; this.flush(); }, 300);
  },
  flush() {
    try {
      if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
      const tmp = STORE_FILE + '.tmp';
      fs.writeFileSync(tmp, JSON.stringify(this.data));
      fs.renameSync(tmp, STORE_FILE);
    } catch (e) { console.error('store flush error:', e && e.message); }
  },
  /* ---- room_state ---- */
  getRoomState(roomId) { const r = this.data.room_state[roomId]; return r ? r.state : null; },
  saveRoomState(roomId, stateObj) {
    this.data.room_state[roomId] = { state: stateObj, updated_at: Date.now() };
    this.scheduleSave();
  },
  deleteRoomState(roomId) {
    if (this.data.room_state[roomId]) { delete this.data.room_state[roomId]; this.scheduleSave(); }
  },
  pruneRoomState(maxAge) {
    const cutoff = Date.now() - maxAge; let changed = false;
    for (const id in this.data.room_state) {
      const r = this.data.room_state[id];
      const t = (r.state && r.state.createdAt) || r.updated_at || 0;
      if (t < cutoff) { delete this.data.room_state[id]; changed = true; }
    }
    if (changed) this.scheduleSave();
  },
  /* ---- rooms_registry ---- */
  regUpdate(id, row) {
    if (row === null) delete this.data.rooms_registry[id];
    else this.data.rooms_registry[id] = row;
    this.scheduleSave();
  },
  regTouch(id, watchers) {
    const r = this.data.rooms_registry[id];
    if (r) { r.watchers = watchers; r.updated_at = Date.now(); this.scheduleSave(); }
  },
  listRooms(since) {
    const rows = [];
    for (const id in this.data.rooms_registry) {
      const r = this.data.rooms_registry[id];
      if (r.updated_at > since) {
        rows.push({ id: r.id, red: r.red, black: r.black, status: r.status, watchers: r.watchers, updated_at: r.updated_at });
      }
    }
    rows.sort((a, b) => b.updated_at - a.updated_at);
    return rows.slice(0, 50);
  },
  pruneRegistry(maxAge) {
    const cutoff = Date.now() - maxAge; let changed = false;
    for (const id in this.data.rooms_registry) {
      if (this.data.rooms_registry[id].updated_at < cutoff) { delete this.data.rooms_registry[id]; changed = true; }
    }
    if (changed) this.scheduleSave();
  },
  /* ---- ai_weights ---- */
  getAiRow() { return this.data.ai_weights; },
  saveAiRow(weightsStr, statsStr) {
    this.data.ai_weights = { id: 1, weights: weightsStr, stats: statsStr, updated_at: Date.now() };
    this.scheduleSave();
  },
  /* ---- online_now ---- */
  addOnline(cid) { this.data.online_now[cid] = Date.now(); this.scheduleSave(); },
  touchOnline(cid) { this.data.online_now[cid] = Date.now(); this.scheduleSave(); },
  dropOnline(cid) { if (this.data.online_now[cid]) { delete this.data.online_now[cid]; this.scheduleSave(); } },
  countOnline(windowMs) {
    const cutoff = Date.now() - windowMs; let n = 0;
    for (const cid in this.data.online_now) if (this.data.online_now[cid] > cutoff) n++;
    return n;
  },
  pruneOnline(maxAge) {
    const cutoff = Date.now() - maxAge; let changed = false;
    for (const cid in this.data.online_now) {
      if (this.data.online_now[cid] < cutoff) { delete this.data.online_now[cid]; changed = true; }
    }
    if (changed) this.scheduleSave();
  },
  /* ---- race_report ---- */
  insertRaceReport(row) {
    this.data.race_report.push(row);
    if (this.data.race_report.length > 2000) this.data.race_report.splice(0, this.data.race_report.length - 2000);
    this.scheduleSave();
  }
};

/* ==========================================================================
 * WebSocket（RFC6455 服务端最小实现，仅应用所需子集，零依赖）
 * ========================================================================*/
const WS = { CONNECTING: 0, OPEN: 1, CLOSING: 2, CLOSED: 3 };
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const WS_MAX_MSG = 1024 * 1024; // 1MB 上限（应用消息均为小 JSON）

function wsAccept(key) {
  return crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
}

function wsEncodeFrame(opcode, payload) {
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2); header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4); header[1] = 126; header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10); header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2);
  }
  header[0] = 0x80 | opcode; // FIN=1
  return Buffer.concat([header, payload]);
}

class WSConn {
  constructor(socket) {
    this._socket = socket;
    this.readyState = WS.OPEN;
    this.onmessage = null;
    this.onclose = null;
    this.onerror = null;
    this._buffer = Buffer.alloc(0);
    this._fragBuffers = [];
    this._fragOpcode = 0;
    this._closeSent = false;
    this._closeFired = false;
    socket.on('data', (d) => this._onData(d));
    socket.on('close', () => this._fireClose());
    socket.on('error', (e) => {
      if (this.onerror) { try { this.onerror(e); } catch (_) {} }
      this._fireClose();
    });
  }
  _onData(chunk) {
    try {
      this._buffer = this._buffer.length === 0 ? chunk : Buffer.concat([this._buffer, chunk]);
      while (this.readyState !== WS.CLOSED) {
        const frame = this._tryParseFrame();
        if (!frame) break;
        this._handleFrame(frame);
      }
      if (this.readyState === WS.CLOSED) this._buffer = null;
    } catch (e) {
      this.close(1011);
    }
  }
  _tryParseFrame() {
    const buf = this._buffer;
    if (!buf || buf.length < 2) return null;
    const b0 = buf[0], b1 = buf[1];
    const fin = (b0 & 0x80) !== 0;
    const opcode = b0 & 0x0f;
    const masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f;
    let off = 2;
    if (len === 126) {
      if (buf.length < off + 2) return null;
      len = buf.readUInt16BE(off); off += 2;
    } else if (len === 127) {
      if (buf.length < off + 8) return null;
      const big = buf.readBigUInt64BE(off); off += 8;
      if (big > BigInt(WS_MAX_MSG)) { this.close(1009); return null; }
      len = Number(big);
    }
    if (len > WS_MAX_MSG) { this.close(1009); return null; }
    const maskKey = masked ? 4 : 0;
    if (buf.length < off + maskKey + len) return null;
    let payload = buf.slice(off + maskKey, off + maskKey + len);
    if (masked) {
      const m = buf.slice(off, off + 4);
      payload = Buffer.from(payload); // copy before unmask
      for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3];
    }
    this._buffer = buf.slice(off + maskKey + len);
    return { fin, opcode, payload };
  }
  _handleFrame(f) {
    switch (f.opcode) {
      case 0x0: { // continuation
        if (this._fragBuffers.length === 0) { this.close(1002); return; }
        this._fragBuffers.push(f.payload);
        if (f.fin) {
          const full = Buffer.concat(this._fragBuffers);
          const op = this._fragOpcode;
          this._fragBuffers = []; this._fragOpcode = 0;
          this._deliver(op, full);
        }
        break;
      }
      case 0x1: case 0x2: {
        if (!f.fin) { this._fragBuffers = [f.payload]; this._fragOpcode = f.opcode; }
        else this._deliver(f.opcode, f.payload);
        break;
      }
      case 0x8: { // close
        const code = f.payload && f.payload.length >= 2 ? f.payload.readUInt16BE(0) : 1005;
        this._sendCloseFrame(code);
        break;
      }
      case 0x9: // ping -> pong
        this._sendRaw(wsEncodeFrame(0xA, f.payload));
        break;
      case 0xA: break; // pong 忽略
      default: this.close(1002);
    }
  }
  _deliver(opcode, payload) {
    if (opcode !== 0x1) return; // 应用只使用文本帧
    const text = payload.toString('utf8');
    if (this.onmessage) {
      try { this.onmessage({ data: text }); }
      catch (e) { console.error('onmessage handler error:', e); }
    }
  }
  _sendRaw(buf) {
    if (this.readyState === WS.CLOSED) return;
    try { this._socket.write(buf); } catch (e) {}
  }
  _sendCloseFrame(code) {
    if (this._closeSent) return;
    this._closeSent = true;
    this.readyState = WS.CLOSING;
    const body = Buffer.alloc(2);
    body.writeUInt16BE(code || 1000, 0);
    this._sendRaw(wsEncodeFrame(0x8, body));
    const sock = this._socket;
    setTimeout(() => { try { sock.destroy(); } catch (e) {} }, 500);
  }
  _fireClose() {
    if (this._closeFired) return;
    this._closeFired = true;
    this.readyState = WS.CLOSED;
    if (this.onclose) { try { this.onclose({}); } catch (e) {} }
  }
  send(str) {
    if (this.readyState !== WS.OPEN) return;
    this._sendRaw(wsEncodeFrame(0x1, Buffer.from(String(str), 'utf8')));
  }
  close(code) {
    this._sendCloseFrame(code === undefined ? 1000 : code);
  }
}

/* ==========================================================================
 * AI 权重全局状态（与 Worker 版一致）
 * ========================================================================*/
var aiWeights = {
  attackKing: 70,
  limitKingMob: 35,
  approach: 30,
  mobility: 5,
  rookNotMoved: 5,
  rookCrossed: 110,
  rookDeveloped: 90,
  horseDeveloped: 25,
  cannonDeveloped: 15,
  pieceSafety: 80,
  hangingPenalty: 80,
  tradeAccuracy: 120,
  pawnPromotion: 50,
  checkBonus: 130,
  centerControl: 20,
  rookCoordination: 60,
  kingSafety: 40
};
var aiTotalStats = { games: 0, redWins: 0, blkWins: 0, draws: 0 };

function loadAiFromStore() {
  const row = store.getAiRow();
  if (!row) return;
  if (row.weights) {
    try {
      const w = JSON.parse(row.weights);
      for (const k in w) {
        if (aiWeights[k] !== undefined && typeof w[k] === 'number') aiWeights[k] = w[k];
      }
    } catch (e) {}
  }
  if (row.stats) {
    try {
      const s = JSON.parse(row.stats);
      if (s && typeof s === 'object') {
        if (typeof s.games === 'number') aiTotalStats.games = s.games;
        if (typeof s.redWins === 'number') aiTotalStats.redWins = s.redWins;
        if (typeof s.blkWins === 'number') aiTotalStats.blkWins = s.blkWins;
        if (typeof s.draws === 'number') aiTotalStats.draws = s.draws;
      }
    } catch (e) {}
  }
}
function saveAiToStore() {
  store.saveAiRow(JSON.stringify(aiWeights), JSON.stringify(aiTotalStats));
  return true;
}

/* ==========================================================================
 * 房间（Durable Object ChessRoom 的 1:1 移植）
 * ========================================================================*/
const rooms = new Map(); // roomId -> ChessRoom
function getRoom(roomId) {
  let room = rooms.get(roomId);
  if (!room) {
    room = new ChessRoom(roomId);
    rooms.set(roomId, room);
  }
  return room;
}

var ChessRoom = class {
  constructor(roomId) {
    this.roomId = roomId;
    this.room = null;
    this.disconnected = {};
    this._cleanupTimer = null;
    this.ctx = {
      waitUntil: (p) => { if (p && typeof p.catch === 'function') p.catch(() => {}); }
    };
  }
  async initRoom(roomId) {
    if (!this.room) {
      this.room = {
        id: roomId,
        players: new Map(),
        spectators: new Set(),
        currentTurn: 'red',
        gameOver: false,
        winner: null,
        redTime: 900,
        blkTime: 900,
        moveHistory: [],
        capturedRed: [],
        capturedBlack: [],
        playerTokens: null,
        gameStarted: false,
        createdAt: Date.now()
      };
      try {
        const saved = store.getRoomState(roomId);
        if (saved && saved.createdAt && Date.now() - saved.createdAt < 36e5) {
          this.room.currentTurn = saved.currentTurn || 'red';
          this.room.gameOver = saved.gameOver || false;
          this.room.winner = saved.winner || null;
          this.room.redTime = saved.redTime != null ? saved.redTime : 900;
          this.room.blkTime = saved.blkTime != null ? saved.blkTime : 900;
          this.room.moveHistory = saved.moveHistory || [];
          this.room.capturedRed = saved.capturedRed || [];
          this.room.capturedBlack = saved.capturedBlack || [];
          this.room.createdAt = saved.createdAt;
          this.room.playerTokens = saved.playerTokens || null;
          this.room.gameStarted = !!saved.gameStarted;
          this.room._restoredFromDb = true;
        }
      } catch (e) {}
    }
  }
  async _saveRoomState() {
    if (!this.room) return;
    try {
      const state = {
        currentTurn: this.room.currentTurn,
        gameOver: this.room.gameOver,
        winner: this.room.winner,
        redTime: this.room.redTime,
        blkTime: this.room.blkTime,
        moveHistory: this.room.moveHistory.slice(-200),
        capturedRed: this.room.capturedRed || [],
        capturedBlack: this.room.capturedBlack || [],
        playerTokens: this.room.playerTokens || null,
        gameStarted: !!this.room.gameStarted,
        createdAt: this.room.createdAt
      };
      store.saveRoomState(this.room.id, state);
    } catch (e) {}
  }
  async _regUpdate(status) {
    const rid = this.room ? this.room.id : null;
    if (!rid) return;
    const hasRed = [...this.room.players.values()].some((p) => p.color === 'red');
    const hasBlack = [...this.room.players.values()].some((p) => p.color === 'black');
    const rn = hasRed ? '\u5DF2\u5165\u5EA7' : '';
    const bn = hasBlack ? '\u5DF2\u5165\u5EA7' : '';
    const watchers = this.room.spectators ? this.room.spectators.size : 0;
    try {
      if (status === null) {
        store.regUpdate(rid, null);
      } else {
        store.regUpdate(rid, { id: rid, red: rn, black: bn, status: status, watchers: watchers, updated_at: Date.now() });
      }
    } catch (e) {}
  }
  async _regTouch() {
    if (!this.room) return;
    try {
      store.regTouch(this.room.id, this.room.spectators ? this.room.spectators.size : 0);
    } catch (e) {}
  }
  handleConnection(ws) {
    const roomId = this.roomId;
    this.initRoom(roomId).then(() => {
      this.handleRoomWebSocket(ws);
    }).catch(() => {});
  }
  handleRoomWebSocket(ws) {
    const socketData = { color: null, spectator: false, replaced: false };
    ws._socketData = socketData;
    ws._lastSeen = Date.now();
    let heartbeatTimer = null;
    let heartbeatTimeout = null;
    const startHeartbeat = () => {
      stopHeartbeat();
      heartbeatTimer = setInterval(() => {
        if (heartbeatTimeout) { clearTimeout(heartbeatTimeout); heartbeatTimeout = null; }
        try { ws.send(JSON.stringify({ event: 'ping' })); } catch (e) {}
        heartbeatTimeout = setTimeout(() => {
          try { ws.close(); } catch (e) {}
        }, 6e4);
      }, 3e4);
    };
    const stopHeartbeat = () => {
      if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
      if (heartbeatTimeout) { clearTimeout(heartbeatTimeout); heartbeatTimeout = null; }
    };
    startHeartbeat();
    ws.onmessage = async (event) => {
      try {
        ws._lastSeen = Date.now();
        const data = JSON.parse(event.data);
        const eventName = data.event || data[0];
        const payload = data.payload || data[1];
        if (eventName === 'pong') {
          if (heartbeatTimeout) { clearTimeout(heartbeatTimeout); heartbeatTimeout = null; }
        } else if (eventName === 'ping') {
          try { ws.send(JSON.stringify({ event: 'pong' })); } catch (e) {}
        } else if (eventName === 'create_room') {
          if (this._cleanupTimer) { clearTimeout(this._cleanupTimer); this._cleanupTimer = null; }
          for (const [pws, p] of [...this.room.players]) {
            const st = pws.readyState;
            const rep = pws._socketData && pws._socketData.replaced;
            if (st === 2 || st === 3 || rep) this.room.players.delete(pws);
          }
          let lastColor = null;
          if (payload && typeof payload === 'object' && payload.lastColor) {
            lastColor = payload.lastColor;
          }
          if (this.room.players.size > 0) {
            let existingColor = [...this.room.players.values()][0].color;
            let myColor = existingColor === 'red' ? 'black' : 'red';
            const takenByLive = [...this.room.players.entries()].some(([pws2, p2]) => p2.color === myColor && pws2 !== ws && !(pws2._socketData && pws2._socketData.replaced));
            if (takenByLive) myColor = myColor === 'red' ? 'black' : 'red';
            const stillTaken = [...this.room.players.values()].some((p2) => p2.color === myColor);
            if (stillTaken) {
              ws.send(JSON.stringify({ event: 'error', data: '\u623F\u95F4\u5DF2\u6EE1' }));
              return;
            }
            const myPid = Math.random().toString(36).slice(2) + Date.now().toString(36);
            if (!this.room.playerTokens) this.room.playerTokens = {};
            this.room.playerTokens[myColor] = myPid;
            this.room.players.set(ws, { id: Math.random().toString(36).slice(2), color: myColor, assignedAt: Date.now() });
            socketData.color = myColor;
            ws.send(JSON.stringify({ event: 'room_created', data: { roomId: this.room.id, color: myColor, pid: myPid } }));
            this.ctx.waitUntil(this._regUpdate(this.room.players.size >= 2 ? 'playing' : 'waiting'));
          } else {
            let myColor;
            if (lastColor === 'red') { myColor = 'black'; }
            else if (lastColor === 'black') { myColor = 'red'; }
            else { myColor = Math.random() < 0.5 ? 'red' : 'black'; }
            const myPid = Math.random().toString(36).slice(2) + Date.now().toString(36);
            if (!this.room.playerTokens) this.room.playerTokens = {};
            this.room.playerTokens[myColor] = myPid;
            this.room.players.set(ws, { id: Math.random().toString(36).slice(2), color: myColor, assignedAt: Date.now() });
            socketData.color = myColor;
            ws.send(JSON.stringify({ event: 'room_created', data: { roomId: this.room.id, color: myColor, pid: myPid } }));
            this.ctx.waitUntil(this._regUpdate(this.room.players.size >= 2 ? 'playing' : 'waiting'));
          }
        } else if (eventName === 'join_room') {
          if (this._cleanupTimer) { clearTimeout(this._cleanupTimer); this._cleanupTimer = null; }
          for (const [pws, p] of [...this.room.players]) {
            const st = pws.readyState;
            const rep = pws._socketData && pws._socketData.replaced;
            if (st === 2 || st === 3 || rep) this.room.players.delete(pws);
          }
          if (this.room.players.size === 0) {
            ws.send(JSON.stringify({ event: 'error', data: '\u623F\u95F4\u4E0D\u5B58\u5728' }));
            return;
          }
          if (this.room.players.size >= 2) {
            this.room.spectators.add(ws);
            socketData.spectator = true;
            ws.send(JSON.stringify({ event: 'spectator_joined', data: { roomId: this.room.id, moveHistory: this.room.moveHistory } }));
            this.ctx.waitUntil(this._regTouch());
            return;
          }
          let color = [...this.room.players.values()][0].color === 'red' ? 'black' : 'red';
          const joinPid = Math.random().toString(36).slice(2) + Date.now().toString(36);
          if (!this.room.playerTokens) this.room.playerTokens = {};
          this.room.playerTokens[color] = joinPid;
          this.room.players.set(ws, { id: Math.random().toString(36).slice(2), color, assignedAt: Date.now() });
          socketData.color = color;
          if (this.room.players.size >= 2) this.room.gameStarted = true;
          ws.send(JSON.stringify({ event: 'room_joined', data: { roomId: this.room.id, color, pid: joinPid } }));
          this.broadcastRoomState();
          this.broadcastToPlayers(JSON.stringify({ event: 'game_start', data: { currentTurn: this.room.currentTurn } }));
          this.startRoomTimer();
          this.ctx.waitUntil(this._regUpdate(this.room.players.size >= 2 ? 'playing' : 'waiting'));
        } else if (eventName === 'make_move') {
          if (!this.room || this.room.gameOver) {
            ws.send(JSON.stringify({ event: 'move_rejected', data: { reason: 'invalid_state' } }));
            return;
          }
          if (this.room.players.size < 2 && this.room.moveHistory.length === 0 && !this.room.gameStarted) {
            ws.send(JSON.stringify({ event: 'move_rejected', data: { reason: 'invalid_state' } }));
            return;
          }
          if (!socketData.color) {
            for (const [pws, player] of this.room.players) {
              if (pws === ws) { socketData.color = player.color; break; }
            }
          }
          if (!socketData.color) {
            ws.send(JSON.stringify({ event: 'move_rejected', data: { reason: 'no_color' } }));
            return;
          }
          const histLen = this.room.moveHistory.length;
          const lastMv = this.room.moveHistory[histLen - 1];
          if (lastMv && payload && lastMv.fromRow === payload.fromRow && lastMv.fromCol === payload.fromCol && lastMv.toRow === payload.toRow && lastMv.toCol === payload.toCol) {
            try {
              ws.send(JSON.stringify({ event: 'move_ack', data: { moveHistoryLen: this.room.moveHistory.length, lastMove: { fromRow: lastMv.fromRow, fromCol: lastMv.fromCol, toRow: lastMv.toRow, toCol: lastMv.toCol }, currentTurn: this.room.currentTurn } }));
            } catch (e) {}
            return;
          }
          if (this.room.currentTurn !== socketData.color) {
            ws.send(JSON.stringify({ event: 'move_rejected', data: { reason: 'not_your_turn' } }));
            return;
          }
          const move = { ...payload, timestamp: Date.now() };
          this.room.moveHistory.push(move);
          if (move.captured) {
            if (!this.room.capturedRed) this.room.capturedRed = [];
            if (!this.room.capturedBlack) this.room.capturedBlack = [];
            if (move.captured.color === 'red') this.room.capturedRed.push(move.captured);
            else this.room.capturedBlack.push(move.captured);
          }
          this.room.currentTurn = socketData.color === 'red' ? 'black' : 'red';
          if (move.redLeft !== void 0) this.room.redTime = move.redLeft;
          if (move.blkLeft !== void 0) this.room.blkTime = move.blkLeft;
          if (move.gameOver) {
            this.room.gameOver = true;
            this.room._gameEndedAt = Date.now();
            this.room.winner = move.winner;
            if (this.room._timer) { clearInterval(this.room._timer); this.room._timer = null; }
          }
          const opponentMove = { ...move, redLeft: this.room.redTime, blkLeft: this.room.blkTime };
          const ackData = { moveHistoryLen: this.room.moveHistory.length, lastMove: { fromRow: move.fromRow, fromCol: move.fromCol, toRow: move.toRow, toCol: move.toCol }, currentTurn: this.room.currentTurn };
          try { ws.send(JSON.stringify({ event: 'move_ack', data: ackData })); } catch (e) {}
          this.broadcastToOpponent(ws, JSON.stringify({ event: 'opponent_move', data: opponentMove }));
          this.broadcastRoomStateToOpponent(ws);
          this.broadcastToSpectators(JSON.stringify({ event: 'opponent_move', data: opponentMove }));
          this.ctx.waitUntil(this._saveRoomState());
        } else if (eventName === 'resign') {
          if (!this.room) return;
          this.room.gameOver = true;
          this.room._gameEndedAt = Date.now();
          this.room.winner = socketData.color === 'red' ? 'black' : 'red';
          if (this.room._timer) { clearInterval(this.room._timer); this.room._timer = null; }
          this.broadcastToRoom(JSON.stringify({ event: 'game_over', data: { winner: this.room.winner, reason: 'resign' } }));
          await this._saveRoomState();
        } else if (eventName === 'request_draw') {
          if (!this.room) return;
          this.broadcastToOpponent(ws, JSON.stringify({ event: 'draw_requested', data: { from: socketData.color } }));
        } else if (eventName === 'accept_draw') {
          if (!this.room) return;
          this.room.gameOver = true;
          this.room._gameEndedAt = Date.now();
          this.room.winner = 'draw';
          if (this.room._timer) { clearInterval(this.room._timer); this.room._timer = null; }
          this.broadcastToRoom(JSON.stringify({ event: 'game_over', data: { winner: 'draw', reason: 'draw' } }));
          await this._saveRoomState();
        } else if (eventName === 'reject_draw') {
          if (!this.room) return;
          this.broadcastToOpponent(ws, JSON.stringify({ event: 'draw_rejected', data: {} }));
        } else if (eventName === 'chat') {
          if (!this.room) return;
          const msg = { from: socketData.color || 'spectator', color: socketData.color, message: payload, timestamp: Date.now() };
          this.broadcastToRoom(JSON.stringify({ event: 'chat', data: msg }));
        } else if (eventName === 'rematch_request') {
          if (!this.room || this.room.players.size < 2) return;
          this.broadcastToOpponent(ws, JSON.stringify({ event: 'rematch_requested', data: {} }));
        } else if (eventName === 'accept_rematch') {
          if (!this.room || this.room.players.size < 2) return;
          const playerEntries = [...this.room.players.entries()];
          if (playerEntries.length === 2) {
            const [wsA, dataA] = playerEntries[0];
            const [wsB, dataB] = playerEntries[1];
            const tmpColor = dataA.color;
            dataA.color = dataB.color;
            dataB.color = tmpColor;
            if (wsA._socketData) wsA._socketData.color = dataA.color;
            if (wsB._socketData) wsB._socketData.color = dataB.color;
          }
          if (this.room.playerTokens && this.room.playerTokens.red && this.room.playerTokens.black) {
            const tmpTok = this.room.playerTokens.red;
            this.room.playerTokens.red = this.room.playerTokens.black;
            this.room.playerTokens.black = tmpTok;
          }
          this.room.gameOver = false;
          this.room._gameEndedAt = null;
          this.room.winner = null;
          this.room.currentTurn = 'red';
          this.room.redTime = 900;
          this.room.blkTime = 900;
          this.room.moveHistory = [];
          this.room.capturedRed = [];
          this.room.capturedBlack = [];
          this.room.createdAt = Date.now();
          this.startRoomTimer();
          this.broadcastToRoom(JSON.stringify({ event: 'rematch_start', data: {} }));
          this.broadcastRoomState();
          this.ctx.waitUntil(this._regUpdate('playing'));
          await this._saveRoomState();
        } else if (eventName === 'request_undo') {
          if (!this.room || this.room.moveHistory.length === 0 || this.room.gameOver) return;
          this.broadcastToOpponent(ws, JSON.stringify({ event: 'undo_requested', data: {} }));
        } else if (eventName === 'accept_undo') {
          if (!this.room || this.room.moveHistory.length === 0) return;
          const lastMove = this.room.moveHistory.pop();
          this.room.gameOver = false;
          this.room.winner = null;
          this.room.currentTurn = lastMove.currentTurn === 'red' ? 'black' : 'red';
          if (lastMove.captured) {
            if (lastMove.captured.color === 'red' && this.room.capturedRed && this.room.capturedRed.length > 0) {
              this.room.capturedRed.pop();
            } else if (lastMove.captured.color === 'black' && this.room.capturedBlack && this.room.capturedBlack.length > 0) {
              this.room.capturedBlack.pop();
            }
          }
          this.broadcastToOpponent(ws, JSON.stringify({ event: 'undo_accepted', data: {} }));
          this.broadcastRoomState();
          await this._saveRoomState();
        } else if (eventName === 'reject_undo') {
          if (!this.room) return;
          this.broadcastToOpponent(ws, JSON.stringify({ event: 'undo_rejected', data: {} }));
        } else if (eventName === 'reconnect_room') {
          if (!this.room) {
            try { await this.initRoom(this.roomId); } catch (e2) {}
          }
          if (!this.room) return;
          const otherEntries = [...this.room.players.entries()].filter(([pws]) => pws !== ws);
          const isReplaceable = (c) => otherEntries.some(([pws, p]) => p.color === c && (pws.readyState === WS.CLOSED || pws.readyState === WS.CLOSING || pws.readyState === WS.CONNECTING || this.disconnected && this.disconnected[c]));
          const isFree = (c) => !otherEntries.some(([, p]) => p.color === c);
          if (payload && payload.pid && this.room.playerTokens) {
            const tokColor = this.room.playerTokens.red === payload.pid ? 'red' : this.room.playerTokens.black === payload.pid ? 'black' : null;
            if (tokColor) { payload.color = tokColor; }
          }
          let color = payload.color;
          if (color) {
            if (isFree(color)) {
            } else {
              const sameColorEntry = otherEntries.find(([, p]) => p.color === color);
              if (sameColorEntry) {
                const [hws, seat] = sameColorEntry;
                const pidMatches2 = !!(payload.pid && this.room.playerTokens && this.room.playerTokens[color] === payload.pid);
                const holderDead = hws.readyState !== 1 || hws._socketData && hws._socketData.replaced;
                const holderIdle = !hws._lastSeen || Date.now() - hws._lastSeen > 25e3;
                const preDiscSeat = !!(this.disconnected && this.disconnected[color] && seat.assignedAt && this.disconnected[color] > seat.assignedAt);
                if (!pidMatches2 && !holderDead && !holderIdle && !preDiscSeat) {
                  const alt2 = color === 'red' ? 'black' : 'red';
                  if (isFree(alt2) || isReplaceable(alt2)) { color = alt2; }
                  else {
                    ws.send(JSON.stringify({ event: 'error', data: '\u623F\u95F4\u5DF2\u6EE1\uFF0C\u65E0\u6CD5\u91CD\u8FDE' }));
                    try { ws.close(); } catch (e2) {}
                    return;
                  }
                }
              } else {
                const alt = color === 'red' ? 'black' : 'red';
                if (isFree(alt) || isReplaceable(alt)) { color = alt; }
                else {
                  ws.send(JSON.stringify({ event: 'error', data: '\u623F\u95F4\u5DF2\u6EE1\uFF0C\u65E0\u6CD5\u91CD\u8FDE' }));
                  try { ws.close(); } catch (e) {}
                  return;
                }
              }
            }
          }
          if (!color) {
            const existingColors = [...this.room.players.values()].map((p) => p.color);
            if (existingColors.includes('red')) color = 'black';
            else if (existingColors.includes('black')) color = 'red';
            else if (this.disconnected.red) color = 'red';
            else if (this.disconnected.black) color = 'black';
          }
          if (!color) {
            ws.send(JSON.stringify({ event: 'error', data: '\u65E0\u6CD5\u91CD\u8FDE' }));
            return;
          }
          if (this.disconnected[color]) delete this.disconnected[color];
          if (this.room._disconnectTimer) { clearTimeout(this.room._disconnectTimer); this.room._disconnectTimer = null; }
          let rotatedPid = null;
          const pidMatches = !!(payload.pid && this.room.playerTokens && this.room.playerTokens[color] === payload.pid);
          if (!pidMatches) {
            rotatedPid = Math.random().toString(36).slice(2) + Date.now().toString(36);
            if (!this.room.playerTokens) this.room.playerTokens = {};
            this.room.playerTokens[color] = rotatedPid;
          }
          for (const [pws, player] of this.room.players) {
            if (player.color === color && pws !== ws) {
              if (pws._socketData) pws._socketData.replaced = true;
              this.room.players.delete(pws);
              break;
            }
          }
          this.room.players.set(ws, { id: Math.random().toString(36).slice(2), color, assignedAt: Date.now() });
          socketData.color = color;
          if (this.room.players.size >= 2) this.room.gameStarted = true;
          const gameInProgress = !this.room.gameOver && this.room.moveHistory.length > 0;
          try {
            ws.send(JSON.stringify({ event: 'room_state', data: {
              roomId: this.room.id,
              color,
              moveHistory: this.room.moveHistory,
              currentTurn: this.room.currentTurn,
              gameOver: this.room.gameOver,
              winner: this.room.winner,
              redTime: this.room.redTime,
              blkTime: this.room.blkTime,
              capturedRed: this.room.capturedRed,
              capturedBlack: this.room.capturedBlack,
              gameStarted: true,
              pid: rotatedPid || payload.pid || void 0
            } }));
          } catch (e) {}
          this.broadcastRoomState();
          if (!this.room.gameOver && this.room.players.size >= 2) {
            this.startRoomTimer();
          }
          this.broadcastToOpponent(ws, JSON.stringify({ event: 'player_reconnected', data: { color } }));
          this.ctx.waitUntil(this._regUpdate(this.room.players.size >= 2 ? 'playing' : 'waiting'));
        } else if (eventName === 'leave_room') {
          if (!this.room) return;
          if (socketData.spectator) {
            this.room.spectators.delete(ws);
            return;
          }
          const room = this.room;
          this.ctx.waitUntil(this._regUpdate(null));
          this.room = null;
          if (room._timer) { clearInterval(room._timer); room._timer = null; }
          room.players.delete(ws);
          for (const [pws] of room.players) {
            try { pws.send(JSON.stringify({ event: 'opponent_left', data: {} })); } catch (e) {}
          }
          room.players.forEach((p, pws) => { try { pws.close(); } catch (e) {} });
          room.spectators.forEach((s) => { try { s.close(); } catch (e) {} });
          try { ws.close(); } catch (e) {}
          try { store.deleteRoomState(room.id); } catch (e) {}
          if (this._cleanupTimer) { clearTimeout(this._cleanupTimer); this._cleanupTimer = null; }
          this.disconnected = {};
        }
      } catch (e) {
        console.error('Room WebSocket message error:', e);
      }
    };
    ws.onclose = () => {
      stopHeartbeat();
      if (socketData.spectator) {
        if (this.room) this.room.spectators.delete(ws);
        return;
      }
      if (!this.room || !socketData.color) return;
      if (socketData.replaced) return;
      this.room.players.delete(ws);
      if (this.room._timer) { clearInterval(this.room._timer); this.room._timer = null; }
      this.disconnected[socketData.color] = Date.now();
      this.broadcastToOpponent(ws, JSON.stringify({ event: 'player_disconnected', data: { color: socketData.color } }));
      this._saveRoomState();
      this.ctx.waitUntil(this._regUpdate(this.room.players.size >= 2 ? 'playing' : 'waiting'));
      if (!this.room._disconnectTimer) {
        this.room._disconnectTimer = setTimeout(() => {
          if (!this.room || this.room.gameOver) return;
          const now = Date.now();
          for (const color of ['red', 'black']) {
            if (this.disconnected[color] && now - this.disconnected[color] > 18e4) {
              this.room.gameOver = true;
              this.room._gameEndedAt = now;
              this.room.winner = color === 'red' ? 'black' : 'red';
              this.broadcastToRoom(JSON.stringify({ event: 'game_over', data: { winner: this.room.winner, reason: 'disconnect_timeout' } }));
              this.broadcastToRoom(JSON.stringify({ event: 'room_timeout', data: {} }));
              if (this.room._timer) { clearInterval(this.room._timer); this.room._timer = null; }
              this._saveRoomState();
              break;
            }
          }
          this.room._disconnectTimer = null;
        }, 183e3);
      }
    };
    ws.onerror = () => {
      stopHeartbeat();
    };
  }
  broadcastToRoom(msg) {
    if (!this.room) return;
    for (const ws of this.room.players.keys()) { try { ws.send(msg); } catch (e) {} }
    for (const ws of this.room.spectators) { try { ws.send(msg); } catch (e) {} }
  }
  broadcastRoomState() {
    if (!this.room) return;
    const baseState = this.getRoomState();
    for (const [ws, player] of this.room.players) {
      try { ws.send(JSON.stringify({ event: 'room_state', data: { ...baseState, color: player.color } })); } catch (e) {}
    }
    for (const ws of this.room.spectators) {
      try { ws.send(JSON.stringify({ event: 'room_state', data: baseState })); } catch (e) {}
    }
  }
  broadcastToPlayers(msg) {
    if (!this.room) return;
    for (const ws of this.room.players.keys()) { try { ws.send(msg); } catch (e) {} }
  }
  broadcastToSpectators(msg) {
    if (!this.room) return;
    for (const ws of this.room.spectators) { try { ws.send(msg); } catch (e) {} }
  }
  broadcastToOpponent(ws, msg) {
    if (!this.room) return;
    for (const [pws, player] of this.room.players) {
      if (pws !== ws) { try { pws.send(msg); } catch (e) {} }
    }
  }
  broadcastRoomStateToOpponent(ws) {
    if (!this.room) return;
    const baseState = this.getRoomState();
    for (const [pws, player] of this.room.players) {
      if (pws !== ws) {
        try { pws.send(JSON.stringify({ event: 'room_state', data: { ...baseState, color: player.color } })); } catch (e) {}
      }
    }
  }
  getRoomState() {
    return {
      roomId: this.room.id,
      playerCount: this.room.players.size,
      currentTurn: this.room.currentTurn,
      gameOver: this.room.gameOver,
      winner: this.room.winner,
      moveHistory: this.room.moveHistory,
      redTime: this.room.redTime,
      blkTime: this.room.blkTime,
      capturedRed: this.room.capturedRed,
      capturedBlack: this.room.capturedBlack,
      gameStarted: this.room.players.size >= 2
    };
  }
  startRoomTimer() {
    if (!this.room) return;
    if (this.room._timer) clearInterval(this.room._timer);
    this.room._timerLastTick = Date.now();
    this.room._timer = setInterval(() => {
      if (!this.room) return;
      if (this.room.gameOver) {
        clearInterval(this.room._timer);
        this.room._timer = null;
        return;
      }
      this.broadcastToRoom(JSON.stringify({ event: 'time_update', data: { redTime: this.room.redTime, blkTime: this.room.blkTime, currentTurn: this.room.currentTurn } }));
      const now = Date.now();
      const elapsed = Math.max(1, Math.round((now - this.room._timerLastTick) / 1e3));
      this.room._timerLastTick = now;
      if (this.room.currentTurn === 'red') {
        this.room.redTime = Math.max(0, this.room.redTime - elapsed);
        if (this.room.redTime <= 0) {
          this.room.gameOver = true;
          this.room.winner = 'black';
          this.room._gameEndedAt = Date.now();
          clearInterval(this.room._timer);
          this.room._timer = null;
          this.broadcastToRoom(JSON.stringify({ event: 'timeout', data: { winner: 'black' } }));
          this.broadcastToRoom(JSON.stringify({ event: 'game_over', data: { winner: 'black', reason: 'timeout' } }));
          this._saveRoomState();
        }
      } else {
        this.room.blkTime = Math.max(0, this.room.blkTime - elapsed);
        if (this.room.blkTime <= 0) {
          this.room.gameOver = true;
          this.room.winner = 'red';
          this.room._gameEndedAt = Date.now();
          clearInterval(this.room._timer);
          this.room._timer = null;
          this.broadcastToRoom(JSON.stringify({ event: 'timeout', data: { winner: 'red' } }));
          this.broadcastToRoom(JSON.stringify({ event: 'game_over', data: { winner: 'red', reason: 'timeout' } }));
          this._saveRoomState();
        }
      }
    }, 1e3);
  }
};

/* ==========================================================================
 * 大厅 WebSocket（Worker handleWebSocket 的 1:1 移植）
 * ========================================================================*/
var activeConnections = new Set();
var onlineCount = 0;

function broadcastOnlineCount() {
  const msg = JSON.stringify({ event: 'online_count', data: onlineCount });
  for (const ws of activeConnections) {
    try { ws.send(msg); } catch (e) {}
  }
}

async function handleLobbyWebSocket(ws) {
  const cid = 'c' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  let dbTouched = 0;
  onlineCount++;
  activeConnections.add(ws);
  ws._xqCounted = true;
  try { store.addOnline(cid); } catch (e) {}

  const sendRealCount = async () => {
    let n = onlineCount;
    try {
      const r = store.countOnline(25e3);
      if (typeof r === 'number') n = r;
    } catch (e) {}
    const msg = JSON.stringify({ event: 'online_count', data: n });
    for (const c of activeConnections) {
      try { c.send(msg); } catch (e) {}
    }
  };
  await sendRealCount();

  ws.onmessage = async (event) => {
    try {
      const data = JSON.parse(event.data);
      const eventName = data.event || data[0];
      const payload = data.payload || data[1];
      if (eventName === 'create_room') {
        let lastColor = null;
        let requestedRoomId = null;
        if (payload && typeof payload === 'object') {
          requestedRoomId = payload.roomId;
          lastColor = payload.lastColor;
        } else {
          requestedRoomId = payload;
        }
        const roomId = requestedRoomId || Math.random().toString(36).slice(2, 8).toUpperCase();
        ws.send(JSON.stringify({ event: 'redirect_room', data: { roomId, action: 'create', lastColor } }));
      } else if (eventName === 'ping') {
        try { ws.send(JSON.stringify({ event: 'pong' })); } catch (e) {}
        if (Date.now() - dbTouched > 15e3) {
          dbTouched = Date.now();
          try { store.touchOnline(cid); } catch (e) {}
        }
      } else if (eventName === 'join_room') {
        const roomId = payload;
        ws.send(JSON.stringify({ event: 'redirect_room', data: { roomId, action: 'join' } }));
      } else if (eventName === 'reconnect_room') {
        const roomId = payload.roomId;
        if (!roomId) return;
        ws.send(JSON.stringify({ event: 'redirect_room', data: { roomId, action: 'reconnect', color: payload.color } }));
      }
    } catch (e) {
      console.error('WebSocket message error:', e);
    }
  };
  const _dropConn = () => {
    if (!ws._xqCounted) return;
    ws._xqCounted = false;
    onlineCount--;
    activeConnections.delete(ws);
    try { store.dropOnline(cid); } catch (e) {}
    sendRealCount();
  };
  ws.onclose = () => { _dropConn(); };
  ws.onerror = () => { _dropConn(); };
}

/* ==========================================================================
 * /api/* （Worker handleApiRequest 的 1:1 移植，D1 换为 JSON 存储）
 * ========================================================================*/
function jsonRes(res, obj, status) {
  const body = JSON.stringify(obj);
  res.writeHead(status || 200, {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store'
  });
  res.end(body);
}

function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > (limit || 1024 * 256)) { reject(new Error('body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

async function handleApiRequest(req, res, url) {
  const p = url.pathname;
  if (req.method === 'OPTIONS') {
    res.writeHead(200, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    });
    res.end();
    return;
  }
  loadAiFromStore();
  if (p === '/api/ai/db-check') {
    const raw = store.getAiRow();
    jsonRes(res, { raw, aiTotalStats });
    return;
  }
  if (p === '/api/train/start') {
    jsonRes(res, { success: true });
    return;
  }
  if (p === '/api/train/stop') {
    jsonRes(res, { success: true });
    return;
  }
  if (p === '/api/train/status') {
    loadAiFromStore();
    jsonRes(res, {
      training: false,
      session: { games: 0, redWins: 0, blkWins: 0, draws: 0 },
      total: aiTotalStats,
      weights: aiWeights
    });
    return;
  }
  if (p === '/api/ai/data') {
    if (req.method === 'GET') {
      loadAiFromStore();
      jsonRes(res, { weights: aiWeights, trainStats: aiTotalStats });
      return;
    }
    if (req.method === 'POST') {
      try {
        loadAiFromStore();
        const data = JSON.parse(await readBody(req));
        if (data.weights) {
          const mergeFactor = data.isDelta ? 0.3 : 0.5;
          for (const k in data.weights) {
            if (aiWeights[k] !== void 0) {
              const newValue = data.weights[k];
              aiWeights[k] = aiWeights[k] * (1 - mergeFactor) + newValue * mergeFactor;
            }
          }
        }
        if (data.stats) {
          for (const k in data.stats) {
            if (aiTotalStats[k] !== void 0) {
              aiTotalStats[k] += data.stats[k] || 0;
            }
          }
        }
        let saved = false;
        saved = saveAiToStore();
        jsonRes(res, { success: true, weights: aiWeights, total: aiTotalStats, saved });
      } catch (e) {
        jsonRes(res, { success: false, error: e.message }, 400);
      }
      return;
    }
  }
  if (p === '/api/rooms') {
    try {
      const rows = store.listRooms(Date.now() - 12e5);
      jsonRes(res, { rooms: rows });
      return;
    } catch (e) {}
    jsonRes(res, { rooms: [] });
    return;
  }
  if (p === '/api/online') {
    try {
      store.pruneOnline(3e5);
      const n = store.countOnline(25e3);
      jsonRes(res, { count: n });
      return;
    } catch (e) {}
    jsonRes(res, { count: onlineCount });
    return;
  }
  if (p === '/api/create-room') {
    const customId = url.searchParams.get('id');
    const roomId = customId || Math.random().toString(36).slice(2, 8).toUpperCase();
    jsonRes(res, { roomId });
    return;
  }
  jsonRes(res, { error: 'Not found' }, 404);
}

/* ==========================================================================
 * HTTP 服务（Worker main fetch 的 1:1 移植 + 静态资源服务）
 * ========================================================================*/
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.htm': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
  '.wasm': 'application/wasm',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.txt': 'text/plain; charset=utf-8',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
  '.mp3': 'audio/mpeg',
  '.ogg': 'audio/ogg',
  '.wav': 'audio/wav'
};

function serveStatic(req, res, pathName) {
  // 与 Worker 一致："/" 或任何以 .html 结尾的路径 -> index.html（no-store）
  if (pathName === '/' || pathName.endsWith('.html')) {
    fs.readFile(path.join(DIST_DIR, 'index.html'), (err, buf) => {
      if (err) {
        res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Not found');
        return;
      }
      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store, no-cache, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      });
      res.end(req.method === 'HEAD' ? undefined : buf);
    });
    return;
  }
  const safePath = path.normalize(path.join(DIST_DIR, pathName));
  if (safePath !== DIST_DIR && !safePath.startsWith(DIST_DIR + path.sep)) {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not found');
    return;
  }
  fs.stat(safePath, (err, st) => {
    if (err || !st.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not found');
      return;
    }
    const ext = path.extname(safePath).toLowerCase();
    const etag = '"' + st.size.toString(16) + '-' + Math.floor(st.mtimeMs).toString(16) + '"';
    if (req.headers['if-none-match'] === etag) {
      res.writeHead(304, { ETag: etag });
      res.end();
      return;
    }
    const isBigBinary = /\.(part\d+|wasm|nnue)$/i.test(safePath) || ext === '.wasm';
    const headers = {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Cache-Control': isBigBinary ? 'public, max-age=86400' : 'no-cache',
      'ETag': etag,
      'Content-Length': st.size,
      'Accept-Ranges': 'none'
    };
    res.writeHead(200, headers);
    if (req.method === 'HEAD') { res.end(); return; }
    const stream = fs.createReadStream(safePath);
    stream.on('error', () => { try { res.destroy(); } catch (e) {} });
    stream.pipe(res);
  });
}

async function handleRequest(req, res) {
  let url;
  try {
    url = new URL(req.url, 'http://internal');
  } catch (e) {
    res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Bad request');
    return;
  }
  let pathName = url.pathname;
  try { pathName = decodeURIComponent(pathName); } catch (e) {}
  if (pathName.indexOf('\0') !== -1) {
    res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Bad request');
    return;
  }
  // POST /api/race-report —— 与 Worker 一致（无 CORS 头，返回纯文本 ok）
  if (pathName === '/api/race-report' && req.method === 'POST') {
    try {
      const raw = await readBody(req);
      let b = {};
      try { b = JSON.parse(raw || '{}') || {}; } catch (e2) {}
      store.insertRaceReport({
        ts: Date.now(),
        asn: 0,
        city: '',
        entry: String(b.entry || '').slice(0, 64),
        t_self: b.t_self | 0,
        t_alt: b.t_alt | 0,
        t_p1: b.t_p1 | 0, t_p2: b.t_p2 | 0, t_p3: b.t_p3 | 0,
        t_p4: b.t_p4 | 0, t_p5: b.t_p5 | 0, t_p6: b.t_p6 | 0,
        t_p7: b.t_p7 | 0, t_p8: b.t_p8 | 0, t_p9: b.t_p9 | 0
      });
    } catch (e) {}
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('ok');
    return;
  }
  if (pathName.startsWith('/api/')) {
    try {
      await handleApiRequest(req, res, url);
    } catch (e) {
      console.error('api error:', e);
      try { jsonRes(res, { error: 'internal' }, 500); } catch (e2) {}
    }
    return;
  }
  // 非 WebSocket 升级的 /ws 等 -> 静态查找（与 Worker ASSETS 行为一致，未命中 404）
  serveStatic(req, res, pathName);
}

/* ==========================================================================
 * WebSocket 升级路由（与 Worker 一致：无 roomId -> 大厅；有 roomId -> 房间）
 * ========================================================================*/
const server = http.createServer((req, res) => {
  handleRequest(req, res).catch((e) => {
    console.error('request error:', e);
    try { res.destroy(); } catch (e2) {}
  });
});

server.on('upgrade', (req, socket, head) => {
  try {
    const upgradeHeader = (req.headers['upgrade'] || '').toLowerCase();
    const key = req.headers['sec-websocket-key'];
    if (upgradeHeader !== 'websocket' || !key) {
      socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
      socket.destroy();
      return;
    }
    socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      'Sec-WebSocket-Accept: ' + wsAccept(key) + '\r\n' +
      '\r\n'
    );
    socket.setNoDelay(true);
    const conn = new WSConn(socket);
    if (head && head.length) conn._onData(head);
    let roomId = null;
    try {
      const u = new URL(req.url, 'http://internal');
      roomId = u.searchParams.get('roomId');
    } catch (e) {}
    if (roomId) {
      getRoom(roomId).handleConnection(conn);
    } else {
      handleLobbyWebSocket(conn);
    }
  } catch (e) {
    console.error('upgrade error:', e);
    try { socket.destroy(); } catch (e2) {}
  }
});

/* 定期维护：清理过期在线记录 / 房间注册表 / 房间状态（对齐 Worker 的查询窗口与 TTL） */
setInterval(() => {
  try {
    store.pruneOnline(3e5);      // 在线记录 5 分钟过期
    store.pruneRegistry(12e5);   // 房间列表 20 分钟窗口
    store.pruneRoomState(36e5);  // 房间状态 1 小时 TTL
  } catch (e) {}
}, 60e3).unref();

process.on('uncaughtException', (e) => { console.error('uncaughtException:', e); });
process.on('unhandledRejection', (e) => { console.error('unhandledRejection:', e); });
process.on('SIGTERM', () => { try { store.flush(); } catch (e) {} process.exit(0); });
process.on('SIGINT', () => { try { store.flush(); } catch (e) {} process.exit(0); });
process.on('exit', () => { try { store.flush(); } catch (e) {} });

store.load();
store.pruneOnline(3e5); // 启动时清掉上次进程遗留的在线记录

server.listen(PORT, HOST, () => {
  console.log('[xq] 中国象棋服务已启动: http://' + HOST + ':' + PORT + ' (dist=' + DIST_DIR + ')');
  if (!fs.existsSync(path.join(DIST_DIR, 'index.html'))) {
    console.warn('[xq] 警告: dist/index.html 不存在，请确认部署目录结构');
  }
});
