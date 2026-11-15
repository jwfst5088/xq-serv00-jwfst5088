class SocketClient {
  constructor(options = {}) {
    this.options = options;
    this.url = options.url || (window.location.protocol === 'https:' ? 'wss://' + window.location.host + '/ws' : 'ws://' + window.location.host + '/ws');
    this.baseUrl = this.url.split('?')[0];
    this.roomId = options.roomId || (this.url.includes('roomId=') ? this.url.split('roomId=')[1].split('&')[0] : null);
    this.ws = null;
    this.callbacks = {};
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = options.reconnectionAttempts || Infinity;
    this.reconnectDelay = options.reconnectionDelay || 1000;
    this.reconnection = options.reconnection !== false;
    this.connected = false;
    this.connecting = false;
    this.connectTimeout = options.timeout || 3000;
    this.pingInterval = options.pingInterval || 4000;
    this.pingTimeout = options.pingTimeout || 8000;
    this._connectTimer = null;
    this._pingTimer = null;
    this._pongTimer = null;
    this._reconnectTimer = null;
    this._lastPong = 0;
    console.log('SocketClient URL:', this.url, 'roomId:', this.roomId);
  }

  on(event, callback) {
    if (!this.callbacks[event]) {
      this.callbacks[event] = [];
    }
    this.callbacks[event].push(callback);
  }

  off(event, callback) {
    if (!this.callbacks[event]) return;
    this.callbacks[event] = this.callbacks[event].filter(cb => cb !== callback);
  }

  emit(event, data = {}) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.warn('WebSocket not connected');
      return;
    }
    this.ws.send(JSON.stringify({ event, payload: data }));
  }

  _startPing() {
    this._stopPing();
    this._lastPong = Date.now();
    this._pingTimer = setInterval(() => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
      // 判定连接已死：连续 pingTimeout 内没收到任何 pong。
      // 旧实现每次发 ping 都 clearTimeout+重设 pongTimer，而 pingInterval(4s) < pingTimeout(8s)，
      // 计时器永远被刷新、永不到期 —— 网络静默断开时前端永远发现不了，也就永远不会重连。
      if (Date.now() - this._lastPong > this.pingTimeout) {
        this._forceReconnect('pong_timeout');
        return;
      }
      try { this.ws.send(JSON.stringify({ event: 'ping' })); } catch (e) {}
    }, this.pingInterval);
  }

  // 主动判死并重建连接：网络静默（拔网线/切 WiFi/丢包）时浏览器不会及时触发 onclose，
  // 不能只 ws.close() 干等 TCP 超时，否则表现就是"断线后再也回不来"。
  _forceReconnect(reason) {
    if (this._forceReconnecting) return;
    this._forceReconnecting = true;
    const dead = this.ws;
    this.ws = null;
    if (dead) {
      dead.onclose = null;
      dead.onerror = null;
      dead.onmessage = null;
      try { dead.close(); } catch (e) {}
    }
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._pongTimer) { clearTimeout(this._pongTimer); this._pongTimer = null; }
    this.connected = false;
    this.connecting = false;
    console.warn('SocketClient 判定连接已死(' + reason + ')，主动重建');
    this._trigger('disconnect');
    if (this.reconnection) {
      if (this._reconnectTimer) clearTimeout(this._reconnectTimer);
      this._reconnectTimer = setTimeout(() => {
        this._reconnectTimer = null;
        this._forceReconnecting = false;
        this.reconnectAttempts++;
        this.connect();
      }, this.reconnectDelay);
    } else {
      this._forceReconnecting = false;
    }
  }

  _stopPing() {
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._pongTimer) { clearTimeout(this._pongTimer); this._pongTimer = null; }
    if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
  }

  connect() {
    if (this.ws) {
      this._stopPing();
      if (this._connectTimer) { clearTimeout(this._connectTimer); this._connectTimer = null; }
      this.ws.onclose = null;
      this.ws.onerror = null;
      this.ws.onmessage = null;
      try { this.ws.close(); } catch (e) {}
      this.ws = null;
    }

    this.connecting = true;
    this._trigger('connecting');

    this.ws = new WebSocket(this.url);

    if (this._connectTimer) clearTimeout(this._connectTimer);
    this._connectTimer = setTimeout(() => {
      if (this.ws && this.ws.readyState !== WebSocket.OPEN) {
        try { this.ws.close(); } catch (e) {}
      }
    }, this.connectTimeout);

    this.ws.onopen = () => {
      if (this._connectTimer) { clearTimeout(this._connectTimer); this._connectTimer = null; }
      this.connected = true;
      this.connecting = false;
      this._forceReconnecting = false;
      this.reconnectAttempts = 0;
      this._lastPong = Date.now();
      this._startPing();
      console.log('WebSocket connection opened');
      this._trigger('connect');
    };

    this.ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.event === 'pong') {
          this._lastPong = Date.now();
          if (this._pongTimer) { clearTimeout(this._pongTimer); this._pongTimer = null; }
          return;
        }
        if (data.event === 'ping') {
          try { this.ws.send(JSON.stringify({ event: 'pong' })); } catch (e) {}
          return;
        }
        if (data.event) {
          this._trigger(data.event, data.data);
        }
      } catch (e) {
        console.error('Failed to parse WebSocket message:', e);
      }
    };

    this.ws.onerror = (error) => {
      this.connected = false;
      this.connecting = false;
      this._stopPing();
      if (this._connectTimer) { clearTimeout(this._connectTimer); this._connectTimer = null; }
      console.error('WebSocket error:', error);
      this._trigger('connect_error');
    };

    this.ws.onclose = () => {
      this.connected = false;
      this.connecting = false;
      this._stopPing();
      if (this._connectTimer) { clearTimeout(this._connectTimer); this._connectTimer = null; }
      this._trigger('disconnect');

      if (this.reconnection && this.reconnectAttempts < this.maxReconnectAttempts) {
        this._reconnectTimer = setTimeout(() => {
          this._reconnectTimer = null;
          this.reconnectAttempts++;
          this.connect();
        }, this.reconnectDelay);
      }
    };
  }

  disconnect() {
    this.reconnection = false;
    this._stopPing();
    if (this._connectTimer) { clearTimeout(this._connectTimer); this._connectTimer = null; }
    if (this.ws) {
      this.ws.onclose = null;
      this.ws.onerror = null;
      this.ws.onmessage = null;
      try { this.ws.close(); } catch (e) {}
      this.ws = null;
    }
    this.connected = false;
    this.connecting = false;
  }

  reconnectToRoom(roomId) {
    this.roomId = roomId;
    this.url = this.baseUrl + '?roomId=' + roomId;
    const oldCallbacks = { ...this.callbacks };
    const oldWs = this.ws;
    this.ws = null;
    this.connected = false;
    this.connecting = false;
    this.callbacks = oldCallbacks;
    this._stopPing();
    if (this._connectTimer) { clearTimeout(this._connectTimer); this._connectTimer = null; }
    if (oldWs) {
      oldWs.onclose = null;
      oldWs.onerror = null;
      oldWs.onmessage = null;
      try { oldWs.close(); } catch (e) {}
    }
    this.reconnection = true;
    this.connect();
  }

  _trigger(event, data) {
    if (this.callbacks[event]) {
      this.callbacks[event].forEach(callback => callback(data));
    }
  }
}

const io = function(options) {
  const client = new SocketClient(options || {});
  client.connect();
  return client;
};