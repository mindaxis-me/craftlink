const WebSocket = require('ws');
const EventEmitter = require('events');

const DEFAULT_RECONNECT_BASE_MS = 1000;
const DEFAULT_RECONNECT_MAX_MS = 30000;

class CraftLink extends EventEmitter {
  constructor(options = {}) {
    super();
    this.url = String(options.url || 'ws://localhost:4800');
    this.token = String(options.token || '');
    this.reconnect = options.reconnect !== false;
    this.reconnectBaseMs = Number(options.reconnectBaseMs || DEFAULT_RECONNECT_BASE_MS);
    this.reconnectMaxMs = Number(options.reconnectMaxMs || DEFAULT_RECONNECT_MAX_MS);
    this._ws = null;
    this._reconnectAttempt = 0;
    this._closed = false;
  }

  async connect() {
    return new Promise((resolve, reject) => {
      const wsUrl = this.token
        ? `${this.url}?token=${encodeURIComponent(this.token)}`
        : this.url;
      const ws = new WebSocket(wsUrl);
      this._ws = ws;

      ws.on('open', () => {
        this._reconnectAttempt = 0;
        this.emit('connected');
        resolve();
      });

      ws.on('message', (data) => {
        this._handleMessage(data);
      });

      ws.on('close', (code, reason) => {
        this.emit('disconnected', { code, reason: String(reason || '') });
        if (this.reconnect && !this._closed) {
          this._scheduleReconnect();
        }
      });

      ws.on('error', (err) => {
        this.emit('error', err);
        if (!ws._opened) reject(err);
      });

      ws._opened = false;
      const origOnOpen = ws.onopen;
      ws.on('open', () => { ws._opened = true; });
    });
  }

  disconnect() {
    this._closed = true;
    if (this._ws) {
      this._ws.close();
      this._ws = null;
    }
  }

  sendCommand(command) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return false;
    this._ws.send(JSON.stringify({ type: 'command', command }));
    return true;
  }

  _scheduleReconnect() {
    const delay = Math.min(
      this.reconnectBaseMs * Math.pow(2, this._reconnectAttempt),
      this.reconnectMaxMs
    );
    this._reconnectAttempt++;
    setTimeout(() => {
      if (this._closed) return;
      this.connect().catch(() => {});
    }, delay);
  }

  _handleMessage(raw) {
    let msg;
    try {
      msg = typeof raw === 'string' ? JSON.parse(raw) : JSON.parse(raw.toString());
    } catch (_) {
      return;
    }

    const type = msg.type;
    if (!type) return;

    switch (type) {
      case 'subchunk':
        this.emit('chunk', msg);
        break;
      case 'blockUpdate':
        this.emit('blockUpdate', msg);
        break;
      case 'entityBatch':
        if (Array.isArray(msg.entities)) {
          for (const entity of msg.entities) {
            this.emit('entity', entity);
          }
        }
        break;
      case 'entityGone':
        this.emit('entityGone', msg);
        break;
      case 'entityAnimation':
        this.emit('entityAnimation', msg);
        break;
      case 'sound':
        this.emit('sound', msg);
        break;
      case 'particle':
        this.emit('particle', msg);
        break;
      case 'blockAction':
        this.emit('blockAction', msg);
        break;
      case 'blockEntities':
        this.emit('blockEntities', msg);
        break;
      case 'time':
        this.emit('time', msg);
        break;
      case 'weather':
        this.emit('weather', msg);
        break;
      case 'scoreboard':
        this.emit('scoreboard', msg);
        break;
      case 'bossbar':
        this.emit('bossbar', msg);
        break;
      case 'containerOpen':
        this.emit('containerOpen', msg);
        break;
      case 'containerClose':
        this.emit('containerClose', msg);
        break;
      case 'containerContent':
        this.emit('containerContent', msg);
        break;
      case 'containerSlot':
        this.emit('containerSlot', msg);
        break;
      case 'biomes':
        this.emit('biomes', msg);
        break;
      case 'position':
        this.emit('position', msg);
        break;
      case 'allPositions':
        this.emit('allPositions', msg);
        break;
      case 'status':
        this.emit('status', msg);
        break;
      default:
        this.emit('raw', msg);
        break;
    }
  }
}

module.exports = { CraftLink };
