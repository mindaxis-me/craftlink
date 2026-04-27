const WebSocket = require('ws');
const EventEmitter = require('events');
const zlib = require('zlib');

const DEFAULT_RECONNECT_BASE_MS = 1000;
const DEFAULT_RECONNECT_MAX_MS = 30000;
const SECTION_BLOCK_COUNT = 4096;
const LIGHT_NIBBLE_ARRAY_SIZE = SECTION_BLOCK_COUNT / 2;
const BINARY_TYPE_SUBCHUNK = 0x01;
const BINARY_TYPE_SUBCHUNK_GZIP = 0x02;
const BINARY_HEADER_SIZE = 13;
const BINARY_STATE_PAYLOAD_SIZE = SECTION_BLOCK_COUNT * 4;
const BINARY_PAYLOAD_SIZE = BINARY_STATE_PAYLOAD_SIZE + (LIGHT_NIBBLE_ARRAY_SIZE * 2);
const BINARY_FRAME_SIZE = BINARY_HEADER_SIZE + BINARY_PAYLOAD_SIZE;

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
    const chunk = this._decodeBinaryChunk(raw);
    if (chunk) {
      this.emit('chunk', chunk);
      return;
    }

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
        this.emit('chunk', this._normalizeChunk(msg));
        break;
      case 'blockUpdate':
        if (msg.stateId == null && msg.sid != null) {
          msg.stateId = msg.sid;
        }
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

  _decodeBinaryChunk(raw) {
    const frame = this._inflateBinaryFrame(raw);
    if (!frame || frame.length < BINARY_FRAME_SIZE || frame[0] !== BINARY_TYPE_SUBCHUNK) {
      return null;
    }

    const x = frame.readInt32LE(1);
    const z = frame.readInt32LE(5);
    const y = frame.readInt32LE(9);
    const skyOffset = BINARY_HEADER_SIZE + BINARY_STATE_PAYLOAD_SIZE;
    const blockOffset = skyOffset + LIGHT_NIBBLE_ARRAY_SIZE;

    const palette = [];
    const paletteMap = new Map();
    const indices = new Array(SECTION_BLOCK_COUNT);
    for (let i = 0; i < SECTION_BLOCK_COUNT; i++) {
      const sid = frame.readInt32LE(BINARY_HEADER_SIZE + (i * 4));
      let paletteIndex = paletteMap.get(sid);
      if (paletteIndex == null) {
        paletteIndex = palette.length;
        paletteMap.set(sid, paletteIndex);
        palette.push({
          index: paletteIndex,
          name: null,
          sid,
          states: null,
        });
      }
      indices[i] = paletteIndex;
    }

    return {
      type: 'subchunk',
      x,
      y,
      z,
      palette,
      indices: this._encodeIndices(indices, palette.length),
      skyLight: frame.subarray(skyOffset, blockOffset).toString('base64'),
      blockLight: frame.subarray(blockOffset, blockOffset + LIGHT_NIBBLE_ARRAY_SIZE).toString('base64'),
    };
  }

  _inflateBinaryFrame(raw) {
    const buffer = this._toBuffer(raw);
    if (!buffer || buffer.length === 0) return null;

    if (buffer[0] === BINARY_TYPE_SUBCHUNK) {
      return buffer;
    }

    if (buffer[0] !== BINARY_TYPE_SUBCHUNK_GZIP || buffer.length === 1) {
      return null;
    }

    try {
      const inflated = zlib.gunzipSync(buffer.subarray(1));
      return inflated[0] === BINARY_TYPE_SUBCHUNK ? inflated : null;
    } catch (_) {
      return null;
    }
  }

  _toBuffer(raw) {
    if (Buffer.isBuffer(raw)) return raw;
    if (raw instanceof ArrayBuffer) return Buffer.from(raw);
    if (ArrayBuffer.isView(raw)) {
      return Buffer.from(raw.buffer, raw.byteOffset, raw.byteLength);
    }
    if (Array.isArray(raw)) {
      return Buffer.concat(raw.map((part) => this._toBuffer(part)).filter(Boolean));
    }
    return null;
  }

  _encodeIndices(indices, paletteLen) {
    if (paletteLen <= 256) {
      return Buffer.from(indices).toString('base64');
    }

    const buffer = Buffer.alloc(indices.length * 2);
    for (let i = 0; i < indices.length; i++) {
      buffer.writeUInt16LE(indices[i], i * 2);
    }
    return buffer.toString('base64');
  }

  _normalizeChunk(msg) {
    if (!msg || typeof msg !== 'object') return msg;
    if (!Array.isArray(msg.palette)) {
      msg.palette = [];
    }
    if (msg.skyLight === undefined) {
      msg.skyLight = null;
    }
    if (msg.blockLight === undefined) {
      msg.blockLight = null;
    }
    for (const entry of msg.palette) {
      if (!entry || typeof entry !== 'object') continue;
      if (entry.sid == null && entry.stateId != null) {
        entry.sid = entry.stateId;
      }
      if (entry.name === undefined) {
        entry.name = null;
      }
      if (entry.states === undefined) {
        entry.states = null;
      }
    }
    return msg;
  }
}

module.exports = { CraftLink };
