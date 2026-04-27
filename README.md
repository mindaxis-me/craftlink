# CraftLink

Real-time Minecraft server data API. A faster, cleaner alternative to mineflayer.

CraftLink is a Paper/Spigot plugin + Node.js bridge that gives you direct access to your Minecraft server's internals — chunks, entities, blocks, sounds, particles — without the overhead of the MC protocol.

## Why not mineflayer?

mineflayer emulates a full MC client: protocol encryption, packet parsing, world reconstruction — all on a single JS thread. CraftLink skips all of that.

| | mineflayer | CraftLink |
|---|---|---|
| Data source | MC protocol (external client) | Server internals (plugin) |
| Chunk load | ~50ms (network + parse) | ~0.1ms (direct memory) |
| Player slots | 1 per bot | 0 |
| Event loop impact | High (sync packet parse) | Minimal (pre-formatted data) |
| Server permission | None needed (connects like a player) | Plugin installed (server-authorized) |

## Architecture

```
[MC Server]
  └─ CraftLink Plugin (Paper/Spigot)
       ├─ /dev/shm (local: sub-ms IPC)
       └─ WebSocket (remote: LAN/internet)
            └─ CraftLink Bridge (Node.js)
                 ├─ Your bot
                 ├─ Your viewer
                 └─ Your analytics
```

The plugin runs inside the server JVM with direct access to `ChunkSnapshot`, entity state, and block events. It serializes only what you need and sends it via WebSocket (remote) or shared memory (local).

## Quick Start

### 1. Install the plugin

Download `craftlink-plugin.jar` from [Releases](https://github.com/mindaxis-me/craftlink/releases) and drop it in your server's `plugins/` folder.

```yaml
# plugins/MindAxisView/config.yml
ws-port: 4800
auth-token: "your-secret-token"
compress-binary-chunks: true
shm-enabled: true
```

### 2. Install the bridge

```bash
npm install craftlink
```

### 3. Connect

```javascript
const { CraftLink } = require('craftlink');

async function main() {
  const link = new CraftLink({
    url: 'ws://your-server:4800',
    token: 'your-secret-token',
  });

  link.on('chunk', (chunk) => {
    console.log(`Subchunk ${chunk.x}, ${chunk.y}, ${chunk.z}: ${chunk.palette.length} palette entries`);
  });

  link.on('entity', (entity) => {
    console.log(`${entity.name} at ${entity.x}, ${entity.y}, ${entity.z}`);
  });

  link.on('blockUpdate', (update) => {
    console.log(`Block changed at ${update.x}, ${update.y}, ${update.z}`);
  });

  link.on('raw', (msg) => {
    if (msg.type === 'ready') {
      console.log(`Initial snapshot ready: ${msg.chunkCount} chunk columns`);
    }
  });

  await link.connect();
}

main().catch(console.error);
```

## Documentation

- [API Reference](./docs/api-reference.md)
- [Plugin Guide](./docs/plugin-guide.md)
- [Tutorial: Your First Bot](./docs/tutorial-first-bot.md)
- [Tutorial: Build a Viewer](./docs/tutorial-viewer.md)
- [Architecture](./docs/architecture.md)
- [Wire Protocol](./docs/wire-protocol.md)
- [Migration from mineflayer](./docs/migration-from-mineflayer.md)
- [FAQ](./docs/faq.md)

## Features

### Data Streams
- **Chunks** — Binary stateId arrays with lighting and biome data
- **Entities** — Position, equipment, metadata, animations (4-tick batched diffs)
- **Block Updates** — Real-time block changes
- **Block Entities** — Signs, banners, chests, skulls
- **Sounds & Particles** — All server-side audio and visual events
- **Time & Weather** — Day/night cycle, rain, thunder
- **Scoreboard & Boss Bar** — Objectives, scores, boss health
- **Container Events** — Chest/furnace/crafting table contents
- **Block Actions** — Chest open/close, piston, noteblock

### Transport
- **WebSocket** — Remote access with token auth + gzip compression
- **Shared Memory** (`/dev/shm`) — Sub-millisecond local IPC
- Auto-reconnect with exponential backoff

## Plugin Commands

```
/craftlink start          — Start WebSocket server
/craftlink stop           — Stop WebSocket server
/craftlink anchor <player> — Set camera anchor player
/craftlink status         — Show connection stats
/viewer ...               — Legacy alias
```

## Configuration

```yaml
# WebSocket server port
ws-port: 4800

# Bearer token for bridge/WebSocket clients
auth-token: ""

# Compress binary chunk frames with gzip
compress-binary-chunks: true

# Keep local /dev/shm mirrors for same-host acceleration
shm-enabled: true

# Players to track for multi-view
tracked-players:
  - "*"

# Max history buffer (subchunks kept for late-joining clients)
max-history: 2048
```

## Supported Versions

- Paper/Spigot 1.21.x

> **Bedrock Edition**: Bedrock Realm viewing is available as a managed service through [MindAxis View](https://view.mindaxis.me). Contact us for access.

## License

MIT
