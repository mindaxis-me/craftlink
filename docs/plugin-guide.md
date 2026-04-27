# Plugin Guide

CraftLink ships a Paper/Spigot plugin that exposes Minecraft server state over WebSocket and optional `/dev/shm` mirrors for same-host consumers.

## Install

### Option 1: release JAR

1. Download the latest plugin JAR from the [GitHub Releases](https://github.com/mindaxis-me/craftlink/releases) page.
2. Copy it into your server's `plugins/` directory.
3. Start or restart the server.
4. Edit `plugins/MindAxisView/config.yml`.
5. Run `/craftlink start` from console, in game, or via RCON.

### Option 2: build from source

```bash
git clone https://github.com/mindaxis-me/craftlink.git
cd craftlink/plugin
./gradlew shadowJar
```

The built JAR will be under `plugin/build/libs/`.

## Configuration

Generated file: `plugins/MindAxisView/config.yml`

| Key | Default | Meaning |
| --- | --- | --- |
| `ws-port` | `4800` | TCP port for the embedded WebSocket server. |
| `anchor-player` | `""` | Optional player name used as the anchor camera and primary world source. Empty means auto-select the first online player. |
| `tracked-players` | `["*"]` | Which players define the streamed area. `"*"` means every online player in the anchor world. |
| `view-distance` | `8` | Chunk radius around tracked players for initial sync and chunk snapshot delivery. |
| `position-interval-ms` | `50` | How often anchor position, time, and `allPositions` are pushed. |
| `max-history` | `2048` | Per-server text and binary frame history used while new clients are completing initial sync. |
| `auth-token` | `""` | Optional bearer token. If set, WebSocket clients must send it as `Authorization: Bearer ...`, `?token=...`, or `?auth-token=...`. |
| `compress-binary-chunks` | `true` | Wrap binary subchunk frames in a gzip transport frame (`0x02`). Turn this off only if your client already handles raw `0x01` frames and you prefer lower CPU over bandwidth. |
| `shm-enabled` | `true` | Keep `/dev/shm` mirrors updated for same-host bots or analytics. Does not affect remote WebSocket clients. |

Example:

```yaml
ws-port: 4800
anchor-player: ""
tracked-players:
  - "*"
view-distance: 8
position-interval-ms: 50
max-history: 2048
auth-token: "replace-me"
compress-binary-chunks: true
shm-enabled: true
```

## Commands

The plugin's primary command is `/craftlink`. `/viewer` remains available as a compatibility alias.

| Command | Purpose |
| --- | --- |
| `/craftlink start` | Start the WebSocket server and begin streaming. |
| `/craftlink stop` | Stop the server and clear runtime state. |
| `/craftlink anchor <player>` | Change the anchor player. |
| `/craftlink status` | Show running status, anchor, clients, and port. |

Examples:

```text
/craftlink start
/craftlink anchor camera_bot
/craftlink status
```

### RCON examples

Any RCON client can issue the same commands:

```text
craftlink start
craftlink anchor camera_bot
craftlink status
craftlink stop
```

## Supported Minecraft versions

- Paper / Spigot `1.21.x`
- Current build targets Paper API `1.21.4`
- Java `21` is required to build from source

## What the plugin sends

- Initial chunk snapshot around the anchor and tracked players
- Live block updates
- Entity batches every 4 ticks
- Sounds, particles, block actions
- Container packets
- Scoreboard and bossbar updates
- Time, weather, position, status

## Troubleshooting

### Port already in use

Symptoms:

- `/craftlink start` fails
- server log mentions bind failure

Fix:

1. Change `ws-port` in `config.yml`
2. Restart the plugin or server
3. Confirm the new port is reachable from your bridge process

### Authentication errors

Symptoms:

- client handshake closes immediately
- log shows `Rejecting unauthorized WS client`

Fix:

1. Confirm `auth-token` matches exactly
2. If you use the JS bridge, pass `token` in `new CraftLink({ token })`
3. If you use a raw WebSocket client, send either `Authorization: Bearer <token>` or `?token=<token>`

### No `chunk` events on the client

Symptoms:

- connection succeeds
- `position` or `status` arrives
- chunk data does not

Fix:

1. Make sure you are using the official `craftlink` Node bridge or your own parser for binary frames `0x01` and `0x02`
2. Confirm an anchor player is online
3. Confirm the target area is within `view-distance`
4. If you filtered `tracked-players`, make sure those players are online and in the same world as the anchor

### Chunks look incomplete in a custom viewer

CraftLink sends subchunks, biomes, heightmaps, and block entities as separate messages. A viewer needs all of them for full fidelity.

Minimum checklist:

1. Handle `chunk`
2. Handle `biomes`
3. Handle `blockEntities`
4. Handle `raw` `heightmap`
5. Wait for `raw` `ready` before treating the initial snapshot as complete

### `/dev/shm` files do not appear

`shm-enabled` only helps on Linux hosts with writable `/dev/shm`. Remote bridges do not need it. If you run on macOS or Windows, keep using WebSocket.

