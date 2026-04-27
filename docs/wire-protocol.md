# Wire Protocol

This document describes the transport between the CraftLink plugin and clients.

## WebSocket connection

Default endpoint:

```text
ws://<host>:4800
```

Authentication options:

- query string: `?token=<token>`
- query string: `?auth-token=<token>`
- header: `Authorization: Bearer <token>`

If `auth-token` is empty in the plugin config, the socket is open without authentication.

## Connection lifecycle

When a client connects:

1. the plugin captures loaded chunks around the anchor / tracked players
2. the plugin sends chunk snapshots
3. the plugin sends biomes, heightmaps, block entities, position, time, weather, status, overlays
4. the plugin sends `{"type":"ready","chunkCount":...,"ts":...}`
5. queued live frames are flushed

`connected` on the JS bridge only means the socket opened. `ready` means the initial sync is complete.

## Inbound client messages

The stock plugin currently accepts one inbound JSON message type:

```json
{ "type": "command", "command": "tp YourPlayerName 0 120 0" }
```

Behavior:

- command runs as the server console sender
- empty or malformed payloads are ignored
- there is no ack frame in the current protocol

## Binary frames

CraftLink uses two binary frame envelopes for subchunks.

### `0x01`: raw subchunk

Layout:

| Offset | Size | Type | Meaning |
| --- | --- | --- | --- |
| `0` | `1` | `uint8` | frame type = `0x01` |
| `1` | `4` | `int32 LE` | chunk X |
| `5` | `4` | `int32 LE` | chunk Z |
| `9` | `4` | `int32 LE` | section Y |
| `13` | `16384` | `4096 x int32 LE` | block state IDs |
| `16397` | `2048` | bytes | sky-light nibble array |
| `18445` | `2048` | bytes | block-light nibble array |

Total size: `20493` bytes.

Block index order is XZY:

```text
x = (i >> 8) & 0xF
z = (i >> 4) & 0xF
y = i & 0xF
```

### `0x02`: gzip-wrapped subchunk

Layout:

| Offset | Size | Type | Meaning |
| --- | --- | --- | --- |
| `0` | `1` | `uint8` | frame type = `0x02` |
| `1..N` | variable | gzip payload | compressed original `0x01` frame |

The official Node bridge inflates `0x02` automatically and emits a normalized `chunk` event.

## JSON message types

Current outbound JSON `type` values from the plugin:

- `subchunk` (JSON fallback only)
- `biomes`
- `heightmap`
- `blockEntities`
- `blockUpdate`
- `position`
- `time`
- `weather`
- `allPositions`
- `status`
- `entityBatch`
- `entityGone`
- `entityAnimation`
- `sound`
- `entitySound`
- `particle`
- `blockAction`
- `scoreboard`
- `bossbar`
- `containerOpen`
- `containerClose`
- `containerContent`
- `containerSlot`
- `inventory`
- `equipment`
- `skin`
- `ready`

The JS bridge promotes many of these to dedicated events. Unmapped types arrive through `raw`.

## JSON subchunk fallback format

If state-ID resolution is unavailable on the server, the plugin falls back to JSON chunk messages:

```json
{
  "type": "subchunk",
  "x": 0,
  "y": 4,
  "z": 0,
  "palette": [
    { "index": 0, "name": "minecraft:stone", "sid": 1, "states": {} }
  ],
  "indices": "AAAB..."
}
```

`indices` is base64 over palette indexes:

- `uint8` when palette length is `<= 256`
- `uint16LE` otherwise

## Biome section format

```json
{
  "type": "biomes",
  "x": 0,
  "y": 4,
  "z": 0,
  "biomes": [
    { "index": 0, "name": "minecraft:plains" }
  ],
  "biomeIndices": "AAAB..."
}
```

Biome indices describe a 4x4x4 lattice, also in XZY order.

## Entity batch format

Live entity updates are packed as:

```json
{
  "type": "entityBatch",
  "entities": [
    { "type": "entity", "...": "..." },
    { "type": "entityGone", "id": 123 }
  ]
}
```

The JS bridge fans that out into:

- one `entity` event per entity object
- one `entityGone` event for gone objects

## Delta delivery strategy

CraftLink mixes snapshot and diff delivery:

- chunks: full snapshot on connect, then sparse `blockUpdate`
- entities: periodic changed-entity batch every 4 ticks
- weather: only sent when the state changes
- position, time, allPositions: live stream
- overlays and containers: event-driven

The server keeps bounded history for queued initial-sync delivery, but the `ready` message is still the authoritative handoff point for clients.

