# API Reference

`craftlink` exports a single class: `CraftLink`.

```js
const { CraftLink } = require('craftlink');
```

If you use TypeScript, the package now ships declarations in [`bridge/index.d.ts`](../bridge/index.d.ts).

## `new CraftLink(options)`

```ts
new CraftLink({
  url?: string,
  token?: string,
  reconnect?: boolean,
  reconnectBaseMs?: number,
  reconnectMaxMs?: number
})
```

Options:

| Option | Type | Default | Notes |
| --- | --- | --- | --- |
| `url` | `string` | `ws://localhost:4800` | WebSocket endpoint exposed by the plugin. |
| `token` | `string` | `""` | Added as `?token=...` on connect. Must match `auth-token` in the plugin config if set. |
| `reconnect` | `boolean` | `true` | Enables automatic reconnect after socket close. |
| `reconnectBaseMs` | `number` | `1000` | Initial reconnect delay. |
| `reconnectMaxMs` | `number` | `30000` | Maximum reconnect delay. |

## Methods

### `await link.connect()`

Opens the WebSocket connection and resolves when the socket is open.

`connected` fires immediately after the socket opens. If you need the initial chunk snapshot to be fully delivered first, also watch `raw` for the `ready` message.

### `link.disconnect()`

Closes the socket and disables automatic reconnect for this instance.

### `link.sendCommand(command)`

```ts
sendCommand(command: string): boolean
```

Sends an inbound `{"type":"command","command":"..."}` message to the plugin.

- Returns `false` if the socket is not currently open.
- Returns `true` if the command was written to the socket.
- On the plugin side, the command is executed as the server console sender.
- This is fire-and-forget. There is no built-in success response event.

## Event Summary

```js
const link = new CraftLink({ url, token });

link.on('connected', () => {});
link.on('disconnected', ({ code, reason }) => {});
link.on('error', (err) => {});
link.on('chunk', (chunk) => {});
link.on('blockUpdate', (update) => {});
link.on('entity', (entity) => {});
link.on('entityGone', ({ id }) => {});
link.on('entityAnimation', ({ id, animation }) => {});
link.on('sound', (sound) => {});
link.on('particle', (particle) => {});
link.on('blockAction', (action) => {});
link.on('blockEntities', (payload) => {});
link.on('time', (payload) => {});
link.on('weather', (payload) => {});
link.on('scoreboard', (payload) => {});
link.on('bossbar', (payload) => {});
link.on('containerOpen', (payload) => {});
link.on('containerClose', (payload) => {});
link.on('containerContent', (payload) => {});
link.on('containerSlot', (payload) => {});
link.on('biomes', (payload) => {});
link.on('position', (payload) => {});
link.on('allPositions', (payload) => {});
link.on('status', (payload) => {});
link.on('raw', (payload) => {});
```

## Event Payloads

### `connected`

No payload.

### `disconnected`

```ts
{ code: number, reason: string }
```

### `error`

Node `Error`.

### `chunk`

Fires once per 16x16x16 section, not once per chunk column.

```ts
{
  type: 'subchunk',
  x: number,
  y: number,
  z: number,
  palette: Array<{
    index: number,
    name: string | null,
    sid: number,
    states: Record<string, string> | null
  }>,
  indices: string,
  skyLight: string | null,
  blockLight: string | null
}
```

Notes:

- `x` and `z` are chunk coordinates.
- `y` is the section Y, not a block Y.
- `indices` is base64. Decode as `uint8` if `palette.length <= 256`, otherwise `uint16LE`.
- `skyLight` and `blockLight` are base64-encoded 2048-byte nibble arrays when the plugin used binary chunk transport.
- When the plugin falls back to JSON chunk messages, `skyLight` and `blockLight` are `null`.
- For binary transport, `palette[*].name` and `palette[*].states` are `null` because the wire frame only carries `sid`.

### `blockUpdate`

```ts
{
  type: 'blockUpdate',
  x: number,
  y: number,
  z: number,
  name?: string,
  sid?: number,
  stateId?: number,
  states?: Record<string, string>
}
```

`stateId` is a bridge-side alias for `sid`. Prefer `stateId` in application code.

### `entity`

```ts
{
  type: 'entity',
  id: number,
  name: string,
  entityType: 'player' | 'mob' | 'object',
  username?: string,
  itemName?: string,
  x: number,
  y: number,
  z: number,
  pos: { x: number, y: number, z: number },
  yaw: number,
  pitch: number,
  vx: number,
  vy: number,
  vz: number,
  velocity: { x: number, y: number, z: number },
  metadata: number[],
  equipment?: Array<{ name: string } | null>,
  health?: number,
  isUsingItem?: boolean
}
```

Notes:

- `entityType` is a coarse category.
- `name` is the Bukkit entity type name such as `zombie`, `player`, or `item`.
- `metadata[0]` is the packed entity flags byte mirrored from the plugin.
- `equipment` is ordered as `mainHand`, `offHand`, `helmet`, `chestplate`, `leggings`, `boots`.

### `entityGone`

```ts
{ type: 'entityGone', id: number }
```

### `entityAnimation`

```ts
{ type: 'entityAnimation', id: number, animation: string }
```

Current values emitted by the plugin are `hurt`, `death`, and `swingArm`.

### `sound`

```ts
{
  type: 'sound',
  name: string,
  x: number,
  y: number,
  z: number,
  volume: number,
  pitch: number,
  category: string
}
```

### `particle`

```ts
{
  type: 'particle',
  particle: string,
  x: number,
  y: number,
  z: number,
  dx: number,
  dy: number,
  dz: number,
  count: number,
  speed: number,
  data: {
    stateId?: number,
    blockStateId?: number,
    [key: string]: unknown
  }
}
```

### `blockAction`

```ts
{
  type: 'blockAction',
  x: number,
  y: number,
  z: number,
  actionId: number,
  actionParam: number,
  blockId: number
}
```

### `blockEntities`

```ts
{
  type: 'blockEntities',
  chunk: [number, number],
  entities: Array<{
    x: number,
    y: number,
    z: number,
    id: string,
    data: Record<string, unknown>
  }>
}
```

Tracked block entity families currently include chests, trapped chests, enchanting tables, signs, banners, skulls/heads, and beds.

### `time`

```ts
{ type: 'time', timeOfDay: number, age: number }
```

### `weather`

```ts
{
  type: 'weather',
  isRaining: boolean,
  isThundering: boolean,
  rainLevel: number
}
```

### `scoreboard`

```ts
type ScoreboardMessage =
  | { type: 'scoreboard', action: 'setObjective', name: string, displayName: string, renderType: string }
  | { type: 'scoreboard', action: 'removeObjective', name: string }
  | { type: 'scoreboard', action: 'setScore', objective: string, entry: string, value: number, displayName: string | null }
  | { type: 'scoreboard', action: 'removeScore', objective: string | null, entry: string }
  | { type: 'scoreboard', action: 'setDisplay', slot: string, objective: string | null }
```

Typical `slot` values are `sidebar`, `list`, `below_name`, or `sidebar.team.<color>`.

### `bossbar`

```ts
type BossbarMessage =
  | { type: 'bossbar', action: 'add', id: string, name: string, progress: number, color: string, division: string, darkenSky: boolean, playBossMusic: boolean, createWorldFog: boolean }
  | { type: 'bossbar', action: 'remove', id: string }
  | { type: 'bossbar', action: 'updateProgress', id: string, progress: number }
  | { type: 'bossbar', action: 'updateName', id: string, name: string }
  | { type: 'bossbar', action: 'updateStyle', id: string, color: string, division: string }
  | { type: 'bossbar', action: 'updateProperties', id: string, darkenSky: boolean, playBossMusic: boolean, createWorldFog: boolean }
```

### `containerOpen`

```ts
{
  type: 'containerOpen',
  windowId: number,
  containerType: string,
  title: string,
  slots: number
}
```

### `containerClose`

```ts
{ type: 'containerClose', windowId: number }
```

### `containerContent`

```ts
{
  type: 'containerContent',
  windowId: number,
  items: Array<{
    slot: number,
    id: string,
    count: number,
    damage?: number,
    displayName?: string,
    enchantments?: Array<{ id: string, level: number }>,
    nbt: Record<string, unknown>
  } | null>
}
```

### `containerSlot`

```ts
{
  type: 'containerSlot',
  windowId: number,
  slot: number,
  item: {
    slot: number,
    id: string,
    count: number,
    damage?: number,
    displayName?: string,
    enchantments?: Array<{ id: string, level: number }>,
    nbt: Record<string, unknown>
  } | null
}
```

### `biomes`

```ts
{
  type: 'biomes',
  x: number,
  y: number,
  z: number,
  biomes: Array<{ index: number, name: string }>,
  biomeIndices: string
}
```

`biomeIndices` is base64 over a 4x4x4 lattice, also in XZY order.

### `position`

```ts
{ type: 'position', x: number, y: number, z: number, yaw: number, pitch: number }
```

This is the anchor player position.

### `allPositions`

```ts
{
  type: 'allPositions',
  players: Array<{
    id: string,
    name: string,
    world: string,
    x: number,
    y: number,
    z: number,
    yaw: number,
    pitch: number,
    isAnchor: boolean
  }>
}
```

### `status`

```ts
{ type: 'status', health: number, food: number, experience: number, level: number }
```

### `raw`

Receives any plugin message type that the JS bridge does not promote to a dedicated event.

Current common payloads are:

- `ready`: `{ type: 'ready', chunkCount: number, ts: number }`
- `heightmap`: `{ type: 'heightmap', x: number, z: number, heights: number[] }`
- `inventory`: `{ type: 'inventory', selectedSlot: number, slots: [...] }`
- `equipment`: `{ type: 'equipment', mainHand, offHand, helmet, chestplate, leggings, boots }`
- `skin`: `{ type: 'skin', username: string, skinUrl?: string }`
- `entitySound`: `{ type: 'entitySound', name, entityId, volume, pitch, category }`

If you are building a full viewer, always listen to `raw` in addition to the dedicated events.

