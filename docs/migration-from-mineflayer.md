# Migration from mineflayer

CraftLink and mineflayer solve different layers of the stack.

- mineflayer is a Minecraft client implementation
- CraftLink is a server-authorized data bridge

The migration path is easiest when you separate "read world state" from "act like a client".

## Concept mapping

| mineflayer | CraftLink |
| --- | --- |
| `bot.on('spawn')` | `link.on('connected')` for socket open, plus `raw` `ready` for initial snapshot complete |
| `bot.blockAt(pos)` | maintain your own block cache from `chunk` and `blockUpdate` |
| `bot.entities` | maintain your own entity map from `entity` and `entityGone` |
| `bot.time` | `time` event |
| `bot.isRaining` | `weather` event |
| `bot.inventory` | `raw` `inventory` and `equipment` messages |
| `bot.chat('/tp ...')` | `link.sendCommand('tp ...')` |
| `bot.dig/place/attack` | not part of the public JS bridge; use server commands, your own companion action layer, or a hybrid setup |

## What changes in your code

### 1. You own the world cache

mineflayer gives you query helpers on top of its own reconstructed world model. CraftLink gives you the stream earlier.

Typical replacement:

```js
const blocks = new Map();

link.on('chunk', (chunk) => {
  // decode and index the full 16x16x16 section
});

link.on('blockUpdate', (update) => {
  blocks.set(`${update.x},${update.y},${update.z}`, update);
});
```

### 2. You own the entity map

```js
const entities = new Map();

link.on('entity', (entity) => entities.set(entity.id, entity));
link.on('entityGone', ({ id }) => entities.delete(id));
```

### 3. Actions are different

CraftLink does not simulate a player. That means there is no built-in equivalent to:

- movement physics
- pathfinding
- raycast-based digging
- inventory clicking APIs
- combat timing

The current public bridge exposes `sendCommand()` only. Use it when your automation can be expressed as server commands.

## Constraints to plan for

### No pathfinder in CraftLink alone

If your mineflayer bot depends on `mineflayer-pathfinder`, you need another layer:

- keep mineflayer for movement and actions
- or build a server-side actor/plugin companion
- or use CraftLink only for read-heavy features such as viewer state and analytics

### No direct player-slot avatar

CraftLink does not join the server as a player, so anything that depends on the bot being a physical player entity must come from somewhere else.

### `connected` is not the same as "world is ready"

mineflayer's `spawn` usually implies a usable world snapshot shortly after. In CraftLink, wait for `raw` `ready` if your app needs the initial chunk sync first.

## Good migration patterns

### Pattern 1: replace read-heavy mineflayer code

Use CraftLink for:

- world viewers
- analytics dashboards
- mob and block monitors
- alerting and observability

Keep actions elsewhere.

### Pattern 2: hybrid mineflayer + CraftLink

This is the easiest upgrade path for existing bots.

Use mineflayer for:

- pathfinding
- inventory interaction
- combat
- client-authenticated actions

Use CraftLink for:

- faster chunk snapshots
- out-of-band entity tracking
- zero-slot spectator pipelines
- remote dashboards

### Pattern 3: command-driven automation

If your automation is already mostly command-based, replace the client entirely:

```js
link.sendCommand('tp YourPlayerName 0 120 0');
link.sendCommand('time set day');
link.sendCommand('say crop monitor online');
```

## When not to migrate

Stay on mineflayer alone if your core problem is:

- reproducing a real player
- pathfinding through the world
- anti-cheat-aware movement
- rich inventory and combat behavior

Use CraftLink when your core problem is:

- reading server state fast
- observing multiple consumers at once
- avoiding player-slot usage
- building viewers and analytics

