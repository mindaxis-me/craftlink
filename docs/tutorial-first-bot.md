# Tutorial: Your First Bot

This walkthrough installs the plugin, connects from Node.js, counts blocks from chunk data, watches mobs, and sends a command back to the server.

## 1. Install the plugin on Paper

Follow the steps in [plugin-guide.md](./plugin-guide.md), then start streaming:

```text
/craftlink start
```

For local testing, a simple config is enough:

```yaml
ws-port: 4800
auth-token: "dev-token"
tracked-players:
  - "*"
```

## 2. Install the npm package

```bash
npm install craftlink
```

## 3. Connect in 5 lines

```js
const { CraftLink } = require('craftlink');
const link = new CraftLink({ url: 'ws://127.0.0.1:4800', token: 'dev-token' });
link.on('connected', () => console.log('connected'));
link.on('error', console.error);
link.connect();
```

If you need the initial snapshot to finish before you start processing, also wait for the `ready` message on `raw`.

## 4. Receive chunk data and count blocks

CraftLink emits one `chunk` event per subchunk. The `indices` field is base64 over palette indexes.

```js
const { CraftLink } = require('craftlink');

function decodeIndices(chunk) {
  const raw = Buffer.from(chunk.indices, 'base64');
  const wide = chunk.palette.length > 256;
  const out = new Array(wide ? raw.length / 2 : raw.length);

  for (let i = 0; i < out.length; i++) {
    out[i] = wide ? raw.readUInt16LE(i * 2) : raw[i];
  }
  return out;
}

async function main() {
  const link = new CraftLink({ url: 'ws://127.0.0.1:4800', token: 'dev-token' });

  link.on('chunk', (chunk) => {
    const indices = decodeIndices(chunk);
    let solid = 0;

    for (const paletteIndex of indices) {
      const entry = chunk.palette[paletteIndex];
      const isAir = entry.sid === 0 || entry.name === 'minecraft:air';
      if (!isAir) solid++;
    }

    console.log(`subchunk ${chunk.x},${chunk.y},${chunk.z}: ${solid} non-air blocks`);
  });

  await link.connect();
}

main().catch(console.error);
```

## 5. Watch entities and print the mob list

```js
const { CraftLink } = require('craftlink');

async function main() {
  const link = new CraftLink({ url: 'ws://127.0.0.1:4800', token: 'dev-token' });
  const entities = new Map();

  link.on('entity', (entity) => {
    entities.set(entity.id, entity);
  });

  link.on('entityGone', ({ id }) => {
    entities.delete(id);
  });

  setInterval(() => {
    const mobs = [...entities.values()]
      .filter((entity) => entity.entityType === 'mob')
      .map((entity) => `${entity.name}@${entity.x.toFixed(1)},${entity.y.toFixed(1)},${entity.z.toFixed(1)}`);

    console.log(mobs.length ? mobs.join('\n') : 'no mobs tracked');
  }, 2000);

  await link.connect();
}

main().catch(console.error);
```

## 6. Send a command and teleport a player

`sendCommand()` writes a console command to the server through the authenticated socket.

```js
const { CraftLink } = require('craftlink');

async function main() {
  const link = new CraftLink({ url: 'ws://127.0.0.1:4800', token: 'dev-token' });
  await link.connect();

  const ok = link.sendCommand('tp YourPlayerName 0 120 0');
  console.log(ok ? 'command sent' : 'socket not open');
}

main().catch(console.error);
```

Replace `YourPlayerName` with an actual online Java Edition player.

## 7. Skeleton: auto-farm bot

CraftLink is strongest as a fast read layer. A minimal farm bot usually keeps its own world cache and reacts to block/entity updates.

```js
const { CraftLink } = require('craftlink');

async function main() {
  const link = new CraftLink({ url: 'ws://127.0.0.1:4800', token: 'dev-token' });
  const world = new Map();
  const mobs = new Map();

  link.on('blockUpdate', (block) => {
    world.set(`${block.x},${block.y},${block.z}`, block);
  });

  link.on('entity', (entity) => {
    if (entity.entityType === 'mob') mobs.set(entity.id, entity);
  });

  link.on('entityGone', ({ id }) => {
    mobs.delete(id);
  });

  setInterval(() => {
    const hostileNearby = [...mobs.values()].some((mob) =>
      ['zombie', 'skeleton', 'creeper'].includes(mob.name)
    );

    if (hostileNearby) {
      link.sendCommand('say hostile mob near the farm');
    }
  }, 1000);

  await link.connect();
}

main().catch(console.error);
```

The next layer is yours:

- add a crop-state cache from `chunk` and `blockUpdate`
- model your farm coordinates explicitly
- trigger server commands or your own companion plugin when action is needed

