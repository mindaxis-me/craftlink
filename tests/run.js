const { CraftLink } = require('../bridge');
const assert = require('assert');
const zlib = require('zlib');

// Basic instantiation test
const link = new CraftLink({ url: 'ws://localhost:4800', token: 'test' });
assert.strictEqual(link.url, 'ws://localhost:4800');
assert.strictEqual(link.token, 'test');
assert.strictEqual(link.reconnect, true);

// Message parsing test
const events = [];
link.on('chunk', (d) => events.push(['chunk', d]));
link.on('entity', (d) => events.push(['entity', d]));
link.on('blockUpdate', (d) => events.push(['blockUpdate', d]));
link.on('sound', (d) => events.push(['sound', d]));

link._handleMessage(JSON.stringify({
  type: 'subchunk',
  x: 0,
  y: 4,
  z: 0,
  palette: [{ index: 0, name: 'minecraft:stone', sid: 1, states: {} }],
  indices: Buffer.from([0, 0, 0]).toString('base64'),
}));
link._handleMessage(JSON.stringify({ type: 'entityBatch', entities: [{ id: 1, name: 'zombie' }] }));
link._handleMessage(JSON.stringify({ type: 'blockUpdate', x: 1, y: 64, z: 2, sid: 42 }));
link._handleMessage(JSON.stringify({ type: 'sound', name: 'block.chest.open', x: 0, y: 64, z: 0 }));

const raw = Buffer.alloc(13 + (4096 * 4) + 2048 + 2048);
raw[0] = 0x01;
raw.writeInt32LE(3, 1);
raw.writeInt32LE(-2, 5);
raw.writeInt32LE(7, 9);
for (let i = 0; i < 4096; i++) {
  raw.writeInt32LE(i < 2 ? 5 : 0, 13 + (i * 4));
}
raw[13 + (4096 * 4)] = 0xC3;
raw[13 + (4096 * 4) + 2048] = 0x94;

const gzipFrame = Buffer.concat([Buffer.from([0x02]), zlib.gzipSync(raw)]);
link._handleMessage(gzipFrame);

assert.strictEqual(events.length, 5);
assert.deepStrictEqual(events[0], ['chunk', {
  type: 'subchunk',
  x: 0,
  y: 4,
  z: 0,
  palette: [{ index: 0, name: 'minecraft:stone', sid: 1, states: {} }],
  indices: Buffer.from([0, 0, 0]).toString('base64'),
  skyLight: null,
  blockLight: null,
}]);
assert.deepStrictEqual(events[1], ['entity', { id: 1, name: 'zombie' }]);
assert.deepStrictEqual(events[2], ['blockUpdate', {
  type: 'blockUpdate',
  x: 1,
  y: 64,
  z: 2,
  sid: 42,
  stateId: 42,
}]);
assert.deepStrictEqual(events[3], ['sound', {
  type: 'sound',
  name: 'block.chest.open',
  x: 0,
  y: 64,
  z: 0,
}]);
assert.deepStrictEqual(events[4][0], 'chunk');
assert.deepStrictEqual(events[4][1].type, 'subchunk');
assert.deepStrictEqual(events[4][1].x, 3);
assert.deepStrictEqual(events[4][1].y, 7);
assert.deepStrictEqual(events[4][1].z, -2);
assert.deepStrictEqual(events[4][1].palette, [
  { index: 0, name: null, sid: 5, states: null },
  { index: 1, name: null, sid: 0, states: null },
]);
assert.strictEqual(Buffer.from(events[4][1].indices, 'base64').length, 4096);
assert.strictEqual(events[4][1].skyLight, Buffer.from([0xC3, ...new Array(2047).fill(0)]).toString('base64'));
assert.strictEqual(events[4][1].blockLight, Buffer.from([0x94, ...new Array(2047).fill(0)]).toString('base64'));

// sendCommand without connection
assert.strictEqual(link.sendCommand('tp @s 0 64 0'), false);

console.log('craftlink tests passed');
