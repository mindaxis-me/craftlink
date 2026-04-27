const { CraftLink } = require('../bridge');
const assert = require('assert');

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

link._handleMessage(JSON.stringify({ type: 'subchunk', x: 0, z: 0 }));
link._handleMessage(JSON.stringify({ type: 'entityBatch', entities: [{ id: 1, name: 'zombie' }] }));
link._handleMessage(JSON.stringify({ type: 'blockUpdate', x: 1, y: 64, z: 2, stateId: 42 }));
link._handleMessage(JSON.stringify({ type: 'sound', name: 'block.chest.open', x: 0, y: 64, z: 0 }));

assert.deepStrictEqual(events, [
  ['chunk', { type: 'subchunk', x: 0, z: 0 }],
  ['entity', { id: 1, name: 'zombie' }],
  ['blockUpdate', { type: 'blockUpdate', x: 1, y: 64, z: 2, stateId: 42 }],
  ['sound', { type: 'sound', name: 'block.chest.open', x: 0, y: 64, z: 0 }],
]);

// sendCommand without connection
assert.strictEqual(link.sendCommand('tp @s 0 64 0'), false);

console.log('craftlink tests passed');
