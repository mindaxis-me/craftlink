# FAQ

## How is CraftLink different from mineflayer?

mineflayer is a client. CraftLink is a server plugin plus bridge.

mineflayer connects like a normal player and reconstructs the world from protocol packets. CraftLink reads the world directly inside Paper and streams it outward.

Use mineflayer when you need a player-like actor. Use CraftLink when you need fast server-authorized reads, viewer data, analytics, or zero-slot observers.

## Does it consume a player slot?

No. The plugin runs inside the server and the bridge connects to the plugin's WebSocket endpoint, not to the Minecraft login server.

## Does it support Bedrock Edition?

The OSS repository targets Minecraft Java Edition on Paper / Spigot.

Bedrock Realm support is not documented or shipped here. If you need that workflow, use the managed MindAxis View service instead of trying to infer private implementation details from this repo.

## What is the server-side performance impact?

The plugin does add work, but it avoids the full cost of a separate external client.

The main cost centers are:

- serializing initial chunk snapshots
- broadcasting entity diffs
- intercepting sounds, particles, overlays, and containers

In exchange, you avoid a full mineflayer-style client session and the JavaScript-side packet/world reconstruction that comes with it.

## Can multiple clients connect at the same time?

Yes. The WebSocket server broadcasts to all connected clients.

Typical use cases:

- one bot plus one viewer
- multiple dashboards
- one same-host local consumer plus remote observers

## Does it support SSL/TLS?

Not natively inside the plugin.

Recommended pattern:

1. keep the plugin on a private port
2. set `auth-token`
3. put TLS termination in front with nginx, Caddy, a cloud tunnel, or another reverse proxy

## Do I have to use `/dev/shm`?

No. `/dev/shm` is optional and only useful for same-host Linux consumers. Remote apps should use WebSocket.

## Can I build a full bot with just CraftLink?

Only if your actions can be expressed through server commands or your own companion layer.

The public JS bridge gives you excellent read access and a `sendCommand()` write path. It is not a full replacement for pathfinding, physics, inventory clicking, or melee automation by itself.

