package me.mindaxis.view;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import net.kyori.adventure.bossbar.BossBar;
import net.kyori.adventure.bossbar.BossBarImplementation;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import org.bukkit.Bukkit;
import org.bukkit.Chunk;
import org.bukkit.ChunkSnapshot;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.BlockFace;
import org.bukkit.entity.Entity;
import org.bukkit.entity.Item;
import org.bukkit.entity.LivingEntity;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.HandlerList;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockFadeEvent;
import org.bukkit.event.block.BlockFormEvent;
import org.bukkit.event.block.BlockGrowEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.event.block.BlockSpreadEvent;
import org.bukkit.event.block.SignChangeEvent;
import org.bukkit.event.block.EntityBlockFormEvent;
import org.bukkit.event.block.FluidLevelChangeEvent;
import org.bukkit.event.block.LeavesDecayEvent;
import org.bukkit.event.entity.EntityDamageEvent;
import org.bukkit.event.entity.EntityDeathEvent;
import org.bukkit.event.entity.EntityChangeBlockEvent;
import org.bukkit.event.entity.PlayerDeathEvent;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.event.player.PlayerAnimationEvent;
import org.bukkit.event.player.PlayerItemHeldEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerRespawnEvent;
import org.bukkit.event.world.ChunkLoadEvent;
import org.bukkit.event.world.TimeSkipEvent;
import org.bukkit.event.weather.ThunderChangeEvent;
import org.bukkit.event.weather.WeatherChangeEvent;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.PlayerInventory;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.profile.PlayerTextures;
import org.bukkit.scheduler.BukkitTask;
import org.bukkit.scoreboard.DisplaySlot;
import org.bukkit.scoreboard.Objective;
import org.bukkit.scoreboard.Scoreboard;
import org.java_websocket.WebSocket;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MindAxis View Plugin - streams chunk data via WebSocket for the MindAxis viewer.
 *
 * Captures ChunkSnapshot on the main thread, then serializes + broadcasts
 * asynchronously to avoid blocking the server tick.
 */
public class MindAxisViewPlugin extends JavaPlugin implements Listener {

    private WSServer wsServer;
    private boolean running = false;
    private int wsPort;
    private int viewDistance;
    private int positionIntervalMs;
    private int maxHistory;
    private boolean compressBinaryChunks;
    private boolean shmEnabled;
    private String wsAuthToken;
    private List<String> trackedPlayerNames = new ArrayList<>();

    private Player anchorPlayer;
    private BukkitTask positionTask;
    private BukkitTask statusTask;
    private BukkitTask entityTask;
    private BukkitTask actionTask;

    // Entity animation event file paths
    private static final Path ENTITY_EVENTS_PATH = Paths.get("/dev/shm/mindaxis-entity-events.json");
    private static final Path ENTITY_EVENTS_TMP_PATH = Paths.get("/dev/shm/mindaxis-entity-events.json.tmp");
    // Buffer for entity animation events (flushed every 4 ticks with entity broadcast)
    private final java.util.concurrent.ConcurrentLinkedQueue<int[]> pendingAnimationEvents = new java.util.concurrent.ConcurrentLinkedQueue<>();
    // animation type constants: 0=hurt, 1=death, 2=swingArm

    // NMS packet interceptor handler names (Netty pipeline)
    private static final String SOUND_HANDLER_NAME = "mindaxis-sound-interceptor";
    private static final String BLOCK_EVENT_HANDLER_NAME = "mindaxis-block-event-interceptor";
    private static final String PARTICLE_HANDLER_NAME = "mindaxis-particle-interceptor";
    private static final String OVERLAY_HANDLER_NAME = "mindaxis-overlay-interceptor";
    private static final String CONTAINER_HANDLER_NAME = "mindaxis-container-interceptor";

    // Action request/result file paths
    private static final Path ACTION_REQUEST_PATH = Paths.get("/dev/shm/mindaxis-action-request.json");
    private static final Path ACTION_REQUEST_TMP_PATH = Paths.get("/dev/shm/mindaxis-action-request.json.tmp");
    private static final Path ACTION_RESULT_PATH = Paths.get("/dev/shm/mindaxis-action-result.json");
    private static final Path ACTION_RESULT_TMP_PATH = Paths.get("/dev/shm/mindaxis-action-result.json.tmp");
    private String lastProcessedActionId = "";

    // Track which chunks have been sent to avoid duplicates
    private final Set<Long> sentChunks = new HashSet<>();

    // Track chunk keys written to /dev/shm for manifest updates (thread-safe)
    private final Set<String> shmManifestEntries = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Track last sent values to avoid redundant messages
    private String lastInventoryHash = "";
    private String lastEquipmentHash = "";
    private double lastHealth = -1;
    private int lastFood = -1;
    private float lastExp = -1;
    private int lastLevel = -1;

    // Track last weather state to only send on change
    private boolean lastIsRaining = false;
    private boolean lastIsThundering = false;

    // Entity tracking state
    private static final double ENTITY_RANGE = 96.0;
    private static final double ENTITY_POS_THRESHOLD = 0.1;
    private static final double ENTITY_YAW_THRESHOLD = 5.0;
    private final Map<Integer, double[]> lastEntityState = new HashMap<>(); // entityId -> [x, y, z, yaw, pitch]
    private final Map<Integer, String> lastEntitySignature = new HashMap<>();
    private final Set<Integer> previousEntityIds = new HashSet<>();
    private final Object overlayStateLock = new Object();
    private final Map<String, ScoreboardObjectiveState> scoreboardObjectives = new LinkedHashMap<>();
    private final Map<String, Map<String, ScoreboardScoreState>> scoreboardScores = new LinkedHashMap<>();
    private final Map<String, String> scoreboardDisplays = new LinkedHashMap<>();
    private final Map<String, BossBarState> bossBars = new LinkedHashMap<>();

    @Override
    public void onEnable() {
        saveDefaultConfig();
        wsPort = getConfig().getInt("ws-port", 4800);
        viewDistance = getConfig().getInt("view-distance", 8);
        positionIntervalMs = getConfig().getInt("position-interval-ms", 50);
        maxHistory = getConfig().getInt("max-history", 2048);
        compressBinaryChunks = getConfig().getBoolean("compress-binary-chunks", true);
        shmEnabled = getConfig().getBoolean("shm-enabled", true);
        wsAuthToken = getConfig().getString("auth-token", "").trim();
        trackedPlayerNames = new ArrayList<>(getConfig().getStringList("tracked-players"));
        if (trackedPlayerNames.isEmpty()) trackedPlayerNames.add("*");

        // Register command
        ViewerCommand cmd = new ViewerCommand(this);
        getCommand("viewer").setExecutor(cmd);
        getCommand("viewer").setTabCompleter(cmd);

        // Auto-start if configured anchor player is set
        String defaultAnchor = getConfig().getString("anchor-player", "");
        if (!defaultAnchor.isEmpty()) {
            getLogger().info("Default anchor player configured: " + defaultAnchor);
        }

        // Initialize NMS stateId resolver for fast chunk serialization
        ChunkSerializer.initStateIdResolver(getLogger());

        getLogger().info("MindAxis View Plugin enabled. Use /viewer start to begin.");
    }

    @Override
    public void onDisable() {
        if (running) {
            stopViewer();
        }
    }

    /**
     * Start the viewer: WS server + event listeners + position broadcast task.
     */
    public void startViewer() {
        if (running) return;
        running = true;
        sentChunks.clear();

        // Start WebSocket server
        wsServer = new WSServer(wsPort, maxHistory, getLogger(), wsAuthToken);
        wsServer.setOnClientConnect((conn) -> {
            Bukkit.getScheduler().runTask(this, () -> {
                if (!running || anchorPlayer == null || !anchorPlayer.isOnline()) return;
                getLogger().info("[MindAxisView] New client — sending direct initial sync from live ChunkSnapshot");
                sendInitialSyncToClient(conn, anchorPlayer);
                lastInventoryHash = "";
                lastEquipmentHash = "";
                lastHealth = -1;
                sendSkin(anchorPlayer);
                sendInventory(anchorPlayer);
                sendEquipment(anchorPlayer);
                broadcastStatusForce(anchorPlayer.getHealth(), anchorPlayer.getFoodLevel(),
                        anchorPlayer.getExp(), anchorPlayer.getLevel());
            });
        });
        wsServer.start();

        // Register event listeners
        Bukkit.getPluginManager().registerEvents(this, this);

        // Auto-select anchor player if not set
        if (anchorPlayer == null || !anchorPlayer.isOnline()) {
            String configAnchor = getConfig().getString("anchor-player", "");
            if (!configAnchor.isEmpty()) {
                anchorPlayer = Bukkit.getPlayer(configAnchor);
            }
            if (anchorPlayer == null && !Bukkit.getOnlinePlayers().isEmpty()) {
                anchorPlayer = Bukkit.getOnlinePlayers().iterator().next();
                getLogger().info("Auto-selected anchor player: " + anchorPlayer.getName());
            }
        }

        // Send initial chunks and player state around anchor
        if (anchorPlayer != null) {
            sendChunksAroundPlayer(anchorPlayer);
            sendSkin(anchorPlayer);
            sendInventory(anchorPlayer);
            sendEquipment(anchorPlayer);
            // Install NMS packet interceptors on anchor's Netty channel
            installSoundInterceptor(anchorPlayer);
            installOverlayInterceptor(anchorPlayer);
            installBlockEventInterceptor(anchorPlayer);
            installParticleInterceptor(anchorPlayer);
            installContainerInterceptor(anchorPlayer);
        }

        // Start position broadcast task (every N ticks, 1 tick = 50ms)
        int ticks = Math.max(1, positionIntervalMs / 50);
        positionTask = Bukkit.getScheduler().runTaskTimer(this, this::broadcastPosition, ticks, ticks);

        // Start status broadcast task (every 20 ticks = 1 sec)
        statusTask = Bukkit.getScheduler().runTaskTimer(this, this::broadcastStatus, 20L, 20L);

        // Start entity broadcast task (every 4 ticks = 200ms)
        entityTask = Bukkit.getScheduler().runTaskTimer(this, this::broadcastEntities, 4L, 4L);

        // Start action request polling task (every tick = 50ms, on main thread)
        actionTask = Bukkit.getScheduler().runTaskTimer(this, this::processActionRequest, 1L, 1L);

        getLogger().info("Viewer started on WS port " + wsPort);
    }

    /**
     * Stop the viewer: cleanup everything.
     */
    public void stopViewer() {
        if (!running) return;
        running = false;

        // Cancel tasks
        if (positionTask != null) {
            positionTask.cancel();
            positionTask = null;
        }
        if (statusTask != null) {
            statusTask.cancel();
            statusTask = null;
        }
        if (entityTask != null) {
            entityTask.cancel();
            entityTask = null;
        }
        if (actionTask != null) {
            actionTask.cancel();
            actionTask = null;
        }

        // Remove packet interceptors from anchor player's Netty pipeline
        if (anchorPlayer != null && anchorPlayer.isOnline()) {
            removeSoundInterceptor(anchorPlayer);
            removeOverlayInterceptor(anchorPlayer);
            removeBlockEventInterceptor(anchorPlayer);
            removeParticleInterceptor(anchorPlayer);
            removeContainerInterceptor(anchorPlayer);
        }

        // Clear entity tracking
        lastEntityState.clear();
        previousEntityIds.clear();
        lastEntitySignature.clear();
        synchronized (overlayStateLock) {
            scoreboardObjectives.clear();
            scoreboardScores.clear();
            scoreboardDisplays.clear();
            bossBars.clear();
        }

        // Unregister event listeners
        HandlerList.unregisterAll((Listener) this);

        // Stop WS server
        if (wsServer != null) {
            try {
                wsServer.stop(1000);
            } catch (InterruptedException e) {
                getLogger().warning("WS server stop interrupted");
            }
            wsServer = null;
        }

        sentChunks.clear();
        getLogger().info("Viewer stopped.");
    }

    // ---- Event Handlers ----

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onChunkLoad(ChunkLoadEvent event) {
        if (!running || wsServer == null) return;
        Chunk chunk = event.getChunk();
        if (!isTrackedChunk(chunk)) return;
        snapshotAndSend(chunk);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockPlace(BlockPlaceEvent event) {
        if (!running || wsServer == null) return;
        sendBlockUpdate(event.getBlock());
        scheduleBlockEntityChunkUpdate(
                event.getBlockPlaced(),
                shouldIncludeNeighborBlockEntityChunks(event.getBlockPlaced().getType())
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockBreak(BlockBreakEvent event) {
        if (!running || wsServer == null) return;
        Material brokenType = event.getBlock().getType();
        // Send air for the broken block (on next tick, after break completes)
        Bukkit.getScheduler().runTaskLater(this, () -> {
            sendBlockUpdate(event.getBlock());
            sendBlockEntityChunkUpdate(event.getBlock(), shouldIncludeNeighborBlockEntityChunks(brokenType));
        }, 1L);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onSignChange(SignChangeEvent event) {
        if (!running || wsServer == null) return;
        scheduleBlockEntityChunkUpdate(event.getBlock(), false);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockFade(BlockFadeEvent event) {
        if (!running || wsServer == null) return;
        scheduleBlockUpdate(event.getBlock());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockForm(BlockFormEvent event) {
        if (!running || wsServer == null) return;
        scheduleBlockUpdate(event.getBlock());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockGrow(BlockGrowEvent event) {
        if (!running || wsServer == null) return;
        scheduleBlockUpdate(event.getBlock());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockSpread(BlockSpreadEvent event) {
        if (!running || wsServer == null) return;
        scheduleBlockUpdate(event.getBlock());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onLeavesDecay(LeavesDecayEvent event) {
        if (!running || wsServer == null) return;
        scheduleBlockUpdate(event.getBlock());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onFluidLevelChange(FluidLevelChangeEvent event) {
        if (!running || wsServer == null) return;
        scheduleBlockUpdate(event.getBlock());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityChangeBlock(EntityChangeBlockEvent event) {
        if (!running || wsServer == null) return;
        scheduleBlockUpdate(event.getBlock());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityBlockForm(EntityBlockFormEvent event) {
        if (!running || wsServer == null) return;
        scheduleBlockUpdate(event.getBlock());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onWeatherChange(WeatherChangeEvent event) {
        if (!running || wsServer == null || anchorPlayer == null || !anchorPlayer.isOnline()) return;
        if (!event.getWorld().equals(anchorPlayer.getWorld())) return;
        lastIsRaining = event.toWeatherState();
        lastIsThundering = event.getWorld().isThundering();
        broadcastWeatherNow(event.getWorld());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onThunderChange(ThunderChangeEvent event) {
        if (!running || wsServer == null || anchorPlayer == null || !anchorPlayer.isOnline()) return;
        if (!event.getWorld().equals(anchorPlayer.getWorld())) return;
        lastIsRaining = event.getWorld().hasStorm();
        lastIsThundering = event.toThunderState();
        broadcastWeatherNow(event.getWorld());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onTimeSkip(TimeSkipEvent event) {
        if (!running || wsServer == null || anchorPlayer == null || !anchorPlayer.isOnline()) return;
        if (!event.getWorld().equals(anchorPlayer.getWorld())) return;
        broadcastTimeNow(event.getWorld());
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerJoin(PlayerJoinEvent event) {
        if (!running) return;
        // Auto-select anchor if none set
        if (anchorPlayer == null) {
            anchorPlayer = event.getPlayer();
            getLogger().info("Auto-selected anchor player: " + anchorPlayer.getName());
            sendChunksAroundPlayer(anchorPlayer);
            installSoundInterceptor(anchorPlayer);
            installOverlayInterceptor(anchorPlayer);
            installBlockEventInterceptor(anchorPlayer);
            installParticleInterceptor(anchorPlayer);
            installContainerInterceptor(anchorPlayer);
        }
        // If the joining player is (or became) the anchor, send full state
        if (anchorPlayer != null && anchorPlayer.equals(event.getPlayer())) {
            // Reset hashes to force re-send
            lastInventoryHash = "";
            lastEquipmentHash = "";
            lastHealth = -1;
            // Delay to allow profile data and inventory to load
            Bukkit.getScheduler().runTaskLater(this, () -> {
                if (anchorPlayer == null || !anchorPlayer.isOnline()) return;
                sendSkin(anchorPlayer);
                sendInventory(anchorPlayer);
                sendEquipment(anchorPlayer);
            }, 20L);
            // Second attempt after 2 seconds for late-loading profile data
            Bukkit.getScheduler().runTaskLater(this, () -> {
                if (anchorPlayer == null || !anchorPlayer.isOnline()) return;
                lastInventoryHash = "";
                lastEquipmentHash = "";
                sendInventory(anchorPlayer);
                sendEquipment(anchorPlayer);
            }, 40L);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPlayerItemHeld(PlayerItemHeldEvent event) {
        if (!running || wsServer == null) return;
        if (!event.getPlayer().equals(anchorPlayer)) return;
        // Send inventory with updated selected slot
        Bukkit.getScheduler().runTaskLater(this, () -> sendInventory(anchorPlayer), 1L);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onInventoryClick(InventoryClickEvent event) {
        if (!running || wsServer == null) return;
        if (!(event.getWhoClicked() instanceof Player player)) return;
        if (!player.equals(anchorPlayer)) return;
        // Delay 1 tick for inventory state to update
        Bukkit.getScheduler().runTaskLater(this, () -> {
            sendInventory(player);
            sendEquipment(player);
        }, 1L);
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerDeath(PlayerDeathEvent event) {
        if (!running || wsServer == null) return;
        if (!event.getEntity().equals(anchorPlayer)) return;
        // Send zero health
        broadcastStatusForce(0, 0, 0, 0);
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerRespawn(PlayerRespawnEvent event) {
        if (!running || wsServer == null) return;
        if (!event.getPlayer().equals(anchorPlayer)) return;
        // Delay to allow respawn to complete
        Bukkit.getScheduler().runTaskLater(this, () -> {
            sendInventory(anchorPlayer);
            sendEquipment(anchorPlayer);
        }, 5L);
    }

    // ---- Entity Animation Event Handlers ----

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityDamage(EntityDamageEvent event) {
        if (!running || wsServer == null) return;
        Location entityLoc = event.getEntity().getLocation();
        if (!isTrackedLocation(entityLoc, ENTITY_RANGE)) return;
        sendEntityAnimation(event.getEntity().getEntityId(), "hurt");
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onEntityDeath(EntityDeathEvent event) {
        if (!running || wsServer == null) return;
        Location entityLoc = event.getEntity().getLocation();
        if (!isTrackedLocation(entityLoc, ENTITY_RANGE)) return;
        sendEntityAnimation(event.getEntity().getEntityId(), "death");
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerAnimation(PlayerAnimationEvent event) {
        if (!running || wsServer == null) return;
        Location playerLoc = event.getPlayer().getLocation();
        if (!isTrackedLocation(playerLoc, ENTITY_RANGE)) return;
        sendEntityAnimation(event.getPlayer().getEntityId(), "swingArm");
    }

    /**
     * Send an entity animation event via WS broadcastLive and buffer for /dev/shm.
     * @param entityId Bukkit entity ID
     * @param animation one of "hurt", "death", "swingArm", "eat"
     */
    private void sendEntityAnimation(int entityId, String animation) {
        // Broadcast via WS immediately
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "entityAnimation");
        msg.addProperty("id", entityId);
        msg.addProperty("animation", animation);
        String json = msg.toString();
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(json));

        // Buffer for /dev/shm write (flushed in broadcastEntities every 4 ticks)
        int animType;
        switch (animation) {
            case "hurt" -> animType = 0;
            case "death" -> animType = 1;
            case "swingArm" -> animType = 2;
            case "eat" -> animType = 3;
            default -> animType = -1;
        }
        if (animType >= 0) {
            pendingAnimationEvents.add(new int[]{entityId, animType});
        }
    }

    private void scheduleBlockUpdate(org.bukkit.block.Block block) {
        Bukkit.getScheduler().runTaskLater(this, () -> sendBlockUpdate(block), 1L);
    }

    private void scheduleBlockEntityChunkUpdate(org.bukkit.block.Block block, boolean includeNeighborChunks) {
        Bukkit.getScheduler().runTaskLater(this, () -> sendBlockEntityChunkUpdate(block, includeNeighborChunks), 1L);
    }

    private void sendBlockEntityChunkUpdate(org.bukkit.block.Block block, boolean includeNeighborChunks) {
        if (wsServer == null || block == null) return;

        for (Chunk chunk : collectBlockEntityChunks(block, includeNeighborChunks)) {
            String json = ChunkSerializer.serializeBlockEntities(chunk, true);
            if (json == null) continue;
            Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastMessage(json));
        }
    }

    private List<Chunk> collectBlockEntityChunks(org.bukkit.block.Block block, boolean includeNeighborChunks) {
        Map<Long, Chunk> chunks = new LinkedHashMap<>();
        addBlockEntityChunk(chunks, block.getChunk());
        if (includeNeighborChunks) {
            addBlockEntityChunk(chunks, block.getRelative(BlockFace.NORTH).getChunk());
            addBlockEntityChunk(chunks, block.getRelative(BlockFace.SOUTH).getChunk());
            addBlockEntityChunk(chunks, block.getRelative(BlockFace.EAST).getChunk());
            addBlockEntityChunk(chunks, block.getRelative(BlockFace.WEST).getChunk());
        }
        return new ArrayList<>(chunks.values());
    }

    private void addBlockEntityChunk(Map<Long, Chunk> chunks, Chunk chunk) {
        chunks.put(chunkKey(chunk.getX(), chunk.getZ()), chunk);
    }

    private boolean shouldIncludeNeighborBlockEntityChunks(Material material) {
        return material == Material.CHEST
                || material == Material.TRAPPED_CHEST
                || material.name().endsWith("_BED");
    }

    // ---- Chunk Sending ----

    /**
     * Snapshot a chunk on the main thread, then serialize + broadcast async.
     * Uses binary protocol (raw stateId int32 LE) when NMS stateId is available,
     * falls back to JSON palette+indices otherwise.
     */
    private void snapshotAndSend(Chunk chunk) {
        long key = chunkKey(chunk.getX(), chunk.getZ());
        if (sentChunks.contains(key)) return;
        sentChunks.add(key);

        // Snapshot must be taken on the main thread
        ChunkSnapshot snapshot = chunk.getChunkSnapshot();
        String blockEntitiesJson = ChunkSerializer.serializeBlockEntities(chunk, false);

        // Serialize and broadcast async to avoid blocking the tick
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            try {
                sendSnapshotBroadcast(snapshot, blockEntitiesJson);
                if (shmEnabled) {
                    writeHeightmapToShm(snapshot);
                }
                if (shmEnabled) {
                    ChunkSerializer.serializeToShm(snapshot, shmManifestEntries);
                }
            } catch (Exception e) {
                getLogger().warning("Failed to serialize chunk " + chunk.getX() + "," + chunk.getZ() + ": " + e.getMessage());
            }
        });
    }

    /**
     * Send all loaded chunks around a player.
     */
    private void sendChunksAroundPlayer(Player player) {
        for (Chunk chunk : collectLoadedChunksAroundTrackedPlayers(player)) {
            snapshotAndSend(chunk);
        }
    }

    private void sendInitialSyncToClient(WebSocket conn, Player player) {
        if (conn == null || !conn.isOpen() || player == null || !player.isOnline()) return;

        List<ChunkSyncData> snapshots = new ArrayList<>();
        for (Chunk chunk : collectLoadedChunksAroundTrackedPlayers(player)) {
            snapshots.add(new ChunkSyncData(
                    chunk.getChunkSnapshot(),
                    ChunkSerializer.serializeBlockEntities(chunk, false)
            ));
        }

        Location loc = player.getLocation();
        World world = player.getWorld();
        String positionJson = buildPositionJson(loc);
        String timeJson = buildTimeJson(world);
        String weatherJson = buildWeatherJson(world);
        String allPositionsJson = buildAllPositionsJson();
        String statusJson = buildStatusJson(player.getHealth(), player.getFoodLevel(),
                player.getExp(), player.getLevel());
        List<String> scoreboardJson = syncScoreboardStateFromPlayer(player);
        List<String> bossBarJson = syncBossBarsFromPlayer(player);

        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            try {
                for (ChunkSyncData syncData : snapshots) {
                    if (!conn.isOpen()) return;
                    sendSnapshotToClient(conn, syncData.snapshot(), syncData.blockEntitiesJson());
                }
                wsServer.flushQueuedFrames(conn);
                if (!conn.isOpen()) return;
                conn.send(positionJson);
                conn.send(timeJson);
                conn.send(weatherJson);
                conn.send(allPositionsJson);
                conn.send(statusJson);
                for (String msg : scoreboardJson) {
                    conn.send(msg);
                }
                for (String msg : bossBarJson) {
                    conn.send(msg);
                }
                conn.send(buildReadyJson(snapshots.size()));
                getLogger().info("[MindAxisView] Initial sync complete: " + snapshots.size()
                        + " chunk columns sent to " + conn.getRemoteSocketAddress());
            } catch (Exception e) {
                getLogger().warning("[MindAxisView] Initial sync failed for "
                        + conn.getRemoteSocketAddress() + ": " + e.getMessage());
            }
        });
    }

    private boolean tracksAllPlayers() {
        if (trackedPlayerNames == null || trackedPlayerNames.isEmpty()) return true;
        for (String configured : trackedPlayerNames) {
            if ("*".equals(String.valueOf(configured).trim())) return true;
        }
        return false;
    }

    private List<Player> resolveTrackedPlayers(Player focusPlayer) {
        LinkedHashMap<UUID, Player> tracked = new LinkedHashMap<>();
        if (focusPlayer != null && focusPlayer.isOnline()) {
            tracked.put(focusPlayer.getUniqueId(), focusPlayer);
        }
        if (anchorPlayer != null && anchorPlayer.isOnline()) {
            tracked.put(anchorPlayer.getUniqueId(), anchorPlayer);
        }
        if (tracksAllPlayers()) {
            for (Player onlinePlayer : Bukkit.getOnlinePlayers()) {
                if (onlinePlayer != null && onlinePlayer.isOnline()) {
                    tracked.put(onlinePlayer.getUniqueId(), onlinePlayer);
                }
            }
        } else {
            for (String configuredName : trackedPlayerNames) {
                String name = String.valueOf(configuredName).trim();
                if (name.isEmpty() || "*".equals(name)) continue;
                Player matched = Bukkit.getPlayerExact(name);
                if (matched == null) matched = Bukkit.getPlayer(name);
                if (matched != null && matched.isOnline()) {
                    tracked.put(matched.getUniqueId(), matched);
                }
            }
        }

        World trackedWorld = null;
        if (focusPlayer != null && focusPlayer.isOnline()) {
            trackedWorld = focusPlayer.getWorld();
        } else if (anchorPlayer != null && anchorPlayer.isOnline()) {
            trackedWorld = anchorPlayer.getWorld();
        } else {
            for (Player candidate : tracked.values()) {
                if (candidate != null && candidate.isOnline()) {
                    trackedWorld = candidate.getWorld();
                    break;
                }
            }
        }

        List<Player> players = new ArrayList<>();
        for (Player candidate : tracked.values()) {
            if (candidate == null || !candidate.isOnline()) continue;
            if (trackedWorld != null && candidate.getWorld() != trackedWorld) continue;
            players.add(candidate);
        }
        return players;
    }

    private boolean isTrackedLocation(Location location, double range, List<Player> trackedPlayers) {
        if (location == null || trackedPlayers == null || trackedPlayers.isEmpty()) return false;
        double rangeSquared = range * range;
        for (Player trackedPlayer : trackedPlayers) {
            if (trackedPlayer == null || !trackedPlayer.isOnline()) continue;
            Location trackedLoc = trackedPlayer.getLocation();
            if (trackedLoc.getWorld() != location.getWorld()) continue;
            if (trackedLoc.distanceSquared(location) <= rangeSquared) return true;
        }
        return false;
    }

    private boolean isTrackedLocation(Location location, double range) {
        return isTrackedLocation(location, range, resolveTrackedPlayers(anchorPlayer));
    }

    private boolean isTrackedChunk(Chunk chunk) {
        if (chunk == null) return false;
        List<Player> trackedPlayers = resolveTrackedPlayers(anchorPlayer);
        if (trackedPlayers.isEmpty()) return false;
        for (Player trackedPlayer : trackedPlayers) {
            if (trackedPlayer == null || !trackedPlayer.isOnline()) continue;
            if (trackedPlayer.getWorld() != chunk.getWorld()) continue;
            int playerChunkX = trackedPlayer.getLocation().getBlockX() >> 4;
            int playerChunkZ = trackedPlayer.getLocation().getBlockZ() >> 4;
            if (Math.abs(chunk.getX() - playerChunkX) <= viewDistance &&
                    Math.abs(chunk.getZ() - playerChunkZ) <= viewDistance) {
                return true;
            }
        }
        return false;
    }

    private List<Chunk> collectLoadedChunksAroundTrackedPlayers(Player focusPlayer) {
        LinkedHashMap<Long, Chunk> chunks = new LinkedHashMap<>();
        for (Player trackedPlayer : resolveTrackedPlayers(focusPlayer)) {
            if (trackedPlayer == null || !trackedPlayer.isOnline()) continue;
            int cx = trackedPlayer.getLocation().getBlockX() >> 4;
            int cz = trackedPlayer.getLocation().getBlockZ() >> 4;
            World world = trackedPlayer.getWorld();
            for (int dx = -viewDistance; dx <= viewDistance; dx++) {
                for (int dz = -viewDistance; dz <= viewDistance; dz++) {
                    int chunkX = cx + dx;
                    int chunkZ = cz + dz;
                    if (!world.isChunkLoaded(chunkX, chunkZ)) continue;
                    chunks.put(chunkKey(chunkX, chunkZ), world.getChunkAt(chunkX, chunkZ));
                }
            }
        }
        return new ArrayList<>(chunks.values());
    }

    private record ChunkSyncData(ChunkSnapshot snapshot, String blockEntitiesJson) {
    }

    private void sendSnapshotBroadcast(ChunkSnapshot snapshot, String blockEntitiesJson) {
        if (ChunkSerializer.isStateIdAvailable()) {
            List<byte[]> binaryMessages = ChunkSerializer.serializeBinary(snapshot);
            for (byte[] binMsg : binaryMessages) {
                wsServer.broadcastBinary(compressBinaryChunks
                        ? ChunkSerializer.gzipBinaryFrame(binMsg)
                        : binMsg);
            }
        } else {
            List<String> messages = ChunkSerializer.serialize(snapshot);
            for (String msg : messages) {
                wsServer.broadcastMessage(msg);
            }
        }

        List<String> biomeMessages = ChunkSerializer.serializeBiomes(snapshot);
        for (String msg : biomeMessages) {
            wsServer.broadcastMessage(msg);
        }

        wsServer.broadcastMessage(ChunkSerializer.serializeHeightmap(snapshot));
        if (blockEntitiesJson != null) {
            wsServer.broadcastMessage(blockEntitiesJson);
        }
    }

    private void sendSnapshotToClient(WebSocket conn, ChunkSnapshot snapshot, String blockEntitiesJson) {
        if (ChunkSerializer.isStateIdAvailable()) {
            List<byte[]> binaryMessages = ChunkSerializer.serializeBinary(snapshot);
            for (byte[] binMsg : binaryMessages) {
                if (!conn.isOpen()) return;
                conn.send(compressBinaryChunks
                        ? ChunkSerializer.gzipBinaryFrame(binMsg)
                        : binMsg);
            }
        } else {
            List<String> messages = ChunkSerializer.serialize(snapshot);
            for (String msg : messages) {
                if (!conn.isOpen()) return;
                conn.send(msg);
            }
        }

        List<String> biomeMessages = ChunkSerializer.serializeBiomes(snapshot);
        for (String msg : biomeMessages) {
            if (!conn.isOpen()) return;
            conn.send(msg);
        }

        if (conn.isOpen()) {
            conn.send(ChunkSerializer.serializeHeightmap(snapshot));
            if (blockEntitiesJson != null) {
                conn.send(blockEntitiesJson);
            }
        }
    }

    private String buildReadyJson(int chunkCount) {
        JsonObject ready = new JsonObject();
        ready.addProperty("type", "ready");
        ready.addProperty("chunkCount", chunkCount);
        ready.addProperty("ts", System.currentTimeMillis());
        return ready.toString();
    }

    // ---- Shared-Memory Heightmap for Bot ----

    /**
     * Write chunk heightmap to /dev/shm/mindaxis-hm/{cx}_{cz} as 512 bytes
     * (256 × int16 little-endian). The bot's getSurfaceY() reads these files
     * for fast surface-Y lookups without packet parsing overhead.
     * Index order: x + z*16 (x changes fastest, outer loop z).
     */
    private void writeHeightmapToShm(ChunkSnapshot snapshot) {
        int cx = snapshot.getX();
        int cz = snapshot.getZ();
        java.nio.file.Path dir = java.nio.file.Paths.get("/dev/shm/mindaxis-hm");
        try {
            java.nio.file.Files.createDirectories(dir);
            java.nio.file.Path file = dir.resolve(cx + "_" + cz);
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(512);
            buf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
            for (int z = 0; z < 16; z++) {
                for (int x = 0; x < 16; x++) {
                    buf.putShort((short) snapshot.getHighestBlockYAt(x, z));
                }
            }
            java.nio.file.Files.write(file, buf.array());
        } catch (Exception e) {
            // silently ignore - not critical
        }
    }

    // ---- Block Updates ----

    /**
     * Send a single block update message.
     */
    private void sendBlockUpdate(org.bukkit.block.Block block) {
        if (wsServer == null) return;

        org.bukkit.block.data.BlockData bd = block.getBlockData();
        String blockData = bd.getAsString();
        int bracketStart = blockData.indexOf('[');
        String name = bracketStart == -1 ? blockData : blockData.substring(0, bracketStart);

        JsonObject msg = new JsonObject();
        msg.addProperty("type", "blockUpdate");
        msg.addProperty("x", block.getX());
        msg.addProperty("y", block.getY());
        msg.addProperty("z", block.getZ());
        msg.addProperty("name", name);

        // Include stateId if available (skips name-based translation in bridge)
        int sid = ChunkSerializer.getStateId(bd);
        if (sid >= 0) {
            msg.addProperty("sid", sid);
        }

        if (bracketStart != -1) {
            JsonObject states = new JsonObject();
            String statesStr = blockData.substring(bracketStart + 1, blockData.length() - 1);
            for (String pair : statesStr.split(",")) {
                String[] kv = pair.split("=", 2);
                if (kv.length == 2) {
                    states.addProperty(kv[0], kv[1]);
                }
            }
            msg.add("states", states);
        }

        String json = msg.toString();
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastMessage(json));
    }

    private String buildPositionJson(Location loc) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "position");
        msg.addProperty("x", (float) loc.getX());
        msg.addProperty("y", (float) loc.getY());
        msg.addProperty("z", (float) loc.getZ());
        msg.addProperty("yaw", loc.getYaw());
        msg.addProperty("pitch", loc.getPitch());
        return msg.toString();
    }

    private String buildTimeJson(World world) {
        JsonObject timeMsg = new JsonObject();
        timeMsg.addProperty("type", "time");
        timeMsg.addProperty("timeOfDay", world.getTime());
        timeMsg.addProperty("age", world.getFullTime());
        return timeMsg.toString();
    }

    private String buildWeatherJson(World world) {
        boolean isRaining = world.hasStorm();
        boolean isThundering = world.isThundering();
        JsonObject weatherMsg = new JsonObject();
        weatherMsg.addProperty("type", "weather");
        weatherMsg.addProperty("isRaining", isRaining);
        weatherMsg.addProperty("isThundering", isThundering);
        weatherMsg.addProperty("rainLevel", isRaining ? 1.0f : 0.0f);
        return weatherMsg.toString();
    }

    private String buildAllPositionsJson() {
        JsonObject allPosMsg = new JsonObject();
        allPosMsg.addProperty("type", "allPositions");
        JsonArray playersArray = new JsonArray();
        List<Player> trackedPlayers = resolveTrackedPlayers(anchorPlayer);
        for (Player trackedPlayer : trackedPlayers) {
            if (trackedPlayer == null || !trackedPlayer.isOnline()) continue;
            Location pLoc = trackedPlayer.getLocation();
            JsonObject pObj = new JsonObject();
            pObj.addProperty("id", trackedPlayer.getName());
            pObj.addProperty("name", trackedPlayer.getName());
            pObj.addProperty("world", pLoc.getWorld() != null ? pLoc.getWorld().getName() : "");
            pObj.addProperty("x", (float) pLoc.getX());
            pObj.addProperty("y", (float) pLoc.getY());
            pObj.addProperty("z", (float) pLoc.getZ());
            pObj.addProperty("yaw", pLoc.getYaw());
            pObj.addProperty("pitch", pLoc.getPitch());
            pObj.addProperty("isAnchor", anchorPlayer != null &&
                    trackedPlayer.getUniqueId().equals(anchorPlayer.getUniqueId()));
            playersArray.add(pObj);
        }
        allPosMsg.add("players", playersArray);
        return allPosMsg.toString();
    }

    private String buildStatusJson(double health, int food, float exp, int level) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "status");
        msg.addProperty("health", (float) health);
        msg.addProperty("food", food);
        msg.addProperty("experience", exp);
        msg.addProperty("level", level);
        return msg.toString();
    }

    private void broadcastTimeNow(World world) {
        String timeJson = buildTimeJson(world);
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(timeJson));
    }

    private void broadcastWeatherNow(World world) {
        String weatherJson = buildWeatherJson(world);
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(weatherJson));
    }

    // ---- Position Broadcasting ----

    /**
     * Broadcast anchor player position (called by scheduled task).
     */
    private void broadcastPosition() {
        if (!running || wsServer == null || anchorPlayer == null || !anchorPlayer.isOnline()) return;

        // Re-resolve player reference each tick to avoid stale objects after reload
        Player fresh = Bukkit.getPlayer(anchorPlayer.getUniqueId());
        if (fresh == null || !fresh.isOnline()) return;
        anchorPlayer = fresh;

        org.bukkit.Location loc = fresh.getLocation();
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "position");
        msg.addProperty("x", (float) loc.getX());
        msg.addProperty("y", (float) loc.getY());
        msg.addProperty("z", (float) loc.getZ());
        msg.addProperty("yaw", loc.getYaw());
        msg.addProperty("pitch", loc.getPitch());

        String json = msg.toString();

        // Also send time update with each position tick
        JsonObject timeMsg = new JsonObject();
        timeMsg.addProperty("type", "time");
        timeMsg.addProperty("timeOfDay", anchorPlayer.getWorld().getTime());
        timeMsg.addProperty("age", anchorPlayer.getWorld().getFullTime());
        String timeJson = timeMsg.toString();

        // Send weather update only when state changes
        boolean isRaining = anchorPlayer.getWorld().hasStorm();
        boolean isThundering = anchorPlayer.getWorld().isThundering();
        String weatherJson = null;
        if (isRaining != lastIsRaining || isThundering != lastIsThundering) {
            lastIsRaining = isRaining;
            lastIsThundering = isThundering;
            JsonObject weatherMsg = new JsonObject();
            weatherMsg.addProperty("type", "weather");
            weatherMsg.addProperty("isRaining", isRaining);
            weatherMsg.addProperty("isThundering", isThundering);
            weatherMsg.addProperty("rainLevel", isRaining ? 1.0f : 0.0f);
            weatherJson = weatherMsg.toString();
        }

        final String allPosJson = buildAllPositionsJson();

        final String weatherJsonFinal = weatherJson;
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            wsServer.broadcastLive(json);
            wsServer.broadcastLive(timeJson);
            wsServer.broadcastLive(allPosJson);
            if (weatherJsonFinal != null) {
                wsServer.broadcastLive(weatherJsonFinal);
            }
        });
    }

    // ---- Entity Broadcasting ----

    /**
     * Broadcast entity positions, spawns, and despawns (called by scheduled task every 4 ticks).
     * Sends a batched JSON array of entity updates and entityGone messages via broadcastLive
     * to avoid polluting the history buffer with ephemeral entity data.
     */
    private void broadcastEntities() {
        if (!running || wsServer == null || anchorPlayer == null || !anchorPlayer.isOnline()) return;

        List<Player> trackedPlayers = resolveTrackedPlayers(anchorPlayer);
        if (trackedPlayers.isEmpty()) return;
        World trackedWorld = trackedPlayers.get(0).getWorld();

        JsonArray batch = new JsonArray();
        // Full entity list for /dev/shm (all current entities, not just changed)
        JsonArray shmEntities = new JsonArray();
        Set<Integer> currentEntityIds = new HashSet<>();

        for (Entity entity : trackedWorld.getEntities()) {
            // Skip the anchor player itself
            if (entity.equals(anchorPlayer)) continue;

            // Skip non-interesting entity types
            if (entity instanceof org.bukkit.entity.AreaEffectCloud) continue;
            if (entity instanceof org.bukkit.entity.Marker) continue;
            if (entity instanceof org.bukkit.entity.ArmorStand) continue;

            // Only include LivingEntity, Players, and Item entities
            boolean isLiving = entity instanceof LivingEntity;
            boolean isItem = entity instanceof Item;
            if (!isLiving && !isItem) continue;

            // Range check
            Location entityLoc = entity.getLocation();
            if (!isTrackedLocation(entityLoc, ENTITY_RANGE, trackedPlayers)) continue;

            int entityId = entity.getEntityId();
            currentEntityIds.add(entityId);

            double ex = entityLoc.getX();
            double ey = entityLoc.getY();
            double ez = entityLoc.getZ();
            float eyaw = entityLoc.getYaw();
            float epitch = entityLoc.getPitch();

            // Build entity JSON for shm (always includes ALL entities)
            JsonObject shmObj = buildEntityJson(entity, entityId, ex, ey, ez, eyaw, epitch, isLiving, isItem);
            shmEntities.add(shmObj);

            // De-duplicate for WS: emit on meaningful movement OR state/equipment/health changes.
            double[] lastState = lastEntityState.get(entityId);
            String signature = buildEntitySignature(entity, typeName(entity), categoryName(entity, isItem), shmObj);
            String lastSignature = lastEntitySignature.get(entityId);
            boolean positionChanged = true;
            if (lastState != null) {
                double dx = ex - lastState[0];
                double dy = ey - lastState[1];
                double dz = ez - lastState[2];
                double dist = Math.sqrt(dx * dx + dy * dy + dz * dz);
                double yawDiff = Math.abs(eyaw - lastState[3]);
                if (yawDiff > 180) yawDiff = 360 - yawDiff;
                positionChanged = dist >= ENTITY_POS_THRESHOLD || yawDiff >= ENTITY_YAW_THRESHOLD;
            }
            boolean stateChanged = !signature.equals(lastSignature);
            if (!positionChanged && !stateChanged) continue;

            // Update last known state
            lastEntityState.put(entityId, new double[]{ex, ey, ez, eyaw, epitch});
            lastEntitySignature.put(entityId, signature);

            // Re-use the same JSON object for the WS batch
            batch.add(shmObj);
        }

        // Detect despawned entities
        JsonArray shmGone = new JsonArray();
        for (int prevId : previousEntityIds) {
            if (!currentEntityIds.contains(prevId)) {
                JsonObject gone = new JsonObject();
                gone.addProperty("type", "entityGone");
                gone.addProperty("id", prevId);
                batch.add(gone);
                shmGone.add(prevId);
                lastEntityState.remove(prevId);
                lastEntitySignature.remove(prevId);
            }
        }
        previousEntityIds.clear();
        previousEntityIds.addAll(currentEntityIds);

        // Write full entity snapshot to /dev/shm for the mindcraft bot
        writeShmEntities(shmEntities, shmGone);

        // Flush buffered entity animation events to /dev/shm
        flushEntityAnimationEvents();

        // Send batched update if non-empty
        if (batch.size() > 0) {
            JsonObject batchMsg = new JsonObject();
            batchMsg.addProperty("type", "entityBatch");
            batchMsg.add("entities", batch);
            String json = batchMsg.toString();
            Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(json));
        }
    }

    /**
     * Build entity JSON object shared by WS broadcast and /dev/shm file.
     */
    private JsonObject buildEntityJson(Entity entity, int entityId,
            double ex, double ey, double ez, float eyaw, float epitch,
            boolean isLiving, boolean isItem) {
        JsonObject obj = new JsonObject();
        obj.addProperty("type", "entity");
        obj.addProperty("id", entityId);

        // Entity type name (lowercase)
        String typeName = typeName(entity);
        obj.addProperty("name", typeName);

        // Determine entity category
        String category = categoryName(entity, isItem);
        obj.addProperty("entityType", category);

        // For players, include username
        if (entity instanceof Player otherPlayer) {
            obj.addProperty("username", otherPlayer.getName());
        }

        // For item entities, include the item name
        if (isItem) {
            Item itemEntity = (Item) entity;
            ItemStack stack = itemEntity.getItemStack();
            if (stack != null && stack.getType() != Material.AIR) {
                obj.addProperty("itemName", stack.getType().getKey().toString());
            }
        }

        // Position
        obj.addProperty("x", (float) ex);
        obj.addProperty("y", (float) ey);
        obj.addProperty("z", (float) ez);

        // Keep nested pos for WS backward compatibility
        JsonObject pos = new JsonObject();
        pos.addProperty("x", (float) ex);
        pos.addProperty("y", (float) ey);
        pos.addProperty("z", (float) ez);
        obj.add("pos", pos);

        obj.addProperty("yaw", eyaw);
        obj.addProperty("pitch", epitch);

        // Velocity
        org.bukkit.util.Vector vel = entity.getVelocity();
        obj.addProperty("vx", (float) vel.getX());
        obj.addProperty("vy", (float) vel.getY());
        obj.addProperty("vz", (float) vel.getZ());

        // Keep nested velocity for WS backward compatibility
        JsonObject velocity = new JsonObject();
        velocity.addProperty("x", (float) vel.getX());
        velocity.addProperty("y", (float) vel.getY());
        velocity.addProperty("z", (float) vel.getZ());
        obj.add("velocity", velocity);

        // Metadata: entity flags byte (index 0)
        JsonArray metadata = new JsonArray();
        byte flags = 0;
        if (entity.getFireTicks() > 0) flags |= 0x01;
        if (entity instanceof Player p && p.isSneaking()) flags |= 0x02;
        if (entity instanceof Player p2 && p2.isSprinting()) flags |= 0x08;
        if (entity instanceof LivingEntity le && le.isSwimming()) flags |= 0x10;
        if (entity.isInvisible()) flags |= 0x20;
        if (entity.isGlowing()) flags |= 0x40;
        metadata.add(flags);
        obj.add("metadata", metadata);

        // Equipment (for LivingEntity)
        if (isLiving) {
            org.bukkit.inventory.EntityEquipment eq = ((LivingEntity) entity).getEquipment();
            JsonArray equipment = new JsonArray();
            if (eq != null) {
                equipment.add(equipItemName(eq.getItemInMainHand()));
                equipment.add(equipItemName(eq.getItemInOffHand()));
                equipment.add(equipItemName(eq.getHelmet()));
                equipment.add(equipItemName(eq.getChestplate()));
                equipment.add(equipItemName(eq.getLeggings()));
                equipment.add(equipItemName(eq.getBoots()));
            } else {
                for (int i = 0; i < 6; i++) equipment.add(JsonNull.INSTANCE);
            }
            obj.add("equipment", equipment);
        }

        // Health and using-item state (for LivingEntity, via Bukkit API)
        if (isLiving) {
            LivingEntity living = (LivingEntity) entity;
            obj.addProperty("health", (float) living.getHealth());
            obj.addProperty("isUsingItem", living.isHandRaised());
        }

        return obj;
    }

    private String typeName(Entity entity) {
        return entity.getType().name().toLowerCase();
    }

    private String categoryName(Entity entity, boolean isItem) {
        if (entity instanceof Player) {
            return "player";
        }
        if (isItem) {
            return "object";
        }
        return "mob";
    }

    private String buildEntitySignature(Entity entity, String typeName, String category, JsonObject obj) {
        StringBuilder signature = new StringBuilder();
        signature.append(typeName).append('|').append(category);

        if (obj.has("username")) {
            signature.append('|').append(obj.get("username").getAsString());
        }
        if (obj.has("itemName")) {
            signature.append('|').append(obj.get("itemName").getAsString());
        }
        if (obj.has("metadata")) {
            signature.append('|').append(obj.get("metadata"));
        }
        if (obj.has("equipment")) {
            signature.append('|').append(obj.get("equipment"));
        }
        if (obj.has("health")) {
            signature.append('|').append(obj.get("health").getAsFloat());
        }
        if (obj.has("isUsingItem")) {
            signature.append('|').append(obj.get("isUsingItem").getAsBoolean());
        }

        org.bukkit.util.Vector vel = entity.getVelocity();
        signature.append('|').append(Math.round(vel.getX() * 100.0) / 100.0);
        signature.append('|').append(Math.round(vel.getY() * 100.0) / 100.0);
        signature.append('|').append(Math.round(vel.getZ() * 100.0) / 100.0);
        return signature.toString();
    }

    /**
     * Write entity snapshot to /dev/shm/mindaxis-entities.json for the mindcraft bot.
     * Atomic write: write to .tmp then rename.
     */
    private void writeShmEntities(JsonArray entities, JsonArray gone) {
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            try {
                JsonObject root = new JsonObject();
                root.addProperty("ts", System.currentTimeMillis());
                root.add("entities", entities);
                root.add("gone", gone);

                java.nio.file.Path tmpPath = java.nio.file.Paths.get("/dev/shm/mindaxis-entities.json.tmp");
                java.nio.file.Path finalPath = java.nio.file.Paths.get("/dev/shm/mindaxis-entities.json");
                java.nio.file.Files.writeString(tmpPath, root.toString());
                java.nio.file.Files.move(tmpPath, finalPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception e) {
                // silently ignore — not critical, bot will retry next poll
            }
        });
    }

    /**
     * Flush buffered entity animation events to /dev/shm/mindaxis-entity-events.json.
     * Called from broadcastEntities (every 4 ticks). If no events are pending, does nothing.
     */
    private void flushEntityAnimationEvents() {
        if (pendingAnimationEvents.isEmpty()) return;

        JsonArray eventsArray = new JsonArray();
        int[] event;
        while ((event = pendingAnimationEvents.poll()) != null) {
            JsonObject obj = new JsonObject();
            obj.addProperty("id", event[0]);
            String animName = switch (event[1]) {
                case 0 -> "hurt";
                case 1 -> "death";
                case 2 -> "swingArm";
                case 3 -> "eat";
                default -> "unknown";
            };
            obj.addProperty("animation", animName);
            eventsArray.add(obj);
        }

        if (eventsArray.size() == 0) return;

        JsonObject root = new JsonObject();
        root.addProperty("ts", System.currentTimeMillis());
        root.add("events", eventsArray);

        String json = root.toString();
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            try {
                Files.writeString(ENTITY_EVENTS_TMP_PATH, json);
                Files.move(ENTITY_EVENTS_TMP_PATH, ENTITY_EVENTS_PATH,
                        StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception e) {
                // silently ignore — not critical
            }
        });
    }

    // ---- Scoreboard / Bossbar Interceptor (reflection-based) ----

    private static final PlainTextComponentSerializer PLAIN_TEXT = PlainTextComponentSerializer.plainText();
    private static volatile boolean overlayReflectionInitialized = false;
    private static volatile boolean overlayReflectionAvailable = false;
    private static Class<?> scoreboardObjectivePacketClass;
    private static Class<?> scoreboardScorePacketClass;
    private static Class<?> scoreboardResetScorePacketClass;
    private static Class<?> scoreboardDisplayPacketClass;
    private static Class<?> bossEventPacketClass;
    private static Method objectiveGetName;
    private static Method objectiveGetDisplayName;
    private static Method objectiveGetMode;
    private static Method objectiveGetRenderType;
    private static Method scoreGetEntry;
    private static Method scoreGetObjective;
    private static Method scoreGetValue;
    private static Method scoreGetDisplay;
    private static Method resetScoreGetEntry;
    private static Method resetScoreGetObjective;
    private static Method displayGetSlot;
    private static Method displayGetObjective;
    private static Method bossEventDispatch;
    private static Class<?> bossEventConsumerClass;
    private static Class<?> bossBarImplementationImplClass;
    private static Field bossBarImplementationVanillaField;

    // ---- NMS Container Packet Interceptor (reflection-based) ----

    private static volatile boolean containerReflectionInitialized = false;
    private static volatile boolean containerReflectionAvailable = false;
    private static Class<?> openScreenPacketClass;
    private static Class<?> containerClosePacketClass;
    private static Class<?> containerContentPacketClass;
    private static Class<?> containerSlotPacketClass;
    private static Method openScreenGetContainerId;
    private static Method openScreenGetType;
    private static Method openScreenGetTitle;
    private static Method containerCloseGetContainerId;
    private static Method containerContentGetContainerId;
    private static Method containerContentGetItems;
    private static Method containerSlotGetContainerId;
    private static Method containerSlotGetSlot;
    private static Method containerSlotGetItem;
    private static Object menuRegistry;
    private static Object itemRegistry;
    private static Method registryGetKey;
    private static Method holderUnwrapKey;
    private static Method resourceKeyIdentifier;
    private static Method componentGetString;
    private static Method itemStackIsEmpty;
    private static Method itemStackGetItem;
    private static Method itemStackGetCount;
    private static Method itemStackIsDamageableItem;
    private static Method itemStackGetDamageValue;
    private static Method itemStackGetDisplayName;
    private static Method itemStackGetEnchantments;
    private static Method itemStackGetComponent;
    private static Method itemEnchantmentsEntrySet;
    private static Object dataComponentsCustomData;
    private static Method customDataIsEmpty;
    private static Method customDataCopyTag;
    private static Method bossBattleGetId;

    private void initOverlayReflection() {
        if (overlayReflectionInitialized) return;
        overlayReflectionInitialized = true;
        try {
            scoreboardObjectivePacketClass = loadClass(
                    "net.minecraft.network.protocol.game.ClientboundSetObjectivePacket",
                    "net.minecraft.network.protocol.game.PacketPlayOutScoreboardObjective",
                    "net.minecraft.network.packet.s2c.play.ScoreboardObjectiveUpdateS2CPacket"
            );
            objectiveGetName = requireMethod(scoreboardObjectivePacketClass, 0, "getObjectiveName", "getName", "b");
            objectiveGetDisplayName = requireMethod(scoreboardObjectivePacketClass, 0, "getDisplayName", "e");
            objectiveGetMode = requireMethod(scoreboardObjectivePacketClass, 0, "getMethod", "getMode", "f");
            objectiveGetRenderType = requireMethod(scoreboardObjectivePacketClass, 0, "getRenderType", "getType", "g");

            scoreboardScorePacketClass = loadClass(
                    "net.minecraft.network.protocol.game.ClientboundSetScorePacket",
                    "net.minecraft.network.protocol.game.PacketPlayOutScoreboardScore",
                    "net.minecraft.network.packet.s2c.play.ScoreboardScoreUpdateS2CPacket"
            );
            scoreGetEntry = requireMethod(scoreboardScorePacketClass, 0,
                    "owner", "scoreHolderName", "getOwner", "getScoreHolderName", "b");
            scoreGetObjective = requireMethod(scoreboardScorePacketClass, 0,
                    "objectiveName", "getObjectiveName", "e");
            scoreGetValue = requireMethod(scoreboardScorePacketClass, 0,
                    "score", "getScore", "f");
            scoreGetDisplay = findMethod(scoreboardScorePacketClass, 0,
                    "display", "getDisplay", "g");

            scoreboardResetScorePacketClass = tryLoadClass(
                    "net.minecraft.network.protocol.game.ClientboundResetScorePacket"
            );
            if (scoreboardResetScorePacketClass != null) {
                resetScoreGetEntry = requireMethod(scoreboardResetScorePacketClass, 0,
                        "owner", "scoreHolderName", "getOwner", "getScoreHolderName", "b");
                resetScoreGetObjective = findMethod(scoreboardResetScorePacketClass, 0,
                        "objectiveName", "getObjectiveName", "e");
            }

            scoreboardDisplayPacketClass = loadClass(
                    "net.minecraft.network.protocol.game.ClientboundSetDisplayObjectivePacket",
                    "net.minecraft.network.protocol.game.PacketPlayOutScoreboardDisplayObjective",
                    "net.minecraft.network.packet.s2c.play.ScoreboardDisplayS2CPacket"
            );
            displayGetSlot = requireMethod(scoreboardDisplayPacketClass, 0, "getSlot", "b");
            displayGetObjective = requireMethod(scoreboardDisplayPacketClass, 0, "getObjectiveName", "getName", "e");

            bossEventPacketClass = loadClass(
                    "net.minecraft.network.protocol.game.ClientboundBossEventPacket",
                    "net.minecraft.network.protocol.game.PacketPlayOutBoss",
                    "net.minecraft.network.packet.s2c.play.BossBarS2CPacket"
            );
            bossEventDispatch = requireBossEventDispatchMethod(bossEventPacketClass);
            bossEventConsumerClass = bossEventDispatch.getParameterTypes()[0];

            bossBarImplementationImplClass = tryLoadClass("io.papermc.paper.adventure.BossBarImplementationImpl");
            if (bossBarImplementationImplClass != null) {
                bossBarImplementationVanillaField = findFieldAny(bossBarImplementationImplClass, "vanilla");
                if (bossBarImplementationVanillaField != null) {
                    bossBarImplementationVanillaField.setAccessible(true);
                }
            }
            Class<?> bossBattleClass = tryLoadClass("net.minecraft.world.BossBattle");
            if (bossBattleClass != null) {
                bossBattleGetId = requireMethod(bossBattleClass, 0, "getId", "i");
            }

            overlayReflectionAvailable = true;
            getLogger().info("[MindAxisView] Overlay packet reflection initialized successfully");
        } catch (Exception e) {
            overlayReflectionAvailable = false;
            getLogger().warning("[MindAxisView] Overlay packet reflection not available: " + e.getMessage());
            getLogger().warning("[MindAxisView] Scoreboard/Bossbar interception will be disabled");
        }
    }

    private void initContainerReflection() {
        if (containerReflectionInitialized) return;
        containerReflectionInitialized = true;
        try {
            openScreenPacketClass = loadClass(
                    "net.minecraft.network.protocol.game.ClientboundOpenScreenPacket",
                    "net.minecraft.network.packet.s2c.play.OpenScreenS2CPacket"
            );
            containerClosePacketClass = loadClass(
                    "net.minecraft.network.protocol.game.ClientboundContainerClosePacket",
                    "net.minecraft.network.packet.s2c.play.CloseScreenS2CPacket"
            );
            containerContentPacketClass = loadClass(
                    "net.minecraft.network.protocol.game.ClientboundContainerSetContentPacket",
                    "net.minecraft.network.packet.s2c.play.InventoryS2CPacket"
            );
            containerSlotPacketClass = loadClass(
                    "net.minecraft.network.protocol.game.ClientboundContainerSetSlotPacket",
                    "net.minecraft.network.packet.s2c.play.ScreenHandlerSlotUpdateS2CPacket"
            );

            openScreenGetContainerId = requireMethod(openScreenPacketClass, 0, "getContainerId", "getSyncId");
            openScreenGetType = requireMethod(openScreenPacketClass, 0, "getType", "getScreenHandlerType");
            openScreenGetTitle = requireMethod(openScreenPacketClass, 0, "getTitle", "getName");
            containerCloseGetContainerId = requireMethod(containerClosePacketClass, 0, "getContainerId", "getSyncId");
            containerContentGetContainerId = requireMethod(containerContentPacketClass, 0, "containerId", "getContainerId", "getSyncId");
            containerContentGetItems = requireMethod(containerContentPacketClass, 0, "items", "getItems", "getContents");
            containerSlotGetContainerId = requireMethod(containerSlotPacketClass, 0, "getContainerId", "containerId", "getSyncId");
            containerSlotGetSlot = requireMethod(containerSlotPacketClass, 0, "getSlot", "slot");
            containerSlotGetItem = requireMethod(containerSlotPacketClass, 0, "getItem", "item", "getStack");

            Class<?> builtInRegistriesClass = loadClass("net.minecraft.core.registries.BuiltInRegistries");
            Field menuRegistryField = findFieldAny(builtInRegistriesClass, "MENU", "SCREEN_HANDLER");
            if (menuRegistryField == null) {
                throw new NoSuchFieldException("BuiltInRegistries.MENU");
            }
            menuRegistryField.setAccessible(true);
            menuRegistry = menuRegistryField.get(null);

            Field itemRegistryField = findFieldAny(builtInRegistriesClass, "ITEM");
            if (itemRegistryField == null) {
                throw new NoSuchFieldException("BuiltInRegistries.ITEM");
            }
            itemRegistryField.setAccessible(true);
            itemRegistry = itemRegistryField.get(null);
            registryGetKey = requireMethod(menuRegistry.getClass(), 1, "getKey");

            Class<?> holderClass = loadClass("net.minecraft.core.Holder");
            if (registryEntryValue == null) {
                registryEntryValue = requireMethod(holderClass, 0, "value");
            }
            holderUnwrapKey = requireMethod(holderClass, 0, "unwrapKey");

            Class<?> resourceKeyClass = loadClass("net.minecraft.resources.ResourceKey");
            resourceKeyIdentifier = requireMethod(resourceKeyClass, 0, "identifier", "location");

            Class<?> componentClass = loadClass("net.minecraft.network.chat.Component", "net.minecraft.text.Text");
            componentGetString = requireMethod(componentClass, 0, "getString");

            Class<?> itemStackClass = loadClass("net.minecraft.world.item.ItemStack", "net.minecraft.item.ItemStack");
            itemStackIsEmpty = requireMethod(itemStackClass, 0, "isEmpty");
            itemStackGetItem = requireMethod(itemStackClass, 0, "getItem");
            itemStackGetCount = requireMethod(itemStackClass, 0, "getCount", "getAmount");
            itemStackIsDamageableItem = requireMethod(itemStackClass, 0, "isDamageableItem", "isDamageable");
            itemStackGetDamageValue = requireMethod(itemStackClass, 0, "getDamageValue", "getDamage");
            itemStackGetDisplayName = requireMethod(itemStackClass, 0, "getDisplayName", "getHoverName", "getName");
            itemStackGetEnchantments = requireMethod(itemStackClass, 0, "getEnchantments");

            Class<?> itemEnchantmentsClass = loadClass(
                    "net.minecraft.world.item.enchantment.ItemEnchantments",
                    "net.minecraft.component.type.ItemEnchantmentsComponent"
            );
            itemEnchantmentsEntrySet = requireMethod(itemEnchantmentsClass, 0, "entrySet");

            Class<?> dataComponentsClass = tryLoadClass("net.minecraft.core.component.DataComponents");
            if (dataComponentsClass != null) {
                Field customDataField = findFieldAny(dataComponentsClass, "CUSTOM_DATA");
                if (customDataField != null) {
                    customDataField.setAccessible(true);
                    dataComponentsCustomData = customDataField.get(null);
                    itemStackGetComponent = requireMethod(itemStackClass, 1, "get");
                    Class<?> customDataClass = loadClass("net.minecraft.world.item.component.CustomData");
                    customDataIsEmpty = requireMethod(customDataClass, 0, "isEmpty");
                    customDataCopyTag = requireMethod(customDataClass, 0, "copyTag");
                }
            }

            containerReflectionAvailable = true;
            getLogger().info("[MindAxisView] Container packet reflection initialized successfully");
        } catch (Exception e) {
            containerReflectionAvailable = false;
            getLogger().warning("[MindAxisView] Container packet reflection not available: " + e.getMessage());
            getLogger().warning("[MindAxisView] Container interception will be disabled");
        }
    }

    private void installOverlayInterceptor(Player player) {
        initOverlayReflection();
        if (!overlayReflectionAvailable) return;

        try {
            Object craftPlayer = player;
            Method getHandle = craftPlayer.getClass().getMethod("getHandle");
            Object serverPlayer = getHandle.invoke(craftPlayer);

            Field connectionField = serverPlayer.getClass().getField("connection");
            Object gamePacketListener = connectionField.get(serverPlayer);

            Field networkField = findField(gamePacketListener.getClass(), "connection");
            if (networkField == null) {
                networkField = findField(gamePacketListener.getClass().getSuperclass(), "connection");
            }
            if (networkField == null) {
                getLogger().warning("[MindAxisView] Cannot find connection field on " + gamePacketListener.getClass().getName());
                return;
            }
            networkField.setAccessible(true);
            Object networkConnection = networkField.get(gamePacketListener);

            Field channelField = findField(networkConnection.getClass(), "channel");
            if (channelField == null) {
                getLogger().warning("[MindAxisView] Cannot find channel field on " + networkConnection.getClass().getName());
                return;
            }
            channelField.setAccessible(true);
            Object channelObj = channelField.get(networkConnection);

            removeOverlayInterceptorFromChannel(channelObj);

            OverlayChannelHandler handler = new OverlayChannelHandler(
                    this,
                    scoreboardObjectivePacketClass,
                    scoreboardScorePacketClass,
                    scoreboardResetScorePacketClass,
                    scoreboardDisplayPacketClass,
                    bossEventPacketClass
            );

            Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
            Class<?> channelHandlerClass = Class.forName("io.netty.channel.ChannelHandler");
            pipeline.getClass()
                    .getMethod("addBefore", String.class, String.class, channelHandlerClass)
                    .invoke(pipeline, "packet_handler", OVERLAY_HANDLER_NAME, handler);

            getLogger().info("[MindAxisView] Overlay interceptor installed for " + player.getName());
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Failed to install overlay interceptor: " + e.getMessage());
        }
    }

    private void removeOverlayInterceptor(Player player) {
        if (!overlayReflectionAvailable) return;
        try {
            Object craftPlayer = player;
            Method getHandle = craftPlayer.getClass().getMethod("getHandle");
            Object serverPlayer = getHandle.invoke(craftPlayer);
            Field connectionField = serverPlayer.getClass().getField("connection");
            Object gamePacketListener = connectionField.get(serverPlayer);
            Field networkField = findField(gamePacketListener.getClass(), "connection");
            if (networkField == null) networkField = findField(gamePacketListener.getClass().getSuperclass(), "connection");
            if (networkField == null) return;
            networkField.setAccessible(true);
            Object networkConnection = networkField.get(gamePacketListener);
            Field channelField = findField(networkConnection.getClass(), "channel");
            if (channelField == null) return;
            channelField.setAccessible(true);
            Object channelObj = channelField.get(networkConnection);
            removeOverlayInterceptorFromChannel(channelObj);
        } catch (Exception e) {
            // Silently ignore — player may have disconnected
        }
    }

    private void removeOverlayInterceptorFromChannel(Object channelObj) {
        try {
            Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
            Object existing = pipeline.getClass().getMethod("get", String.class).invoke(pipeline, OVERLAY_HANDLER_NAME);
            if (existing != null) {
                pipeline.getClass().getMethod("remove", String.class).invoke(pipeline, OVERLAY_HANDLER_NAME);
                getLogger().info("[MindAxisView] Overlay interceptor removed");
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    private void installContainerInterceptor(Player player) {
        initContainerReflection();
        if (!containerReflectionAvailable) return;

        try {
            Object craftPlayer = player;
            Method getHandle = craftPlayer.getClass().getMethod("getHandle");
            Object serverPlayer = getHandle.invoke(craftPlayer);

            Field connectionField = serverPlayer.getClass().getField("connection");
            Object gamePacketListener = connectionField.get(serverPlayer);

            Field networkField = findField(gamePacketListener.getClass(), "connection");
            if (networkField == null) {
                networkField = findField(gamePacketListener.getClass().getSuperclass(), "connection");
            }
            if (networkField == null) {
                getLogger().warning("[MindAxisView] Cannot find connection field on " + gamePacketListener.getClass().getName());
                return;
            }
            networkField.setAccessible(true);
            Object networkConnection = networkField.get(gamePacketListener);

            Field channelField = findField(networkConnection.getClass(), "channel");
            if (channelField == null) {
                getLogger().warning("[MindAxisView] Cannot find channel field on " + networkConnection.getClass().getName());
                return;
            }
            channelField.setAccessible(true);
            Object channelObj = channelField.get(networkConnection);

            removeContainerInterceptorFromChannel(channelObj);

            ContainerChannelHandler handler = new ContainerChannelHandler(
                    this,
                    openScreenPacketClass,
                    containerClosePacketClass,
                    containerContentPacketClass,
                    containerSlotPacketClass
            );

            Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
            Class<?> channelHandlerClass = Class.forName("io.netty.channel.ChannelHandler");
            pipeline.getClass()
                    .getMethod("addBefore", String.class, String.class, channelHandlerClass)
                    .invoke(pipeline, "packet_handler", CONTAINER_HANDLER_NAME, handler);

            getLogger().info("[MindAxisView] Container interceptor installed for " + player.getName());
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Failed to install container interceptor: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void removeContainerInterceptor(Player player) {
        if (!containerReflectionAvailable) return;
        try {
            Object craftPlayer = player;
            Method getHandle = craftPlayer.getClass().getMethod("getHandle");
            Object serverPlayer = getHandle.invoke(craftPlayer);
            Field connectionField = serverPlayer.getClass().getField("connection");
            Object gamePacketListener = connectionField.get(serverPlayer);
            Field networkField = findField(gamePacketListener.getClass(), "connection");
            if (networkField == null) networkField = findField(gamePacketListener.getClass().getSuperclass(), "connection");
            if (networkField == null) return;
            networkField.setAccessible(true);
            Object networkConnection = networkField.get(gamePacketListener);
            Field channelField = findField(networkConnection.getClass(), "channel");
            if (channelField == null) return;
            channelField.setAccessible(true);
            Object channelObj = channelField.get(networkConnection);
            removeContainerInterceptorFromChannel(channelObj);
        } catch (Exception e) {
            // Silently ignore — player may have disconnected
        }
    }

    private void removeContainerInterceptorFromChannel(Object channelObj) {
        try {
            Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
            Object existing = pipeline.getClass().getMethod("get", String.class).invoke(pipeline, CONTAINER_HANDLER_NAME);
            if (existing != null) {
                pipeline.getClass().getMethod("remove", String.class).invoke(pipeline, CONTAINER_HANDLER_NAME);
                getLogger().info("[MindAxisView] Container interceptor removed");
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    void onScoreboardObjectivePacketIntercepted(Object packet) {
        if (wsServer == null) return;
        try {
            String objectiveName = stringValue(objectiveGetName.invoke(packet));
            int mode = numberValue(objectiveGetMode.invoke(packet)).intValue();
            if (mode == 1) {
                List<String> messages = new ArrayList<>();
                synchronized (overlayStateLock) {
                    scoreboardObjectives.remove(objectiveName);
                    scoreboardScores.remove(objectiveName);
                    messages.add(OverlayPayloads.scoreboardRemoveObjective(objectiveName));
                    List<String> slotsToClear = new ArrayList<>();
                    for (Map.Entry<String, String> entry : scoreboardDisplays.entrySet()) {
                        if (objectiveName.equals(entry.getValue())) {
                            slotsToClear.add(entry.getKey());
                        }
                    }
                    for (String slot : slotsToClear) {
                        scoreboardDisplays.remove(slot);
                        messages.add(OverlayPayloads.scoreboardSetDisplay(slot, null));
                    }
                }
                broadcastOverlayMessages(messages);
                return;
            }

            String displayName = extractPlainText(objectiveGetDisplayName.invoke(packet));
            String renderType = normalizeNamedValue(objectiveGetRenderType.invoke(packet));
            synchronized (overlayStateLock) {
                scoreboardObjectives.put(objectiveName, new ScoreboardObjectiveState(objectiveName, displayName, renderType));
                scoreboardScores.computeIfAbsent(objectiveName, ignored -> new LinkedHashMap<>());
            }
            broadcastOverlayMessage(OverlayPayloads.scoreboardSetObjective(objectiveName, displayName, renderType));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Scoreboard objective extract failed: " + e.getMessage());
        }
    }

    void onScoreboardScorePacketIntercepted(Object packet) {
        if (wsServer == null) return;
        try {
            String entry = stringValue(scoreGetEntry.invoke(packet));
            String objectiveName = stringValue(scoreGetObjective.invoke(packet));
            int value = numberValue(scoreGetValue.invoke(packet)).intValue();
            String displayName = scoreGetDisplay != null ? extractOptionalPlainText(scoreGetDisplay.invoke(packet)) : null;
            synchronized (overlayStateLock) {
                scoreboardScores
                        .computeIfAbsent(objectiveName, ignored -> new LinkedHashMap<>())
                        .put(entry, new ScoreboardScoreState(objectiveName, entry, value, displayName));
            }
            broadcastOverlayMessage(OverlayPayloads.scoreboardSetScore(objectiveName, entry, value, displayName));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Scoreboard score extract failed: " + e.getMessage());
        }
    }

    void onScoreboardResetScorePacketIntercepted(Object packet) {
        if (wsServer == null) return;
        try {
            String entry = stringValue(resetScoreGetEntry.invoke(packet));
            String objectiveName = resetScoreGetObjective == null ? null : blankToNull(stringValue(resetScoreGetObjective.invoke(packet)));
            synchronized (overlayStateLock) {
                if (objectiveName == null) {
                    for (Map<String, ScoreboardScoreState> objectiveScores : scoreboardScores.values()) {
                        objectiveScores.remove(entry);
                    }
                } else {
                    Map<String, ScoreboardScoreState> objectiveScores = scoreboardScores.get(objectiveName);
                    if (objectiveScores != null) {
                        objectiveScores.remove(entry);
                    }
                }
            }
            broadcastOverlayMessage(OverlayPayloads.scoreboardRemoveScore(objectiveName, entry));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Scoreboard reset extract failed: " + e.getMessage());
        }
    }

    void onScoreboardDisplayPacketIntercepted(Object packet) {
        if (wsServer == null) return;
        try {
            String slot = normalizeDisplaySlot(displayGetSlot.invoke(packet));
            String objectiveName = blankToNull(stringValue(displayGetObjective.invoke(packet)));
            synchronized (overlayStateLock) {
                if (objectiveName == null) {
                    scoreboardDisplays.remove(slot);
                } else {
                    scoreboardDisplays.put(slot, objectiveName);
                }
            }
            broadcastOverlayMessage(OverlayPayloads.scoreboardSetDisplay(slot, objectiveName));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Scoreboard display extract failed: " + e.getMessage());
        }
    }

    void onBossEventPacketIntercepted(Object packet) {
        if (wsServer == null || bossEventDispatch == null || bossEventConsumerClass == null) return;
        try {
            final String[] outbound = new String[1];
            Object consumer = java.lang.reflect.Proxy.newProxyInstance(
                    bossEventConsumerClass.getClassLoader(),
                    new Class[]{bossEventConsumerClass},
                    (proxy, method, args) -> {
                        if (args == null || args.length == 0) return null;
                        outbound[0] = handleBossEventCallback(args);
                        return null;
                    }
            );
            bossEventDispatch.invoke(packet, consumer);
            if (outbound[0] != null) {
                broadcastOverlayMessage(outbound[0]);
            }
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Bossbar extract failed: " + e.getMessage());
        }
    }

    private String handleBossEventCallback(Object[] args) {
        if (!(args[0] instanceof UUID uuid)) return null;
        String id = uuid.toString();

        synchronized (overlayStateLock) {
            if (args.length == 1) {
                bossBars.remove(id);
                return OverlayPayloads.bossbarRemove(id);
            }
            if (args.length == 2 && args[1] instanceof Number number) {
                BossBarState state = bossBars.computeIfAbsent(id,
                        ignored -> new BossBarState(id, "", 0F, "white", "progress", false, false, false));
                state.progress = number.floatValue();
                return OverlayPayloads.bossbarUpdateProgress(id, state.progress);
            }
            if (args.length == 2) {
                BossBarState state = bossBars.computeIfAbsent(id,
                        ignored -> new BossBarState(id, "", 0F, "white", "progress", false, false, false));
                state.name = extractPlainText(args[1]);
                return OverlayPayloads.bossbarUpdateName(id, state.name);
            }
            if (args.length == 4 && args[1] instanceof Boolean) {
                BossBarState state = bossBars.computeIfAbsent(id,
                        ignored -> new BossBarState(id, "", 0F, "white", "progress", false, false, false));
                state.darkenSky = booleanValue(args[1]);
                state.playBossMusic = booleanValue(args[2]);
                state.createWorldFog = booleanValue(args[3]);
                return OverlayPayloads.bossbarUpdateProperties(id, state.darkenSky, state.playBossMusic, state.createWorldFog);
            }
            if (args.length == 3) {
                BossBarState state = bossBars.computeIfAbsent(id,
                        ignored -> new BossBarState(id, "", 0F, "white", "progress", false, false, false));
                state.color = normalizeNamedValue(args[1]);
                state.division = normalizeNamedValue(args[2]);
                return OverlayPayloads.bossbarUpdateStyle(id, state.color, state.division);
            }
            if (args.length == 8) {
                BossBarState state = new BossBarState(
                        id,
                        extractPlainText(args[1]),
                        numberValue(args[2]).floatValue(),
                        normalizeNamedValue(args[3]),
                        normalizeNamedValue(args[4]),
                        booleanValue(args[5]),
                        booleanValue(args[6]),
                        booleanValue(args[7])
                );
                bossBars.put(id, state);
                return OverlayPayloads.bossbarAdd(
                        state.id,
                        state.name,
                        state.progress,
                        state.color,
                        state.division,
                        state.darkenSky,
                        state.playBossMusic,
                        state.createWorldFog
                );
            }
        }

        return null;
    }

    private List<String> syncScoreboardStateFromPlayer(Player player) {
        Map<String, ScoreboardObjectiveState> nextObjectives = new LinkedHashMap<>();
        Map<String, Map<String, ScoreboardScoreState>> nextScores = new LinkedHashMap<>();
        Map<String, String> nextDisplays = new LinkedHashMap<>();

        if (player != null && player.isOnline()) {
            Scoreboard scoreboard = player.getScoreboard();
            if (scoreboard != null) {
                for (Objective objective : scoreboard.getObjectives()) {
                    String objectiveName = objective.getName();
                    String displayName = objective.getDisplayName();
                    String renderType = objective.getRenderType() == null
                            ? "integer"
                            : objective.getRenderType().name().toLowerCase(Locale.ROOT);
                    nextObjectives.put(objectiveName, new ScoreboardObjectiveState(objectiveName, displayName, renderType));

                    Map<String, ScoreboardScoreState> objectiveScores = new LinkedHashMap<>();
                    for (String entry : scoreboard.getEntries()) {
                        org.bukkit.scoreboard.Score score = objective.getScore(entry);
                        if (!score.isScoreSet()) continue;
                        String customName = score.customName() == null ? null : PLAIN_TEXT.serialize(score.customName());
                        objectiveScores.put(entry, new ScoreboardScoreState(objectiveName, entry, score.getScore(), customName));
                    }
                    nextScores.put(objectiveName, objectiveScores);
                }

                for (DisplaySlot slot : DisplaySlot.values()) {
                    Objective objective = scoreboard.getObjective(slot);
                    if (objective != null) {
                        nextDisplays.put(slot.getId(), objective.getName());
                    }
                }
            }
        }

        synchronized (overlayStateLock) {
            scoreboardObjectives.clear();
            scoreboardObjectives.putAll(nextObjectives);
            scoreboardScores.clear();
            scoreboardScores.putAll(nextScores);
            scoreboardDisplays.clear();
            scoreboardDisplays.putAll(nextDisplays);
            return buildCurrentScoreboardMessages();
        }
    }

    private List<String> syncBossBarsFromPlayer(Player player) {
        initOverlayReflection();
        Map<String, BossBarState> nextBossBars = new LinkedHashMap<>();

        if (player != null && player.isOnline()) {
            for (BossBar bossBar : player.activeBossBars()) {
                String id = resolveBossBarId(bossBar);
                if (id == null || id.isBlank()) continue;
                nextBossBars.put(id, new BossBarState(
                        id,
                        PLAIN_TEXT.serialize(bossBar.name()),
                        bossBar.progress(),
                        bossBar.color().name().toLowerCase(Locale.ROOT),
                        bossBar.overlay().name().toLowerCase(Locale.ROOT),
                        bossBar.hasFlag(BossBar.Flag.DARKEN_SCREEN),
                        bossBar.hasFlag(BossBar.Flag.PLAY_BOSS_MUSIC),
                        bossBar.hasFlag(BossBar.Flag.CREATE_WORLD_FOG)
                ));
            }
        }

        synchronized (overlayStateLock) {
            bossBars.clear();
            bossBars.putAll(nextBossBars);
            return buildCurrentBossBarMessages();
        }
    }

    private List<String> buildCurrentScoreboardMessages() {
        List<String> messages = new ArrayList<>();
        for (ScoreboardObjectiveState objective : scoreboardObjectives.values()) {
            messages.add(OverlayPayloads.scoreboardSetObjective(
                    objective.name,
                    objective.displayName,
                    objective.renderType
            ));
        }
        for (Map<String, ScoreboardScoreState> objectiveScores : scoreboardScores.values()) {
            for (ScoreboardScoreState score : objectiveScores.values()) {
                messages.add(OverlayPayloads.scoreboardSetScore(
                        score.objective,
                        score.entry,
                        score.value,
                        score.displayName
                ));
            }
        }
        for (Map.Entry<String, String> display : scoreboardDisplays.entrySet()) {
            messages.add(OverlayPayloads.scoreboardSetDisplay(display.getKey(), display.getValue()));
        }
        return messages;
    }

    private List<String> buildCurrentBossBarMessages() {
        List<String> messages = new ArrayList<>();
        for (BossBarState bossBar : bossBars.values()) {
            messages.add(OverlayPayloads.bossbarAdd(
                    bossBar.id,
                    bossBar.name,
                    bossBar.progress,
                    bossBar.color,
                    bossBar.division,
                    bossBar.darkenSky,
                    bossBar.playBossMusic,
                    bossBar.createWorldFog
            ));
        }
        return messages;
    }

    private void broadcastOverlayMessage(String json) {
        if (json == null || wsServer == null) return;
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastMessage(json));
    }

    private void broadcastOverlayMessages(List<String> messages) {
        if (messages == null || messages.isEmpty() || wsServer == null) return;
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            for (String json : messages) {
                wsServer.broadcastMessage(json);
            }
        });
    }

    private String resolveBossBarId(BossBar bossBar) {
        if (bossBar == null || bossBarImplementationImplClass == null
                || bossBarImplementationVanillaField == null || bossBattleGetId == null) {
            return null;
        }
        try {
            Object implementation = BossBarImplementation.get(bossBar, (Class) bossBarImplementationImplClass);
            if (implementation == null) return null;
            Object vanilla = bossBarImplementationVanillaField.get(implementation);
            if (vanilla == null) return null;
            Object id = bossBattleGetId.invoke(vanilla);
            return id == null ? null : id.toString();
        } catch (Exception e) {
            return null;
        }
    }

    private static Method requireBossEventDispatchMethod(Class<?> packetClass) throws NoSuchMethodException {
        Method fallback = null;
        for (String name : new String[]{"dispatch", "accept", "a"}) {
            for (Method method : packetClass.getDeclaredMethods()) {
                if (!method.getName().equals(name) || method.getParameterCount() != 1) continue;
                Class<?> parameterType = method.getParameterTypes()[0];
                if (!parameterType.isInterface()) continue;
                if (parameterType.getEnclosingClass() == packetClass || parameterType.getName().startsWith(packetClass.getName() + "$")) {
                    method.setAccessible(true);
                    return method;
                }
                if (fallback == null) {
                    method.setAccessible(true);
                    fallback = method;
                }
            }
            for (Method method : packetClass.getMethods()) {
                if (!method.getName().equals(name) || method.getParameterCount() != 1) continue;
                Class<?> parameterType = method.getParameterTypes()[0];
                if (!parameterType.isInterface()) continue;
                if (parameterType.getEnclosingClass() == packetClass || parameterType.getName().startsWith(packetClass.getName() + "$")) {
                    method.setAccessible(true);
                    return method;
                }
                if (fallback == null) {
                    method.setAccessible(true);
                    fallback = method;
                }
            }
        }
        if (fallback != null) {
            return fallback;
        }
        throw new NoSuchMethodException(packetClass.getName() + " :: dispatch/accept/a(interface)");
    }

    private static String extractOptionalPlainText(Object value) {
        if (value instanceof java.util.Optional<?> optional) {
            return optional.isPresent() ? extractPlainText(optional.get()) : null;
        }
        return extractPlainText(value);
    }

    private static String extractPlainText(Object value) {
        if (value == null || value == JsonNull.INSTANCE) return "";
        if (value instanceof String string) return string;
        if (value instanceof net.kyori.adventure.text.Component component) {
            return PLAIN_TEXT.serialize(component);
        }
        try {
            Method getString = value.getClass().getMethod("getString");
            Object string = getString.invoke(value);
            if (string != null) return string.toString();
        } catch (Exception ignored) {
        }
        return value.toString();
    }

    private static Number numberValue(Object value) {
        if (value instanceof Number number) return number;
        throw new IllegalArgumentException("Expected number, got: " + value);
    }

    private static boolean booleanValue(Object value) {
        return Boolean.TRUE.equals(value);
    }

    private static String stringValue(Object value) {
        return value == null ? "" : value.toString();
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static String normalizeDisplaySlot(Object value) {
        if (value instanceof DisplaySlot slot) {
            return slot.getId();
        }
        String named = normalizeNamedValue(value);
        if (named.startsWith("sidebar_team_")) {
            return "sidebar.team." + named.substring("sidebar_team_".length());
        }
        return named;
    }

    private static String normalizeNamedValue(Object value) {
        if (value == null) return "";
        for (String methodName : new String[]{"getName", "getSerializedName", "getId", "asString", "c", "b", "a"}) {
            Method method = findMethod(value.getClass(), 0, methodName);
            if (method == null) continue;
            try {
                Object result = method.invoke(value);
                if (result instanceof String string && !string.isBlank()) {
                    return string.toLowerCase(Locale.ROOT);
                }
            } catch (Exception ignored) {
            }
        }
        if (value instanceof Enum<?> enumValue) {
            return enumValue.name().toLowerCase(Locale.ROOT);
        }
        return value.toString().toLowerCase(Locale.ROOT);
    }

    private static final class ScoreboardObjectiveState {
        private final String name;
        private final String displayName;
        private final String renderType;

        private ScoreboardObjectiveState(String name, String displayName, String renderType) {
            this.name = name;
            this.displayName = displayName == null ? "" : displayName;
            this.renderType = (renderType == null || renderType.isBlank()) ? "integer" : renderType;
        }
    }

    private static final class ScoreboardScoreState {
        private final String objective;
        private final String entry;
        private final int value;
        private final String displayName;

        private ScoreboardScoreState(String objective, String entry, int value, String displayName) {
            this.objective = objective;
            this.entry = entry;
            this.value = value;
            this.displayName = displayName;
        }
    }

    private static final class BossBarState {
        private final String id;
        private String name;
        private float progress;
        private String color;
        private String division;
        private boolean darkenSky;
        private boolean playBossMusic;
        private boolean createWorldFog;

        private BossBarState(
                String id,
                String name,
                float progress,
                String color,
                String division,
                boolean darkenSky,
                boolean playBossMusic,
                boolean createWorldFog
        ) {
            this.id = id;
            this.name = name == null ? "" : name;
            this.progress = progress;
            this.color = (color == null || color.isBlank()) ? "white" : color;
            this.division = (division == null || division.isBlank()) ? "progress" : division;
            this.darkenSky = darkenSky;
            this.playBossMusic = playBossMusic;
            this.createWorldFog = createWorldFog;
        }
    }

    // ---- NMS Sound Packet Interceptor (reflection-based) ----

    // Cached reflection state for sound packet interception.
    // Resolved once in initSoundReflection(); null if NMS not available.
    private static volatile boolean soundReflectionInitialized = false;
    private static volatile boolean soundReflectionAvailable = false;
    private static Class<?> soundPacketClass;       // ClientboundSoundPacket
    private static Class<?> entitySoundPacketClass;  // ClientboundSoundEntityPacket
    // ClientboundSoundPacket methods
    private static Method spGetSound;   // RegistryEntry<SoundEvent>
    private static Method spGetX;       // double
    private static Method spGetY;       // double
    private static Method spGetZ;       // double
    private static Method spGetVolume;  // float
    private static Method spGetPitch;   // float
    private static Method spGetSource;  // SoundSource
    // ClientboundSoundEntityPacket methods
    private static Method espGetSound;
    private static Method espGetId;     // int (entity id)
    private static Method espGetVolume;
    private static Method espGetPitch;
    private static Method espGetSource;
    // RegistryEntry.value() → SoundEvent
    private static Method registryEntryValue;
    // SoundEvent.location() → Identifier/ResourceLocation (Mojang record-style accessor)
    private static Method soundEventGetLocation;
    // SoundSource/SoundCategory .getName() → String
    private static Method soundSourceGetName;

    // Cached reflection state for block event packet interception.
    private static volatile boolean blockEventReflectionInitialized = false;
    private static volatile boolean blockEventReflectionAvailable = false;
    private static Class<?> blockEventPacketClass;   // ClientboundBlockEventPacket
    private static Method bepGetPos;
    private static Method bepGetB0;
    private static Method bepGetB1;
    private static Method bepGetBlock;
    private static Method blockPosGetX;
    private static Method blockPosGetY;
    private static Method blockPosGetZ;
    private static Object blockRegistry;             // BuiltInRegistries.BLOCK
    private static Method blockRegistryGetId;       // Registry.getId(block)

    // Cached reflection state for particle packet interception.
    private static volatile boolean particleReflectionInitialized = false;
    private static volatile boolean particleReflectionAvailable = false;
    private static Class<?> particlePacketClass;     // ClientboundLevelParticlesPacket
    private static Method ppGetParticle;             // ParticleOptions / ParticleParam
    private static Method ppGetX;                    // double
    private static Method ppGetY;                    // double
    private static Method ppGetZ;                    // double
    private static Method ppGetOffsetX;              // float
    private static Method ppGetOffsetY;              // float
    private static Method ppGetOffsetZ;              // float
    private static Method ppGetSpeed;                // float
    private static Method ppGetCount;                // int
    private static Method particleOptionsGetType;    // ParticleType / Particle
    private static Object particleTypeRegistry;      // BuiltInRegistries.PARTICLE_TYPE
    private static Method particleRegistryGetKey;    // Registry.getKey(type)
    private static Class<?> blockParticleOptionClass;
    private static Method blockParticleGetState;
    private static Method blockGetId;

    /**
     * Initialize reflection handles for NMS sound packet classes.
     * Safe to call multiple times; only runs once.
     */
    private void initSoundReflection() {
        if (soundReflectionInitialized) return;
        soundReflectionInitialized = true;
        try {
            // Load NMS packet classes (Mojang-mapped names on Paper 1.20.5+)
            soundPacketClass = Class.forName("net.minecraft.network.protocol.game.ClientboundSoundPacket");
            entitySoundPacketClass = Class.forName("net.minecraft.network.protocol.game.ClientboundSoundEntityPacket");

            // ClientboundSoundPacket accessors
            spGetSound = soundPacketClass.getMethod("getSound");
            spGetX = soundPacketClass.getMethod("getX");
            spGetY = soundPacketClass.getMethod("getY");
            spGetZ = soundPacketClass.getMethod("getZ");
            spGetVolume = soundPacketClass.getMethod("getVolume");
            spGetPitch = soundPacketClass.getMethod("getPitch");
            spGetSource = soundPacketClass.getMethod("getSource");

            // ClientboundSoundEntityPacket accessors
            espGetSound = entitySoundPacketClass.getMethod("getSound");
            espGetId = entitySoundPacketClass.getMethod("getId");
            espGetVolume = entitySoundPacketClass.getMethod("getVolume");
            espGetPitch = entitySoundPacketClass.getMethod("getPitch");
            espGetSource = entitySoundPacketClass.getMethod("getSource");

            // RegistryEntry<SoundEvent>.value() → SoundEvent
            Class<?> registryEntryClass = Class.forName("net.minecraft.core.Holder");
            registryEntryValue = registryEntryClass.getMethod("value");

            // SoundEvent.location() → Identifier (Mojang record-style accessor)
            Class<?> soundEventClass = Class.forName("net.minecraft.sounds.SoundEvent");
            soundEventGetLocation = soundEventClass.getMethod("location");

            // SoundSource.getName() → String
            Class<?> soundSourceClass = Class.forName("net.minecraft.sounds.SoundSource");
            soundSourceGetName = soundSourceClass.getMethod("getName");

            soundReflectionAvailable = true;
            getLogger().info("[MindAxisView] Sound packet reflection initialized successfully");
        } catch (Exception e) {
            soundReflectionAvailable = false;
            getLogger().warning("[MindAxisView] Sound packet reflection not available: " + e.getMessage());
            getLogger().warning("[MindAxisView] Sound interception will be disabled");
        }
    }

    /**
     * Initialize reflection handles for NMS block event packet classes.
     * Safe to call multiple times; only runs once.
     */
    private void initBlockEventReflection() {
        if (blockEventReflectionInitialized) return;
        blockEventReflectionInitialized = true;
        try {
            blockEventPacketClass = loadClass("net.minecraft.network.protocol.game.ClientboundBlockEventPacket");
            bepGetPos = requireMethod(blockEventPacketClass, 0, "getPos");
            bepGetB0 = requireMethod(blockEventPacketClass, 0, "getB0");
            bepGetB1 = requireMethod(blockEventPacketClass, 0, "getB1");
            bepGetBlock = requireMethod(blockEventPacketClass, 0, "getBlock");

            Class<?> blockPosClass = loadClass("net.minecraft.core.BlockPos");
            blockPosGetX = requireMethod(blockPosClass, 0, "getX");
            blockPosGetY = requireMethod(blockPosClass, 0, "getY");
            blockPosGetZ = requireMethod(blockPosClass, 0, "getZ");

            Class<?> builtInRegistriesClass = loadClass("net.minecraft.core.registries.BuiltInRegistries");
            Field blockRegistryField = findFieldAny(builtInRegistriesClass, "BLOCK");
            if (blockRegistryField == null) {
                throw new NoSuchFieldException("BuiltInRegistries.BLOCK");
            }
            blockRegistryField.setAccessible(true);
            blockRegistry = blockRegistryField.get(null);
            blockRegistryGetId = requireMethod(blockRegistry.getClass(), 1, "getId");

            blockEventReflectionAvailable = true;
            getLogger().info("[MindAxisView] Block event packet reflection initialized successfully");
        } catch (Exception e) {
            blockEventReflectionAvailable = false;
            getLogger().warning("[MindAxisView] Block event packet reflection not available: " + e.getMessage());
            getLogger().warning("[MindAxisView] Block event interception will be disabled");
        }
    }

    /**
     * Initialize reflection handles for NMS particle packet classes.
     * Safe to call multiple times; only runs once.
     */
    private void initParticleReflection() {
        if (particleReflectionInitialized) return;
        particleReflectionInitialized = true;
        try {
            particlePacketClass = loadClass(
                    "net.minecraft.network.protocol.game.ClientboundLevelParticlesPacket",
                    "net.minecraft.network.protocol.game.PacketPlayOutWorldParticles"
            );
            ppGetParticle = requireMethod(particlePacketClass, 0, "getParticle", "getParameters", "n");
            ppGetX = requireMethod(particlePacketClass, 0, "getX", "f");
            ppGetY = requireMethod(particlePacketClass, 0, "getY", "g");
            ppGetZ = requireMethod(particlePacketClass, 0, "getZ", "h");
            ppGetOffsetX = requireMethod(particlePacketClass, 0, "getXDist", "getOffsetX", "i");
            ppGetOffsetY = requireMethod(particlePacketClass, 0, "getYDist", "getOffsetY", "j");
            ppGetOffsetZ = requireMethod(particlePacketClass, 0, "getZDist", "getOffsetZ", "k");
            ppGetSpeed = requireMethod(particlePacketClass, 0, "getMaxSpeed", "getSpeed", "l");
            ppGetCount = requireMethod(particlePacketClass, 0, "getCount", "m");

            Class<?> particleOptionsClass = loadClass(
                    "net.minecraft.core.particles.ParticleOptions",
                    "net.minecraft.core.particles.ParticleParam"
            );
            particleOptionsGetType = requireMethod(particleOptionsClass, 0, "getType", "a");

            Class<?> builtInRegistriesClass = loadClass("net.minecraft.core.registries.BuiltInRegistries");
            Field particleRegistryField = findFieldAny(builtInRegistriesClass, "PARTICLE_TYPE", "j");
            if (particleRegistryField == null) {
                throw new NoSuchFieldException("BuiltInRegistries.PARTICLE_TYPE");
            }
            particleRegistryField.setAccessible(true);
            particleTypeRegistry = particleRegistryField.get(null);
            particleRegistryGetKey = requireMethod(particleTypeRegistry.getClass(), 1, "getKey", "b");

            blockParticleOptionClass = tryLoadClass(
                    "net.minecraft.core.particles.BlockParticleOption",
                    "net.minecraft.core.particles.ParticleParamBlock"
            );
            if (blockParticleOptionClass != null) {
                blockParticleGetState = requireMethod(blockParticleOptionClass, 0, "getState", "getBlockState", "b");
            }
            Class<?> blockClass = tryLoadClass("net.minecraft.world.level.block.Block");
            if (blockClass != null) {
                blockGetId = requireMethod(blockClass, 1, "getId", "j");
            }

            particleReflectionAvailable = true;
            getLogger().info("[MindAxisView] Particle packet reflection initialized successfully");
        } catch (Exception e) {
            particleReflectionAvailable = false;
            getLogger().warning("[MindAxisView] Particle packet reflection not available: " + e.getMessage());
            getLogger().warning("[MindAxisView] Particle interception will be disabled");
        }
    }

    /**
     * Install a Netty ChannelDuplexHandler on the anchor player's network channel
     * to intercept ALL outgoing sound packets. Uses reflection for all NMS access.
     */
    private void installSoundInterceptor(Player player) {
        initSoundReflection();
        if (!soundReflectionAvailable) return;

        try {
            // Get Netty Channel via reflection: CraftPlayer.getHandle() → ServerPlayer
            // → .connection (ServerGamePacketListenerImpl) → .connection (Connection) → .channel
            Object craftPlayer = player;
            Method getHandle = craftPlayer.getClass().getMethod("getHandle");
            Object serverPlayer = getHandle.invoke(craftPlayer);

            // ServerPlayer.connection → ServerGamePacketListenerImpl
            Field connectionField = serverPlayer.getClass().getField("connection");
            Object gamePacketListener = connectionField.get(serverPlayer);

            // ServerGamePacketListenerImpl.connection → Connection
            // Paper 1.21+: the field name is "connection" in Mojang mappings
            Field networkField = findField(gamePacketListener.getClass(), "connection");
            if (networkField == null) {
                // Fallback: try superclass fields
                networkField = findField(gamePacketListener.getClass().getSuperclass(), "connection");
            }
            if (networkField == null) {
                getLogger().warning("[MindAxisView] Cannot find connection field on " + gamePacketListener.getClass().getName());
                return;
            }
            networkField.setAccessible(true);
            Object networkConnection = networkField.get(gamePacketListener);

            // Connection.channel → Netty Channel
            Field channelField = findField(networkConnection.getClass(), "channel");
            if (channelField == null) {
                getLogger().warning("[MindAxisView] Cannot find channel field on " + networkConnection.getClass().getName());
                return;
            }
            channelField.setAccessible(true);
            Object channelObj = channelField.get(networkConnection);

            // Remove existing handler if present
            removeSoundInterceptorFromChannel(channelObj);

            // Create the SoundChannelHandler (extends ChannelDuplexHandler)
            SoundChannelHandler handler = new SoundChannelHandler(this, soundPacketClass, entitySoundPacketClass);

            // channel.pipeline().addBefore("packet_handler", SOUND_HANDLER_NAME, handler)
            Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
            Class<?> channelHandlerClass = Class.forName("io.netty.channel.ChannelHandler");
            pipeline.getClass()
                    .getMethod("addBefore", String.class, String.class, channelHandlerClass)
                    .invoke(pipeline, "packet_handler", SOUND_HANDLER_NAME, handler);

            getLogger().info("[MindAxisView] Sound interceptor installed for " + player.getName());
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Failed to install sound interceptor: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Remove the sound interceptor from a player's Netty pipeline.
     */
    private void removeSoundInterceptor(Player player) {
        if (!soundReflectionAvailable) return;
        try {
            Object craftPlayer = player;
            Method getHandle = craftPlayer.getClass().getMethod("getHandle");
            Object serverPlayer = getHandle.invoke(craftPlayer);
            Field connectionField = serverPlayer.getClass().getField("connection");
            Object gamePacketListener = connectionField.get(serverPlayer);
            Field networkField = findField(gamePacketListener.getClass(), "connection");
            if (networkField == null) networkField = findField(gamePacketListener.getClass().getSuperclass(), "connection");
            if (networkField == null) return;
            networkField.setAccessible(true);
            Object networkConnection = networkField.get(gamePacketListener);
            Field channelField = findField(networkConnection.getClass(), "channel");
            if (channelField == null) return;
            channelField.setAccessible(true);
            Object channelObj = channelField.get(networkConnection);
            removeSoundInterceptorFromChannel(channelObj);
        } catch (Exception e) {
            // Silently ignore — player may have disconnected
        }
    }

    private void removeSoundInterceptorFromChannel(Object channelObj) {
        try {
            Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
            Object existing = pipeline.getClass().getMethod("get", String.class).invoke(pipeline, SOUND_HANDLER_NAME);
            if (existing != null) {
                pipeline.getClass().getMethod("remove", String.class).invoke(pipeline, SOUND_HANDLER_NAME);
                getLogger().info("[MindAxisView] Sound interceptor removed");
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    /**
     * Install a Netty ChannelDuplexHandler on the anchor player's network channel
     * to intercept ALL outgoing block event packets. Uses reflection for all NMS access.
     */
    private void installBlockEventInterceptor(Player player) {
        initBlockEventReflection();
        if (!blockEventReflectionAvailable) return;

        try {
            Object craftPlayer = player;
            Method getHandle = craftPlayer.getClass().getMethod("getHandle");
            Object serverPlayer = getHandle.invoke(craftPlayer);

            Field connectionField = serverPlayer.getClass().getField("connection");
            Object gamePacketListener = connectionField.get(serverPlayer);

            Field networkField = findField(gamePacketListener.getClass(), "connection");
            if (networkField == null) {
                networkField = findField(gamePacketListener.getClass().getSuperclass(), "connection");
            }
            if (networkField == null) {
                getLogger().warning("[MindAxisView] Cannot find connection field on " + gamePacketListener.getClass().getName());
                return;
            }
            networkField.setAccessible(true);
            Object networkConnection = networkField.get(gamePacketListener);

            Field channelField = findField(networkConnection.getClass(), "channel");
            if (channelField == null) {
                getLogger().warning("[MindAxisView] Cannot find channel field on " + networkConnection.getClass().getName());
                return;
            }
            channelField.setAccessible(true);
            Object channelObj = channelField.get(networkConnection);

            removeBlockEventInterceptorFromChannel(channelObj);

            BlockEventChannelHandler handler = new BlockEventChannelHandler(this, blockEventPacketClass);

            Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
            Class<?> channelHandlerClass = Class.forName("io.netty.channel.ChannelHandler");
            pipeline.getClass()
                    .getMethod("addBefore", String.class, String.class, channelHandlerClass)
                    .invoke(pipeline, "packet_handler", BLOCK_EVENT_HANDLER_NAME, handler);

            getLogger().info("[MindAxisView] Block event interceptor installed for " + player.getName());
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Failed to install block event interceptor: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Remove the block event interceptor from a player's Netty pipeline.
     */
    private void removeBlockEventInterceptor(Player player) {
        if (!blockEventReflectionAvailable) return;
        try {
            Object craftPlayer = player;
            Method getHandle = craftPlayer.getClass().getMethod("getHandle");
            Object serverPlayer = getHandle.invoke(craftPlayer);
            Field connectionField = serverPlayer.getClass().getField("connection");
            Object gamePacketListener = connectionField.get(serverPlayer);
            Field networkField = findField(gamePacketListener.getClass(), "connection");
            if (networkField == null) networkField = findField(gamePacketListener.getClass().getSuperclass(), "connection");
            if (networkField == null) return;
            networkField.setAccessible(true);
            Object networkConnection = networkField.get(gamePacketListener);
            Field channelField = findField(networkConnection.getClass(), "channel");
            if (channelField == null) return;
            channelField.setAccessible(true);
            Object channelObj = channelField.get(networkConnection);
            removeBlockEventInterceptorFromChannel(channelObj);
        } catch (Exception e) {
            // Silently ignore — player may have disconnected
        }
    }

    private void removeBlockEventInterceptorFromChannel(Object channelObj) {
        try {
            Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
            Object existing = pipeline.getClass().getMethod("get", String.class).invoke(pipeline, BLOCK_EVENT_HANDLER_NAME);
            if (existing != null) {
                pipeline.getClass().getMethod("remove", String.class).invoke(pipeline, BLOCK_EVENT_HANDLER_NAME);
                getLogger().info("[MindAxisView] Block event interceptor removed");
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    /**
     * Install a Netty ChannelDuplexHandler on the anchor player's network channel
     * to intercept ALL outgoing particle packets. Uses reflection for all NMS access.
     */
    private void installParticleInterceptor(Player player) {
        initParticleReflection();
        if (!particleReflectionAvailable) return;

        try {
            Object craftPlayer = player;
            Method getHandle = craftPlayer.getClass().getMethod("getHandle");
            Object serverPlayer = getHandle.invoke(craftPlayer);

            Field connectionField = serverPlayer.getClass().getField("connection");
            Object gamePacketListener = connectionField.get(serverPlayer);

            Field networkField = findField(gamePacketListener.getClass(), "connection");
            if (networkField == null) {
                networkField = findField(gamePacketListener.getClass().getSuperclass(), "connection");
            }
            if (networkField == null) {
                getLogger().warning("[MindAxisView] Cannot find connection field on " + gamePacketListener.getClass().getName());
                return;
            }
            networkField.setAccessible(true);
            Object networkConnection = networkField.get(gamePacketListener);

            Field channelField = findField(networkConnection.getClass(), "channel");
            if (channelField == null) {
                getLogger().warning("[MindAxisView] Cannot find channel field on " + networkConnection.getClass().getName());
                return;
            }
            channelField.setAccessible(true);
            Object channelObj = channelField.get(networkConnection);

            removeParticleInterceptorFromChannel(channelObj);

            ParticleChannelHandler handler = new ParticleChannelHandler(this, particlePacketClass);

            Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
            Class<?> channelHandlerClass = Class.forName("io.netty.channel.ChannelHandler");
            pipeline.getClass()
                    .getMethod("addBefore", String.class, String.class, channelHandlerClass)
                    .invoke(pipeline, "packet_handler", PARTICLE_HANDLER_NAME, handler);

            getLogger().info("[MindAxisView] Particle interceptor installed for " + player.getName());
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Failed to install particle interceptor: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Remove the particle interceptor from a player's Netty pipeline.
     */
    private void removeParticleInterceptor(Player player) {
        if (!particleReflectionAvailable) return;
        try {
            Object craftPlayer = player;
            Method getHandle = craftPlayer.getClass().getMethod("getHandle");
            Object serverPlayer = getHandle.invoke(craftPlayer);
            Field connectionField = serverPlayer.getClass().getField("connection");
            Object gamePacketListener = connectionField.get(serverPlayer);
            Field networkField = findField(gamePacketListener.getClass(), "connection");
            if (networkField == null) networkField = findField(gamePacketListener.getClass().getSuperclass(), "connection");
            if (networkField == null) return;
            networkField.setAccessible(true);
            Object networkConnection = networkField.get(gamePacketListener);
            Field channelField = findField(networkConnection.getClass(), "channel");
            if (channelField == null) return;
            channelField.setAccessible(true);
            Object channelObj = channelField.get(networkConnection);
            removeParticleInterceptorFromChannel(channelObj);
        } catch (Exception e) {
            // Silently ignore — player may have disconnected
        }
    }

    private void removeParticleInterceptorFromChannel(Object channelObj) {
        try {
            Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
            Object existing = pipeline.getClass().getMethod("get", String.class).invoke(pipeline, PARTICLE_HANDLER_NAME);
            if (existing != null) {
                pipeline.getClass().getMethod("remove", String.class).invoke(pipeline, PARTICLE_HANDLER_NAME);
                getLogger().info("[MindAxisView] Particle interceptor removed");
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    /**
     * Callback from ParticleChannelHandler when a particle packet is intercepted.
     * @param packet the NMS packet object
     */
    void onParticlePacketIntercepted(Object packet) {
        broadcastParticlePacketReflective(packet);
    }

    void onContainerOpenPacketIntercepted(Object packet) {
        broadcastContainerOpenPacketReflective(packet);
    }

    void onContainerClosePacketIntercepted(Object packet) {
        broadcastContainerClosePacketReflective(packet);
    }

    void onContainerContentPacketIntercepted(Object packet) {
        broadcastContainerContentPacketReflective(packet);
    }

    void onContainerSlotPacketIntercepted(Object packet) {
        broadcastContainerSlotPacketReflective(packet);
    }

    /**
     * Callback from BlockEventChannelHandler when a block event packet is intercepted.
     * @param packet the NMS packet object
     */
    void onBlockEventPacketIntercepted(Object packet) {
        broadcastBlockActionPacketReflective(packet);
    }

    /**
     * Callback from SoundChannelHandler when a sound packet is intercepted.
     * @param packet the NMS packet object
     * @param isEntitySound true if ClientboundSoundEntityPacket, false if ClientboundSoundPacket
     */
    void onSoundPacketIntercepted(Object packet, boolean isEntitySound) {
        if (isEntitySound) {
            broadcastEntitySoundPacketReflective(packet);
        } else {
            broadcastSoundPacketReflective(packet);
        }
    }

    private void broadcastContainerOpenPacketReflective(Object packet) {
        if (wsServer == null) return;
        try {
            int windowId = ((Number) openScreenGetContainerId.invoke(packet)).intValue();
            Object menuType = openScreenGetType.invoke(packet);
            String containerType = resolveRegistryKey(menuRegistry, menuType);
            String title = componentToPlainString(openScreenGetTitle.invoke(packet));
            int slots = resolveContainerSlotCount(containerType);

            String json = ContainerPayloads.containerOpen(windowId, containerType, title, slots);
            Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(json));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Container open packet extract failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void broadcastContainerClosePacketReflective(Object packet) {
        if (wsServer == null) return;
        try {
            int windowId = ((Number) containerCloseGetContainerId.invoke(packet)).intValue();
            String json = ContainerPayloads.containerClose(windowId);
            Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(json));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Container close packet extract failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void broadcastContainerContentPacketReflective(Object packet) {
        if (wsServer == null) return;
        try {
            int windowId = ((Number) containerContentGetContainerId.invoke(packet)).intValue();
            List<?> items = (List<?>) containerContentGetItems.invoke(packet);
            JsonArray serializedItems = serializeContainerItems(items);
            String json = ContainerPayloads.containerContent(windowId, serializedItems);
            Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(json));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Container content packet extract failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void broadcastContainerSlotPacketReflective(Object packet) {
        if (wsServer == null) return;
        try {
            int windowId = ((Number) containerSlotGetContainerId.invoke(packet)).intValue();
            int slot = ((Number) containerSlotGetSlot.invoke(packet)).intValue();
            JsonElement item = serializeContainerItem(containerSlotGetItem.invoke(packet), slot);
            String json = ContainerPayloads.containerSlot(windowId, slot, item);
            Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(json));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Container slot packet extract failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /**
     * Extract sound data from a positional sound packet via reflection and broadcast.
     */
    private void broadcastSoundPacketReflective(Object packet) {
        if (wsServer == null) return;
        try {
            Object registryEntry = spGetSound.invoke(packet);
            Object soundEvent = registryEntryValue.invoke(registryEntry);
            Object resourceLocation = soundEventGetLocation.invoke(soundEvent);
            String soundName = resourceLocation.toString();

            double x = (double) spGetX.invoke(packet);
            double y = (double) spGetY.invoke(packet);
            double z = (double) spGetZ.invoke(packet);
            float volume = (float) spGetVolume.invoke(packet);
            float pitch = (float) spGetPitch.invoke(packet);
            Object soundSource = spGetSource.invoke(packet);
            String category = (String) soundSourceGetName.invoke(soundSource);

            JsonObject msg = new JsonObject();
            msg.addProperty("type", "sound");
            msg.addProperty("name", soundName);
            msg.addProperty("x", x);
            msg.addProperty("y", y);
            msg.addProperty("z", z);
            msg.addProperty("volume", volume);
            msg.addProperty("pitch", pitch);
            msg.addProperty("category", category);
            String json = msg.toString();
            Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(json));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Sound packet extract failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void broadcastBlockActionPacketReflective(Object packet) {
        if (wsServer == null) return;
        try {
            Object pos = bepGetPos.invoke(packet);
            int x = ((Number) blockPosGetX.invoke(pos)).intValue();
            int y = ((Number) blockPosGetY.invoke(pos)).intValue();
            int z = ((Number) blockPosGetZ.invoke(pos)).intValue();
            int actionId = ((Number) bepGetB0.invoke(packet)).intValue();
            int actionParam = ((Number) bepGetB1.invoke(packet)).intValue();
            Object block = bepGetBlock.invoke(packet);
            int blockId = ((Number) blockRegistryGetId.invoke(blockRegistry, block)).intValue();

            String json = buildBlockActionMessage(x, y, z, actionId, actionParam, blockId).toString();
            Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(json));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Block event packet extract failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    static JsonObject buildBlockActionMessage(int x, int y, int z, int actionId, int actionParam, int blockId) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "blockAction");
        msg.addProperty("x", x);
        msg.addProperty("y", y);
        msg.addProperty("z", z);
        msg.addProperty("actionId", actionId);
        msg.addProperty("actionParam", actionParam);
        msg.addProperty("blockId", blockId);
        return msg;
    }

    /**
     * Extract particle data from a particle packet via reflection and broadcast.
     */
    private void broadcastParticlePacketReflective(Object packet) {
        if (wsServer == null) return;
        try {
            Object particleOptions = ppGetParticle.invoke(packet);
            String particleName = resolveParticleName(particleOptions);
            if (particleName == null || particleName.isEmpty()) return;

            double x = ((Number) ppGetX.invoke(packet)).doubleValue();
            double y = ((Number) ppGetY.invoke(packet)).doubleValue();
            double z = ((Number) ppGetZ.invoke(packet)).doubleValue();
            double dx = ((Number) ppGetOffsetX.invoke(packet)).doubleValue();
            double dy = ((Number) ppGetOffsetY.invoke(packet)).doubleValue();
            double dz = ((Number) ppGetOffsetZ.invoke(packet)).doubleValue();
            int count = ((Number) ppGetCount.invoke(packet)).intValue();
            double speed = ((Number) ppGetSpeed.invoke(packet)).doubleValue();
            JsonObject data = extractParticleData(particleOptions);

            String json = ParticlePayloads.toJson(particleName, x, y, z, dx, dy, dz, count, speed, data);
            Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(json));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Particle packet extract failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /**
     * Extract sound data from an entity sound packet via reflection and broadcast.
     */
    private void broadcastEntitySoundPacketReflective(Object packet) {
        if (wsServer == null) return;
        try {
            Object registryEntry = espGetSound.invoke(packet);
            Object soundEvent = registryEntryValue.invoke(registryEntry);
            Object resourceLocation = soundEventGetLocation.invoke(soundEvent);
            String soundName = resourceLocation.toString();

            int entityId = (int) espGetId.invoke(packet);
            float volume = (float) espGetVolume.invoke(packet);
            float pitch = (float) espGetPitch.invoke(packet);
            Object soundSource = espGetSource.invoke(packet);
            String category = (String) soundSourceGetName.invoke(soundSource);

            JsonObject msg = new JsonObject();
            msg.addProperty("type", "entitySound");
            msg.addProperty("name", soundName);
            msg.addProperty("entityId", entityId);
            msg.addProperty("volume", volume);
            msg.addProperty("pitch", pitch);
            msg.addProperty("category", category);
            String json = msg.toString();
            Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(json));
        } catch (Exception e) {
            // Silently ignore
        }
    }

    private String resolveParticleName(Object particleOptions) throws Exception {
        if (particleOptions == null || particleTypeRegistry == null || particleRegistryGetKey == null) return "";
        Object particleType = particleOptionsGetType.invoke(particleOptions);
        if (particleType == null) return "";
        Object resourceLocation = particleRegistryGetKey.invoke(particleTypeRegistry, particleType);
        return resourceLocation != null ? resourceLocation.toString() : "";
    }

    private JsonObject extractParticleData(Object particleOptions) throws Exception {
        JsonObject data = new JsonObject();
        int blockStateId = resolveParticleBlockStateId(particleOptions);
        if (blockStateId >= 0) {
            data.addProperty("stateId", blockStateId);
            data.addProperty("blockStateId", blockStateId);
        }
        return data;
    }

    private int resolveParticleBlockStateId(Object particleOptions) throws Exception {
        if (particleOptions == null || blockParticleOptionClass == null || blockParticleGetState == null || blockGetId == null) return -1;
        if (!blockParticleOptionClass.isInstance(particleOptions)) return -1;
        Object blockState = blockParticleGetState.invoke(particleOptions);
        if (blockState == null) return -1;
        Object stateId = blockGetId.invoke(null, blockState);
        return stateId instanceof Number ? ((Number) stateId).intValue() : -1;
    }

    private String resolveRegistryKey(Object registry, Object value) throws Exception {
        if (registry == null || value == null || registryGetKey == null) return "";
        Object key = registryGetKey.invoke(registry, value);
        return key != null ? key.toString() : "";
    }

    private String componentToPlainString(Object component) throws Exception {
        if (component == null || componentGetString == null) return "";
        Object value = componentGetString.invoke(component);
        return value != null ? value.toString() : "";
    }

    private int resolveContainerSlotCount(String containerType) {
        return switch (containerType) {
            case "minecraft:generic_9x1" -> 9;
            case "minecraft:generic_9x2" -> 18;
            case "minecraft:generic_9x3", "minecraft:shulker_box" -> 27;
            case "minecraft:generic_9x4" -> 36;
            case "minecraft:generic_9x5" -> 45;
            case "minecraft:generic_9x6" -> 54;
            case "minecraft:generic_3x3", "minecraft:crafter_3x3" -> 9;
            case "minecraft:anvil" -> 3;
            case "minecraft:beacon", "minecraft:lectern" -> 1;
            case "minecraft:blast_furnace", "minecraft:furnace", "minecraft:smoker" -> 3;
            case "minecraft:brewing_stand" -> 5;
            case "minecraft:crafting" -> 10;
            case "minecraft:enchantment", "minecraft:stonecutter" -> 2;
            case "minecraft:grindstone", "minecraft:cartography_table", "minecraft:merchant" -> 3;
            case "minecraft:hopper" -> 5;
            case "minecraft:loom", "minecraft:smithing" -> 4;
            default -> -1;
        };
    }

    private JsonArray serializeContainerItems(List<?> items) throws Exception {
        JsonArray serialized = new JsonArray();
        if (items == null) return serialized;
        for (int slot = 0; slot < items.size(); slot++) {
            serialized.add(serializeContainerItem(items.get(slot), slot));
        }
        return serialized;
    }

    private JsonElement serializeContainerItem(Object nmsItemStack, int slot) throws Exception {
        if (nmsItemStack == null || itemStackIsEmpty == null || (Boolean) itemStackIsEmpty.invoke(nmsItemStack)) {
            return JsonNull.INSTANCE;
        }

        JsonObject item = new JsonObject();
        item.addProperty("slot", slot);
        item.addProperty("id", resolveRegistryKey(itemRegistry, itemStackGetItem.invoke(nmsItemStack)));
        item.addProperty("count", ((Number) itemStackGetCount.invoke(nmsItemStack)).intValue());

        JsonObject nbt = new JsonObject();
        boolean damageable = itemStackIsDamageableItem != null && (Boolean) itemStackIsDamageableItem.invoke(nmsItemStack);
        if (damageable) {
            int damage = ((Number) itemStackGetDamageValue.invoke(nmsItemStack)).intValue();
            item.addProperty("damage", damage);
            nbt.addProperty("Damage", damage);
        }

        String displayName = componentToPlainString(itemStackGetDisplayName.invoke(nmsItemStack));
        if (!displayName.isBlank()) {
            item.addProperty("displayName", displayName);
            nbt.addProperty("displayName", displayName);
        }

        JsonArray enchantments = serializeItemEnchantments(nmsItemStack);
        if (enchantments.size() > 0) {
            item.add("enchantments", enchantments);
            nbt.add("Enchantments", enchantments.deepCopy());
        }

        String customData = extractCustomDataSnbt(nmsItemStack);
        if (customData != null && !customData.isBlank() && !"{}".equals(customData)) {
            nbt.addProperty("CustomData", customData);
        }

        item.add("nbt", nbt);
        return item;
    }

    private JsonArray serializeItemEnchantments(Object nmsItemStack) throws Exception {
        JsonArray enchantments = new JsonArray();
        if (itemStackGetEnchantments == null || itemEnchantmentsEntrySet == null) return enchantments;

        Object itemEnchantments = itemStackGetEnchantments.invoke(nmsItemStack);
        if (itemEnchantments == null) return enchantments;

        Object entries = itemEnchantmentsEntrySet.invoke(itemEnchantments);
        if (!(entries instanceof Iterable<?> iterable)) return enchantments;

        for (Object entryObject : iterable) {
            if (!(entryObject instanceof Map.Entry<?, ?> entry)) continue;

            JsonObject enchantment = new JsonObject();
            enchantment.addProperty("id", resolveHolderKey(entry.getKey()));
            Object levelObject = entry.getValue();
            enchantment.addProperty("level", levelObject instanceof Number ? ((Number) levelObject).intValue() : 0);
            enchantments.add(enchantment);
        }

        return enchantments;
    }

    private String resolveHolderKey(Object holder) throws Exception {
        if (holder == null) return "";
        if (holderUnwrapKey != null) {
            Object optionalKey = holderUnwrapKey.invoke(holder);
            if (optionalKey instanceof java.util.Optional<?> optionalKeyValue && optionalKeyValue.isPresent()) {
                Object resourceKey = optionalKeyValue.get();
                Object identifier = resourceKeyIdentifier.invoke(resourceKey);
                if (identifier != null) {
                    return identifier.toString();
                }
            }
        }
        if (registryEntryValue != null) {
            Object value = registryEntryValue.invoke(holder);
            return value != null ? value.toString() : "";
        }
        return "";
    }

    private String extractCustomDataSnbt(Object nmsItemStack) throws Exception {
        if (itemStackGetComponent == null || dataComponentsCustomData == null || customDataIsEmpty == null || customDataCopyTag == null) {
            return null;
        }
        Object customData = itemStackGetComponent.invoke(nmsItemStack, dataComponentsCustomData);
        if (customData == null || (Boolean) customDataIsEmpty.invoke(customData)) {
            return null;
        }
        Object tag = customDataCopyTag.invoke(customData);
        return tag != null ? tag.toString() : null;
    }

    /**
     * Find a declared field by name, searching up the class hierarchy.
     */
    private static Field findField(Class<?> clazz, String name) {
        while (clazz != null) {
            try {
                return clazz.getDeclaredField(name);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        return null;
    }

    private static Field findFieldAny(Class<?> clazz, String... names) {
        for (String name : names) {
            Field field = findField(clazz, name);
            if (field != null) return field;
        }
        return null;
    }

    private static Class<?> loadClass(String... names) throws ClassNotFoundException {
        ClassNotFoundException last = null;
        for (String name : names) {
            try {
                return Class.forName(name);
            } catch (ClassNotFoundException e) {
                last = e;
            }
        }
        throw last != null ? last : new ClassNotFoundException(String.join(", ", names));
    }

    private static Class<?> tryLoadClass(String... names) {
        try {
            return loadClass(names);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    private static Method findMethod(Class<?> clazz, int parameterCount, String... names) {
        for (String name : names) {
            Class<?> current = clazz;
            while (current != null) {
                for (Method method : current.getDeclaredMethods()) {
                    if (method.getName().equals(name) && (parameterCount < 0 || method.getParameterCount() == parameterCount)) {
                        method.setAccessible(true);
                        return method;
                    }
                }
                current = current.getSuperclass();
            }
            for (Method method : clazz.getMethods()) {
                if (method.getName().equals(name) && (parameterCount < 0 || method.getParameterCount() == parameterCount)) {
                    method.setAccessible(true);
                    return method;
                }
            }
        }
        return null;
    }

    private static Method requireMethod(Class<?> clazz, int parameterCount, String... names) throws NoSuchMethodException {
        Method method = findMethod(clazz, parameterCount, names);
        if (method == null) {
            throw new NoSuchMethodException(clazz.getName() + " :: " + String.join("/", names));
        }
        return method;
    }

    /**
     * Return item name as JsonElement or JsonNull for empty/air equipment slots.
     */
    private JsonElement equipItemName(ItemStack item) {
        if (item == null || item.getType() == Material.AIR) return JsonNull.INSTANCE;
        return new com.google.gson.JsonPrimitive(item.getType().getKey().toString());
    }

    // ---- Inventory Broadcasting ----

    /**
     * Send hotbar inventory (slots 0-8) and selected slot.
     * The viewer reads bot.inventory.slots[36..44] for hotbar display.
     */
    private void sendInventory(Player player) {
        if (wsServer == null || player == null || !player.isOnline()) return;

        PlayerInventory inv = player.getInventory();
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "inventory");

        JsonArray slots = new JsonArray();
        for (int i = 0; i < 9; i++) {
            ItemStack item = inv.getItem(i);
            if (item == null || item.getType() == Material.AIR) {
                slots.add(JsonNull.INSTANCE);
            } else {
                slots.add(serializeItem(item, 36 + i));
            }
        }
        msg.add("slots", slots);
        msg.addProperty("selectedSlot", inv.getHeldItemSlot());

        String json = msg.toString();

        // De-duplicate: only send if changed
        String hash = json.hashCode() + "";
        if (hash.equals(lastInventoryHash)) return;
        lastInventoryHash = hash;

        Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(json));
    }

    // ---- Equipment Broadcasting ----

    /**
     * Send full equipment state: main hand, off hand, armor.
     */
    private void sendEquipment(Player player) {
        if (wsServer == null || player == null || !player.isOnline()) return;

        PlayerInventory inv = player.getInventory();
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "equipment");

        msg.add("mainHand", serializeItemSimple(inv.getItemInMainHand()));
        msg.add("offHand", serializeItemSimple(inv.getItemInOffHand()));
        msg.add("helmet", serializeItemSimple(inv.getHelmet()));
        msg.add("chestplate", serializeItemSimple(inv.getChestplate()));
        msg.add("leggings", serializeItemSimple(inv.getLeggings()));
        msg.add("boots", serializeItemSimple(inv.getBoots()));

        String json = msg.toString();

        String hash = json.hashCode() + "";
        if (hash.equals(lastEquipmentHash)) return;
        lastEquipmentHash = hash;

        Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(json));
    }

    // ---- Status Broadcasting ----

    /**
     * Broadcast HP, food, experience periodically (called by scheduled task).
     */
    private void broadcastStatus() {
        if (!running || wsServer == null || anchorPlayer == null || !anchorPlayer.isOnline()) return;

        double health = anchorPlayer.getHealth();
        int food = anchorPlayer.getFoodLevel();
        float exp = anchorPlayer.getExp();
        int level = anchorPlayer.getLevel();

        // Only send if changed
        if (health == lastHealth && food == lastFood && exp == lastExp && level == lastLevel) return;
        lastHealth = health;
        lastFood = food;
        lastExp = exp;
        lastLevel = level;

        broadcastStatusForce(health, food, exp, level);
    }

    private void broadcastStatusForce(double health, int food, float exp, int level) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "status");
        msg.addProperty("health", (float) health);
        msg.addProperty("food", food);
        msg.addProperty("experience", exp);
        msg.addProperty("level", level);

        String json = msg.toString();
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastLive(json));
    }

    // ---- Skin Broadcasting ----

    /**
     * Send player skin texture URL. The viewer uses this to render the player model.
     * Tries PlayerTextures API first, falls back to raw profile properties.
     */
    private void sendSkin(Player player) {
        if (wsServer == null || player == null) return;

        String skinUrl = null;
        try {
            // Method 1: PlayerTextures API (Paper 1.18.2+)
            PlayerTextures textures = player.getPlayerProfile().getTextures();
            URL url = textures.getSkin();
            if (url != null) {
                skinUrl = url.toString();
            }
        } catch (Exception e) {
            // ignore
        }

        // Method 2: Parse raw base64 textures property from the game profile
        if (skinUrl == null) {
            try {
                var profile = player.getPlayerProfile();
                for (var prop : profile.getProperties()) {
                    if ("textures".equals(prop.getName())) {
                        String decoded = new String(java.util.Base64.getDecoder().decode(prop.getValue()));
                        // Parse: {"textures":{"SKIN":{"url":"http://textures.minecraft.net/..."}}}
                        com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
                        JsonObject root = parser.parse(decoded).getAsJsonObject();
                        JsonObject texturesObj = root.getAsJsonObject("textures");
                        if (texturesObj != null && texturesObj.has("SKIN")) {
                            JsonObject skinObj = texturesObj.getAsJsonObject("SKIN");
                            if (skinObj.has("url")) {
                                skinUrl = skinObj.get("url").getAsString();
                            }
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                getLogger().warning("[MindAxisView] Failed to parse skin textures for " + player.getName() + ": " + e.getMessage());
            }
        }

        JsonObject msg = new JsonObject();
        msg.addProperty("type", "skin");
        msg.addProperty("username", player.getName());
        if (skinUrl != null) {
            msg.addProperty("skinUrl", skinUrl);
        }

        String json = msg.toString();
        getLogger().info("[MindAxisView] Skin for " + player.getName() + ": " + (skinUrl != null ? skinUrl : "(none)"));
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastMessage(json));
    }

    // ---- Item Serialization Helpers ----

    /**
     * Serialize an ItemStack for the hotbar display.
     * Returns a JsonObject with slot, name, displayName, count, type.
     */
    private JsonObject serializeItem(ItemStack item, int slot) {
        JsonObject obj = new JsonObject();
        obj.addProperty("slot", slot);
        String name = item.getType().getKey().toString(); // e.g. "minecraft:diamond_sword"
        obj.addProperty("name", name);
        // displayName: strip minecraft: prefix for readability
        String displayName = item.getType().getKey().getKey().replace('_', ' ');
        obj.addProperty("displayName", displayName);
        obj.addProperty("count", item.getAmount());
        // type: numeric ID for the viewer's item rendering
        // Use the Material ordinal as a stable identifier
        obj.addProperty("type", item.getType().ordinal());
        return obj;
    }

    /**
     * Serialize an ItemStack to just its name (for equipment slots).
     * Returns JsonNull if empty/null.
     */
    private JsonElement serializeItemSimple(ItemStack item) {
        if (item == null || item.getType() == Material.AIR) return JsonNull.INSTANCE;
        JsonObject obj = new JsonObject();
        obj.addProperty("name", item.getType().getKey().toString());
        return obj;
    }

    // ---- Action Request Processing (Phase 3: Bukkit API for bot actions) ----

    /**
     * Poll /dev/shm/mindaxis-action-request.json every tick.
     * If a new request is found (different id from last processed), execute it
     * via Bukkit API and write the result to /dev/shm/mindaxis-action-result.json.
     *
     * Runs on the Bukkit main thread so all Bukkit API calls are thread-safe.
     */
    private void processActionRequest() {
        if (!running || anchorPlayer == null || !anchorPlayer.isOnline()) return;

        try {
            if (!Files.exists(ACTION_REQUEST_PATH)) return;

            String content = Files.readString(ACTION_REQUEST_PATH);
            if (content == null || content.isBlank()) return;

            JsonObject request = com.google.gson.JsonParser.parseString(content).getAsJsonObject();
            String id = request.has("id") ? request.get("id").getAsString() : "";

            // Skip if we already processed this request
            if (id.equals(lastProcessedActionId)) return;
            lastProcessedActionId = id;

            String action = request.has("action") ? request.get("action").getAsString() : "";
            JsonObject params = request.has("params") ? request.getAsJsonObject("params") : new JsonObject();

            getLogger().info("[MindAxisAction] Processing action: " + action + " id=" + id);

            // Execute action and capture result
            ActionResult result;
            switch (action) {
                case "dig" -> result = executeDigAction(params);
                case "place" -> result = executePlaceAction(params);
                case "attack" -> result = executeAttackAction(params);
                default -> result = new ActionResult(false, "Unknown action: " + action);
            }

            // Write result
            writeActionResult(id, result);

        } catch (Exception e) {
            getLogger().warning("[MindAxisAction] Error processing action request: " + e.getMessage());
        }
    }

    /**
     * Execute a dig (block break) action via Bukkit API.
     * Uses Player.breakBlock() which respects enchantments, drops items, and fires events.
     */
    private ActionResult executeDigAction(JsonObject params) {
        if (!params.has("x") || !params.has("y") || !params.has("z")) {
            return new ActionResult(false, "Missing x/y/z parameters");
        }

        int x = params.get("x").getAsInt();
        int y = params.get("y").getAsInt();
        int z = params.get("z").getAsInt();

        org.bukkit.World world = anchorPlayer.getWorld();
        org.bukkit.block.Block block = world.getBlockAt(x, y, z);

        if (block.getType() == Material.AIR || block.getType() == Material.CAVE_AIR || block.getType() == Material.VOID_AIR) {
            return new ActionResult(true, null); // Already air, nothing to dig
        }

        try {
            boolean success = anchorPlayer.breakBlock(block);
            if (success) {
                getLogger().info("[MindAxisAction] Broke block at " + x + "," + y + "," + z + " (" + block.getType() + ")");
                return new ActionResult(true, null);
            } else {
                return new ActionResult(false, "breakBlock returned false (event cancelled or unbreakable)");
            }
        } catch (Exception e) {
            return new ActionResult(false, "breakBlock exception: " + e.getMessage());
        }
    }

    /**
     * Execute a place (block place) action via Bukkit API.
     * Sets block type and data at the specified location.
     *
     * Params: x, y, z, itemName (e.g. "minecraft:cobblestone" or "cobblestone")
     * Optional: blockData (full block data string for directional blocks, e.g. "minecraft:oak_stairs[facing=east,half=bottom]")
     */
    private ActionResult executePlaceAction(JsonObject params) {
        if (!params.has("x") || !params.has("y") || !params.has("z")) {
            return new ActionResult(false, "Missing x/y/z parameters");
        }
        if (!params.has("itemName")) {
            return new ActionResult(false, "Missing itemName parameter");
        }

        int x = params.get("x").getAsInt();
        int y = params.get("y").getAsInt();
        int z = params.get("z").getAsInt();
        String itemName = params.get("itemName").getAsString();

        // Normalize item name: add minecraft: prefix if missing
        if (!itemName.contains(":")) {
            itemName = "minecraft:" + itemName;
        }

        // Resolve material from item name
        org.bukkit.NamespacedKey key;
        try {
            key = org.bukkit.NamespacedKey.fromString(itemName);
        } catch (Exception e) {
            return new ActionResult(false, "Invalid item name: " + itemName);
        }
        if (key == null) {
            return new ActionResult(false, "Invalid item name: " + itemName);
        }

        Material material = org.bukkit.Registry.MATERIAL.get(key);
        if (material == null || !material.isBlock()) {
            return new ActionResult(false, "Not a valid block material: " + itemName);
        }

        org.bukkit.World world = anchorPlayer.getWorld();
        org.bukkit.block.Block block = world.getBlockAt(x, y, z);

        try {
            // Use setType with applyPhysics=true for proper block updates
            block.setType(material, true);

            // If blockData string is provided (e.g. for stairs, slabs, etc.), apply it
            if (params.has("blockData")) {
                String bdStr = params.get("blockData").getAsString();
                try {
                    org.bukkit.block.data.BlockData bd = Bukkit.createBlockData(bdStr);
                    block.setBlockData(bd, true);
                } catch (Exception e) {
                    // Block type is set, but extra data failed - non-fatal
                    getLogger().warning("[MindAxisAction] Failed to apply blockData '" + bdStr + "': " + e.getMessage());
                }
            }

            getLogger().info("[MindAxisAction] Placed " + material + " at " + x + "," + y + "," + z);
            return new ActionResult(true, null);
        } catch (Exception e) {
            return new ActionResult(false, "setType exception: " + e.getMessage());
        }
    }

    /**
     * Execute an attack action via Bukkit API.
     * Uses Player.attack(Entity) for melee attacks with proper damage calculation.
     *
     * Params: entityId (Bukkit entity ID integer)
     * Alternative: entityUuid (UUID string) - more reliable across ticks
     */
    private ActionResult executeAttackAction(JsonObject params) {
        Entity target = null;

        // Try UUID first (more reliable across ticks)
        if (params.has("entityUuid")) {
            try {
                UUID uuid = UUID.fromString(params.get("entityUuid").getAsString());
                target = Bukkit.getEntity(uuid);
            } catch (Exception e) {
                // fall through to entityId lookup
            }
        }

        // Fall back to entityId scan
        if (target == null && params.has("entityId")) {
            int entityId = params.get("entityId").getAsInt();
            // Scan nearby entities for matching entityId
            // (Bukkit has no direct getEntity-by-numeric-id, so we scan)
            for (Entity entity : anchorPlayer.getWorld().getEntities()) {
                if (entity.getEntityId() == entityId) {
                    target = entity;
                    break;
                }
            }
        }

        if (target == null) {
            return new ActionResult(false, "Entity not found");
        }

        if (target.equals(anchorPlayer)) {
            return new ActionResult(false, "Cannot attack self");
        }

        try {
            anchorPlayer.attack(target);
            getLogger().info("[MindAxisAction] Attacked entity " + target.getType() + " (id=" + target.getEntityId() + ")");
            return new ActionResult(true, null);
        } catch (Exception e) {
            return new ActionResult(false, "attack exception: " + e.getMessage());
        }
    }

    /**
     * Write action result to /dev/shm/mindaxis-action-result.json atomically.
     */
    private void writeActionResult(String requestId, ActionResult result) {
        JsonObject obj = new JsonObject();
        obj.addProperty("id", requestId);
        obj.addProperty("ts", System.currentTimeMillis());
        obj.addProperty("success", result.success);
        if (result.error != null) {
            obj.addProperty("error", result.error);
        } else {
            obj.add("error", JsonNull.INSTANCE);
        }

        // Atomic write: write to tmp, then rename
        try {
            Files.writeString(ACTION_RESULT_TMP_PATH, obj.toString());
            Files.move(ACTION_RESULT_TMP_PATH, ACTION_RESULT_PATH,
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            getLogger().warning("[MindAxisAction] Failed to write action result: " + e.getMessage());
        }
    }

    /**
     * Simple record for action execution results.
     */
    private record ActionResult(boolean success, String error) {}

    // ---- Getters / Setters ----

    public boolean isRunning() {
        return running;
    }

    public int getWsPort() {
        return wsPort;
    }

    public WSServer getWsServer() {
        return wsServer;
    }

    public Player getAnchorPlayer() {
        return anchorPlayer;
    }

    public void setAnchorPlayer(Player player) {
        // Remove packet interceptors from old anchor before switching
        if (running && anchorPlayer != null && anchorPlayer.isOnline()) {
            removeSoundInterceptor(anchorPlayer);
            removeOverlayInterceptor(anchorPlayer);
            removeBlockEventInterceptor(anchorPlayer);
            removeParticleInterceptor(anchorPlayer);
            removeContainerInterceptor(anchorPlayer);
        }
        this.anchorPlayer = player;
        // Reset change tracking
        lastInventoryHash = "";
        lastEquipmentHash = "";
        lastHealth = -1;
        lastFood = -1;
        lastExp = -1;
        lastLevel = -1;
        // Send chunks + player state around new anchor
        if (running && player != null) {
            sendChunksAroundPlayer(player);
            sendSkin(player);
            sendInventory(player);
            sendEquipment(player);
            broadcastOverlayMessages(syncScoreboardStateFromPlayer(player));
            broadcastOverlayMessages(syncBossBarsFromPlayer(player));
            // Install packet interceptors on new anchor
            installSoundInterceptor(player);
            installOverlayInterceptor(player);
            installBlockEventInterceptor(player);
            installParticleInterceptor(player);
            installContainerInterceptor(player);
        }
    }

    private static long chunkKey(int x, int z) {
        return ((long) x << 32) | (z & 0xFFFFFFFFL);
    }
}
