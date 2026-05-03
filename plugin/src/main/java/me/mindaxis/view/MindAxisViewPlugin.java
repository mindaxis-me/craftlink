package me.mindaxis.view;

import com.destroystokyo.paper.event.entity.EntityRemoveFromWorldEvent;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.channel.ChannelHandler;
import me.mindaxis.view.nms.NmsAdapter;
import net.kyori.adventure.bossbar.BossBar;
import net.kyori.adventure.bossbar.BossBarImplementation;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import org.bukkit.Bukkit;
import org.bukkit.Chunk;
import org.bukkit.ChunkSnapshot;
import org.bukkit.GameMode;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.Chest;
import org.bukkit.block.DoubleChest;
import org.bukkit.block.EnderChest;
import org.bukkit.block.BlockFace;
import org.bukkit.block.ShulkerBox;
import org.bukkit.block.data.BlockData;
import org.bukkit.block.data.Openable;
import org.bukkit.enchantments.Enchantment;
import org.bukkit.entity.Entity;
import org.bukkit.entity.Item;
import org.bukkit.entity.LivingEntity;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.HandlerList;
import org.bukkit.event.Listener;
import org.bukkit.event.block.Action;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockFadeEvent;
import org.bukkit.event.block.BlockFormEvent;
import org.bukkit.event.block.BlockGrowEvent;
import org.bukkit.event.block.BlockPistonExtendEvent;
import org.bukkit.event.block.BlockPistonRetractEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.event.block.BlockRedstoneEvent;
import org.bukkit.event.block.BlockSpreadEvent;
import org.bukkit.event.block.SignChangeEvent;
import org.bukkit.event.block.EntityBlockFormEvent;
import org.bukkit.event.block.FluidLevelChangeEvent;
import org.bukkit.event.block.LeavesDecayEvent;
import org.bukkit.event.entity.EntityDamageEvent;
import org.bukkit.event.entity.EntityDeathEvent;
import org.bukkit.event.entity.EntityChangeBlockEvent;
import org.bukkit.event.entity.EntityPickupItemEvent;
import org.bukkit.event.entity.PlayerDeathEvent;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.event.inventory.InventoryCloseEvent;
import org.bukkit.event.inventory.InventoryOpenEvent;
import org.bukkit.event.player.PlayerAnimationEvent;
import org.bukkit.event.player.PlayerBedEnterEvent;
import org.bukkit.event.player.PlayerBedLeaveEvent;
import org.bukkit.event.player.PlayerDropItemEvent;
import org.bukkit.event.player.PlayerItemHeldEvent;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerPickupItemEvent;
import org.bukkit.event.player.PlayerRespawnEvent;
import org.bukkit.event.world.ChunkLoadEvent;
import org.bukkit.event.world.ChunkUnloadEvent;
import org.bukkit.event.world.TimeSkipEvent;
import org.bukkit.event.weather.ThunderChangeEvent;
import org.bukkit.event.weather.WeatherChangeEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.PlayerInventory;
import org.bukkit.inventory.meta.Damageable;
import org.bukkit.inventory.meta.ItemMeta;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.profile.PlayerTextures;
import org.bukkit.scheduler.BukkitTask;
import org.bukkit.scoreboard.DisplaySlot;
import org.bukkit.scoreboard.Objective;
import org.bukkit.scoreboard.Scoreboard;
import org.java_websocket.WebSocket;

import java.lang.reflect.Constructor;
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
import java.util.function.Consumer;

/**
 * CraftLink viewer transport plugin.
 *
 * Captures ChunkSnapshot on the main thread, then serializes + broadcasts
 * asynchronously to avoid blocking the server tick.
 */
public class MindAxisViewPlugin extends JavaPlugin implements Listener, CraftLinkAPI {

    private WSServer wsServer;
    private NmsAdapter nmsAdapter;
    private boolean running = false;
    private int wsPort;
    private int viewDistance;
    private int positionIntervalMs;
    private int maxHistory;
    private boolean compressBinaryChunks;
    private boolean shmEnabled;
    private int shmRefreshIntervalTicks;
    private String wsAuthToken;
    private List<String> trackedPlayerNames = new ArrayList<>();

    private Player anchorPlayer;
    private volatile Constructor<?> breakTimingBlockPosConstructor;
    private BukkitTask positionTask;
    private BukkitTask statusTask;
    private BukkitTask entityTask;
    private BukkitTask actionTask;
    private BukkitTask shmRefreshTask;
    private BukkitTask blockBreakPollTask;
    private volatile int anchorEntityId = Integer.MIN_VALUE;
    private volatile MirroredPositionState anchorPositionMirror;
    private volatile long lastPositionInterceptBroadcastAt = 0L;
    private volatile String lastServerCorrectionKey = "";
    private volatile long lastServerCorrectionAt = 0L;

    private Path sharedDataRoot;
    private Path sharedChunkMirrorDir;
    private Path sharedHeightmapDir;
    private Path sharedEntitiesPath;
    private Path sharedEntitiesTmpPath;
    private Path sharedEntityEventsPath;
    private Path sharedEntityEventsTmpPath;
    private Path sharedServerCorrectionPath;
    private Path sharedServerCorrectionTmpPath;
    private Path sharedActionRequestPath;
    private Path sharedActionRequestTmpPath;
    private Path sharedActionResultPath;
    private Path sharedActionResultTmpPath;

    // Buffer for entity animation events (flushed every 4 ticks with entity broadcast)
    private final java.util.concurrent.ConcurrentLinkedQueue<int[]> pendingAnimationEvents = new java.util.concurrent.ConcurrentLinkedQueue<>();
    // animation type constants: 0=hurt, 1=death, 2=swingArm

    // NMS packet interceptor handler names (Netty pipeline)
    private static final String POSITION_HANDLER_NAME = "mindaxis-position-interceptor";
    private static final String SOUND_HANDLER_NAME = "mindaxis-sound-interceptor";
    private static final String BLOCK_EVENT_HANDLER_NAME = "mindaxis-block-event-interceptor";
    private static final String PARTICLE_HANDLER_NAME = "mindaxis-particle-interceptor";
    private static final String OVERLAY_HANDLER_NAME = "mindaxis-overlay-interceptor";
    private static final String CONTAINER_HANDLER_NAME = "mindaxis-container-interceptor";
    private static final String BLOCK_BREAK_HANDLER_NAME = "mindaxis-block-break-interceptor";
    private static final String ENTITY_REMOVE_HANDLER_NAME = "mindaxis-entity-remove-interceptor";
    private static final String LEGACY_COMPANION_ACTION_PLUGIN_NAME = "MindAxisAgent";

    private String lastProcessedActionId = "";

    // Chunk retry state: when viewport chunks aren't loaded yet, retry after delay
    private int pendingChunkRetryCount = 0;
    private BukkitTask pendingChunkRetryTask = null;
    private static final int CHUNK_RETRY_MAX = 10;
    private static final long CHUNK_RETRY_DELAY_TICKS = 5L; // 250ms per retry

    // Track the authoritative tracked chunk set owned by the plugin.
    private final Set<Long> trackedChunkKeys = new HashSet<>();
    private String trackedChunkWorldId = "";
    private String trackedChunkViewportSignature = "";

    // Track chunk keys written to the shared-data manifest (thread-safe)
    private final Set<String> shmManifestEntries = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<Long> pendingShmChunkRefreshes = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Map<String, Long> recentBlockActionKeys = new ConcurrentHashMap<>();
    private final Set<String> seenBlockEventPacketChecks = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Track last sent values to avoid redundant messages
    private String lastInventoryHash = "";
    private String lastFullInventoryHash = "";
    private String lastEquipmentHash = "";
    private double lastHealth = -1;
    private int lastFood = -1;
    private float lastExp = -1;
    private int lastLevel = -1;
    private int lastAir = -1;

    // Track last weather state to only send on change
    private boolean lastIsRaining = false;
    private boolean lastIsThundering = false;

    // Entity tracking state
    private static final double ENTITY_RANGE = 96.0;
    private static final double ENTITY_POS_THRESHOLD = 0.1;
    private static final double ENTITY_YAW_THRESHOLD = 5.0;
    private static final int FULL_INVENTORY_SLOT_COUNT = 46;
    private static final long POSITION_POLL_FALLBACK_GRACE_MS = 150L;
    private static final long SERVER_CORRECTION_DUPLICATE_WINDOW_MS = 150L;
    private static final double SERVER_CORRECTION_MIN_DISTANCE = 2.0;
    private final Map<Integer, double[]> lastEntityState = new HashMap<>(); // entityId -> [x, y, z, yaw, pitch]
    private final Map<Integer, String> lastEntitySignature = new HashMap<>();
    private final Set<Integer> previousEntityIds = new HashSet<>();
    private final Object overlayStateLock = new Object();
    private final Map<String, ScoreboardObjectiveState> scoreboardObjectives = new LinkedHashMap<>();
    private final Map<String, Map<String, ScoreboardScoreState>> scoreboardScores = new LinkedHashMap<>();
    private final Map<String, String> scoreboardDisplays = new LinkedHashMap<>();
    private final Map<String, BossBarState> bossBars = new LinkedHashMap<>();
    private static final long BLOCK_ACTION_DUPLICATE_WINDOW_MS = 500L;

    @Override
    public void onEnable() {
        saveDefaultConfig();
        wsPort = getConfig().getInt("ws-port", 4800);
        viewDistance = getConfig().getInt("view-distance", 8);
        positionIntervalMs = getConfig().getInt("position-interval-ms", 50);
        maxHistory = getConfig().getInt("max-history", 2048);
        compressBinaryChunks = getConfig().getBoolean("compress-binary-chunks", true);
        shmEnabled = readCompatBoolean("shared-data-enabled", "shm-enabled", true);
        shmRefreshIntervalTicks = readCompatInt("shared-data-refresh-interval-ticks", "shm-refresh-interval-ticks", 600);
        boolean autoStart = getConfig().getBoolean("auto-start", true);
        wsAuthToken = getConfig().getString("auth-token", "").trim();
        trackedPlayerNames = new ArrayList<>(getConfig().getStringList("tracked-players"));
        if (trackedPlayerNames.isEmpty()) trackedPlayerNames.add("*");
        initializeSharedDataPaths();
        nmsAdapter = NmsAdapter.create(getLogger());

        // Register command
        ViewerCommand cmd = new ViewerCommand(this);
        getCommand("viewer").setExecutor(cmd);
        getCommand("viewer").setTabCompleter(cmd);
        Bukkit.getPluginManager().registerEvents(new ServerListPingListener(this), this);

        // Auto-start if configured anchor player is set
        String defaultAnchor = getConfig().getString("anchor-player", "");
        if (!defaultAnchor.isEmpty()) {
            getLogger().info("Default anchor player configured: " + defaultAnchor);
        }

        // Initialize NMS stateId resolver for fast chunk serialization
        ChunkSerializer.initStateIdResolver(getLogger());
        ChunkSerializer.setStateIdProvider(blockData -> nmsAdapter.getStateId(blockData));

        if (autoStart) {
            getLogger().info("CraftLink enabled with auto-start.");
            startViewer();
        } else {
            getLogger().info("CraftLink enabled. Use /viewer start to begin.");
        }
    }

    @Override
    public void onDisable() {
        if (running) {
            stopViewer();
        }
        ChunkSerializer.clearStateIdProvider();
        nmsAdapter = null;
    }

    private boolean readCompatBoolean(String primaryKey, String legacyKey, boolean defaultValue) {
        if (getConfig().isSet(primaryKey)) {
            return getConfig().getBoolean(primaryKey);
        }
        return getConfig().getBoolean(legacyKey, defaultValue);
    }

    private int readCompatInt(String primaryKey, String legacyKey, int defaultValue) {
        if (getConfig().isSet(primaryKey)) {
            return getConfig().getInt(primaryKey);
        }
        return getConfig().getInt(legacyKey, defaultValue);
    }

    private void initializeSharedDataPaths() {
        String configuredRoot = getConfig().getString("shared-data-root", "").trim();
        if (configuredRoot.isEmpty()) {
            sharedDataRoot = Paths.get("/dev/shm").toAbsolutePath().normalize();
        } else {
            sharedDataRoot = Paths.get(configuredRoot).toAbsolutePath().normalize();
        }

        // Path naming matches bot JS hardcoded paths: mindaxis-chunks, mindaxis-hm, mindaxis-entities.json
        sharedChunkMirrorDir = sharedDataRoot.resolve("mindaxis-chunks");
        sharedHeightmapDir = sharedDataRoot.resolve("mindaxis-hm");
        sharedEntitiesPath = sharedDataRoot.resolve("mindaxis-entities.json");
        sharedEntitiesTmpPath = sharedDataRoot.resolve("mindaxis-entities.json.tmp");
        sharedEntityEventsPath = sharedDataRoot.resolve("mindaxis-entity-events.json");
        sharedEntityEventsTmpPath = sharedDataRoot.resolve("mindaxis-entity-events.json.tmp");
        sharedServerCorrectionPath = sharedDataRoot.resolve("mindaxis-server-correction.json");
        sharedServerCorrectionTmpPath = sharedDataRoot.resolve("mindaxis-server-correction.json.tmp");
        sharedActionRequestPath = sharedDataRoot.resolve("mindaxis-action-request.json");
        sharedActionRequestTmpPath = sharedDataRoot.resolve("mindaxis-action-request.json.tmp");
        sharedActionResultPath = sharedDataRoot.resolve("mindaxis-action-result.json");
        sharedActionResultTmpPath = sharedDataRoot.resolve("mindaxis-action-result.json.tmp");

        try {
            Files.createDirectories(sharedDataRoot);
            Files.createDirectories(sharedChunkMirrorDir);
            Files.createDirectories(sharedHeightmapDir);
        } catch (Exception e) {
            getLogger().warning("Failed to create shared-data directories: " + e.getMessage());
        }

        ChunkSerializer.setSharedDataDir(sharedChunkMirrorDir);
    }

    /**
     * Start the viewer: WS server + event listeners + position broadcast task.
     */
    public void startViewer() {
        if (running) return;
        running = true;
        trackedChunkKeys.clear();
        trackedChunkWorldId = "";
        trackedChunkViewportSignature = "";

        // Start WebSocket server
        wsServer = new WSServer(wsPort, maxHistory, getLogger(), wsAuthToken);
        wsServer.setOnClientConnect((conn) -> {
            Bukkit.getScheduler().runTask(this, () -> {
                if (!running) {
                    wsServer.flushQueuedFrames(conn);
                    return;
                }
                if (!isActiveTrackedPlayer(anchorPlayer)) {
                    wsServer.flushQueuedFrames(conn);
                    return;
                }
                getLogger().info("[MindAxisView] New client — sending direct initial sync from live ChunkSnapshot");
                sendInitialSyncToClient(conn, anchorPlayer);
                lastInventoryHash = "";
                lastFullInventoryHash = "";
                lastEquipmentHash = "";
                lastHealth = -1;
                sendSkinWithRetries(anchorPlayer);
                sendInventoryState(anchorPlayer);
                sendEquipment(anchorPlayer);
                broadcastStatusForce(anchorPlayer.getHealth(), anchorPlayer.getFoodLevel(),
                        anchorPlayer.getExp(), anchorPlayer.getLevel());
            });
        });
        wsServer.start();

        // Register event listeners
        Bukkit.getPluginManager().registerEvents(this, this);

        // Auto-select anchor player if not set
        if (!isActiveTrackedPlayer(anchorPlayer)) {
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
            scheduleTrackedChunksShmRefresh(anchorPlayer, 2L, "viewer-start");
            sendSkinWithRetries(anchorPlayer);
            sendInventoryState(anchorPlayer);
            sendEquipment(anchorPlayer);
            syncPositionMirrorFromPlayer(anchorPlayer, "viewer-start");
            installPositionInterceptor(anchorPlayer);
            // Install NMS packet interceptors on anchor's Netty channel
            installSoundInterceptor(anchorPlayer);
            installOverlayInterceptor(anchorPlayer);
            installBlockEventInterceptor(anchorPlayer);
            installParticleInterceptor(anchorPlayer);
            installContainerInterceptor(anchorPlayer);
            installBlockBreakInterceptor(anchorPlayer);
            installEntityRemoveInterceptor(anchorPlayer);
            broadcastTimeNow(anchorPlayer.getWorld());
            broadcastWeatherNow(anchorPlayer.getWorld());
        }

        // Start position broadcast task (every N ticks, 1 tick = 50ms)
        int ticks = Math.max(1, positionIntervalMs / 50);
        positionTask = Bukkit.getScheduler().runTaskTimer(this, this::broadcastPosition, ticks, ticks);

        // Start status broadcast task (every 20 ticks = 1 sec)
        statusTask = Bukkit.getScheduler().runTaskTimer(this, this::broadcastStatus, 20L, 20L);

        // Start entity broadcast task (every 4 ticks = 200ms)
        entityTask = Bukkit.getScheduler().runTaskTimer(this, this::broadcastEntities, 4L, 4L);

        // Start block break progress polling task (every 2 ticks = 100ms)
        // MC doesn't send ClientboundBlockDestructionPacket to the breaker themselves,
        // so we poll ServerPlayerGameMode directly.
        initBreakPollReflection();
        if (breakPollReflectionAvailable) {
            blockBreakPollTask = Bukkit.getScheduler().runTaskTimer(this, this::pollBlockBreakProgress, 2L, 2L);
        }

        // Start action request polling task (every tick = 50ms, on main thread).
        // Skip file-based polling when a companion plugin owns that channel.
        if (getServer().getPluginManager().getPlugin(LEGACY_COMPANION_ACTION_PLUGIN_NAME) == null) {
            actionTask = Bukkit.getScheduler().runTaskTimer(this, this::processActionRequest, 1L, 1L);
            getLogger().info("No companion action plugin detected — CraftLink will process file-based action requests locally.");
        } else {
            getLogger().info("Companion action plugin detected — file-based action polling remains delegated.");
        }

        if (shmEnabled && shmRefreshIntervalTicks > 0) {
            shmRefreshTask = Bukkit.getScheduler().runTaskTimer(
                    this,
                    () -> refreshTrackedChunksForShm(anchorPlayer, "periodic"),
                    shmRefreshIntervalTicks,
                    shmRefreshIntervalTicks
            );
        }

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
        if (shmRefreshTask != null) {
            shmRefreshTask.cancel();
            shmRefreshTask = null;
        }
        if (blockBreakPollTask != null) {
            blockBreakPollTask.cancel();
            blockBreakPollTask = null;
        }

        // Remove packet interceptors from anchor player's Netty pipeline
        if (isActiveTrackedPlayer(anchorPlayer)) {
            removePositionInterceptor(anchorPlayer);
            removeSoundInterceptor(anchorPlayer);
            removeOverlayInterceptor(anchorPlayer);
            removeBlockEventInterceptor(anchorPlayer);
            removeParticleInterceptor(anchorPlayer);
            removeContainerInterceptor(anchorPlayer);
            removeBlockBreakInterceptor(anchorPlayer);
        }
        anchorEntityId = Integer.MIN_VALUE;
        anchorPositionMirror = null;
        lastPositionInterceptBroadcastAt = 0L;
        lastServerCorrectionKey = "";
        lastServerCorrectionAt = 0L;

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

        trackedChunkKeys.clear();
        trackedChunkWorldId = "";
        trackedChunkViewportSignature = "";
        pendingShmChunkRefreshes.clear();
        recentBlockActionKeys.clear();
        seenBlockEventPacketChecks.clear();
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

    @EventHandler(priority = EventPriority.MONITOR)
    public void onChunkUnload(ChunkUnloadEvent event) {
        if (!running || wsServer == null) return;
        Chunk chunk = event.getChunk();
        // Don't propagate MC-internal unloads for chunks still in view distance.
        // MC may transiently unload/reload chunks due to ticket lifecycle;
        // the viewer should keep showing them.
        if (isTrackedChunk(chunk)) return;
        long key = chunkKey(chunk.getX(), chunk.getZ());
        if (!trackedChunkKeys.remove(key)) return;
        sendChunkUnload(chunk.getX(), chunk.getZ(), "chunk-unload");
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockPlace(BlockPlaceEvent event) {
        if (!running || wsServer == null) return;
        if (event.getBlockPlaced().getBlockData() instanceof org.bukkit.block.data.type.Chest) {
            scheduleBlockUpdate(event.getBlockPlaced());
        } else {
            sendBlockUpdate(event.getBlockPlaced());
        }
        scheduleBlockEntityChunkUpdate(
                event.getBlockPlaced(),
                shouldIncludeNeighborBlockEntityChunks(event.getBlockPlaced().getType()),
                false
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockBreak(BlockBreakEvent event) {
        if (!running || wsServer == null) return;
        BlockData brokenState = event.getBlock().getBlockData();
        if (isTrackedLocation(event.getBlock().getLocation(), ENTITY_RANGE)) {
            broadcastFallbackBlockBreakParticle(event.getBlock(), brokenState);
        }
        Material brokenType = event.getBlock().getType();
        // Send air immediately — don't wait 1 tick.
        // BlockBreakEvent fires before block removal, so we send stateId=0 (air) directly.
        org.bukkit.block.Block block = event.getBlock();
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "blockUpdate");
        msg.addProperty("x", block.getX());
        msg.addProperty("y", block.getY());
        msg.addProperty("z", block.getZ());
        msg.addProperty("name", "minecraft:air");
        msg.addProperty("sid", 0);
        syncShmBlockUpdate(block, 0);
        wsServer.broadcastLive(msg.toString());
        // Deferred: block entity chunk update (needs post-break state)
        Bukkit.getScheduler().runTaskLater(this, () -> {
            sendBlockEntityChunkUpdate(
                    block,
                    shouldIncludeNeighborBlockEntityChunks(brokenType),
                    ChunkSerializer.isTrackedBlockEntityMaterial(brokenType)
            );
        }, 1L);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onSignChange(SignChangeEvent event) {
        if (!running || wsServer == null) return;
        scheduleBlockEntityChunkUpdate(event.getBlock(), false, false);
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
        if (!running || wsServer == null || !isActiveTrackedPlayer(anchorPlayer)) return;
        if (!event.getWorld().equals(anchorPlayer.getWorld())) return;
        lastIsRaining = event.toWeatherState();
        lastIsThundering = event.getWorld().isThundering();
        broadcastWeatherNow(event.getWorld());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onThunderChange(ThunderChangeEvent event) {
        if (!running || wsServer == null || !isActiveTrackedPlayer(anchorPlayer)) return;
        if (!event.getWorld().equals(anchorPlayer.getWorld())) return;
        lastIsRaining = event.getWorld().hasStorm();
        lastIsThundering = event.toThunderState();
        broadcastWeatherNow(event.getWorld());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onTimeSkip(TimeSkipEvent event) {
        if (!running || wsServer == null || !isActiveTrackedPlayer(anchorPlayer)) return;
        if (!event.getWorld().equals(anchorPlayer.getWorld())) return;
        broadcastTimeNow(event.getWorld());
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerJoin(PlayerJoinEvent event) {
        if (!running) return;
        Player joined = event.getPlayer();
        // Auto-select anchor if none set
        if (anchorPlayer == null) {
            anchorPlayer = joined;
            getLogger().info("Auto-selected anchor player: " + anchorPlayer.getName());
            sendChunksAroundPlayer(anchorPlayer);
            scheduleTrackedChunksShmRefresh(anchorPlayer, 2L, "anchor-join");
            syncPositionMirrorFromPlayer(anchorPlayer, "anchor-join");
            installPositionInterceptor(anchorPlayer);
            installSoundInterceptor(anchorPlayer);
            installOverlayInterceptor(anchorPlayer);
            installBlockEventInterceptor(anchorPlayer);
            installParticleInterceptor(anchorPlayer);
            installContainerInterceptor(anchorPlayer);
            installBlockBreakInterceptor(anchorPlayer);
            installEntityRemoveInterceptor(anchorPlayer);
        }
        // Always re-install interceptors when the anchor player (re)joins.
        // Player.equals() and entity IDs change on reconnect, so compare by name.
        if (anchorPlayer != null && joined.getName().equals(anchorPlayer.getName())) {
            // Update reference to the new Player object (Netty channel changed on reconnect)
            anchorPlayer = event.getPlayer();
            // Re-install Netty interceptors on the new channel
            installPositionInterceptor(anchorPlayer);
            installSoundInterceptor(anchorPlayer);
            installOverlayInterceptor(anchorPlayer);
            installBlockEventInterceptor(anchorPlayer);
            installParticleInterceptor(anchorPlayer);
            installContainerInterceptor(anchorPlayer);
            installBlockBreakInterceptor(anchorPlayer);
            installEntityRemoveInterceptor(anchorPlayer);
            // Reset hashes to force re-send
            lastInventoryHash = "";
            lastFullInventoryHash = "";
            lastEquipmentHash = "";
            lastHealth = -1;
            broadcastTimeNow(anchorPlayer.getWorld());
            broadcastWeatherNow(anchorPlayer.getWorld());
            // Delay to allow profile data and inventory to load
            Bukkit.getScheduler().runTaskLater(this, () -> {
                if (!isActiveTrackedPlayer(anchorPlayer)) return;
                sendSkinWithRetries(anchorPlayer);
                sendInventoryState(anchorPlayer);
                sendEquipment(anchorPlayer);
            }, 20L);
            // Second attempt after 2 seconds for late-loading profile data
            Bukkit.getScheduler().runTaskLater(this, () -> {
                if (!isActiveTrackedPlayer(anchorPlayer)) return;
                lastInventoryHash = "";
                lastFullInventoryHash = "";
                lastEquipmentHash = "";
                sendInventoryState(anchorPlayer);
                sendEquipment(anchorPlayer);
            }, 40L);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPlayerItemHeld(PlayerItemHeldEvent event) {
        if (!running || wsServer == null) return;
        if (!event.getPlayer().equals(anchorPlayer)) return;
        // Send inventory with updated selected slot
        Bukkit.getScheduler().runTaskLater(this, () -> sendInventoryState(anchorPlayer), 1L);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onInventoryClick(InventoryClickEvent event) {
        if (!running || wsServer == null) return;
        if (!(event.getWhoClicked() instanceof Player player)) return;
        if (!player.equals(anchorPlayer)) return;
        // Delay 1 tick for inventory state to update
        Bukkit.getScheduler().runTaskLater(this, () -> {
            sendInventoryState(player);
            sendEquipment(player);
        }, 1L);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityPickupItem(EntityPickupItemEvent event) {
        if (!running || wsServer == null) return;
        // entityGone for picked-up items is now handled by packet interceptor
        if (!event.getEntity().equals(anchorPlayer)) return;
        Bukkit.getScheduler().runTaskLater(this, () -> sendInventoryState(anchorPlayer), 1L);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPlayerDropItem(PlayerDropItemEvent event) {
        if (!running || wsServer == null) return;
        if (!event.getPlayer().equals(anchorPlayer)) return;
        Bukkit.getScheduler().runTaskLater(this, () -> sendInventoryState(anchorPlayer), 1L);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onInventoryOpen(InventoryOpenEvent event) {
        if (!running || wsServer == null) return;
        if (!event.getPlayer().equals(anchorPlayer)) return;
        emitInventoryBlockAction(event.getInventory(), true, "inventory-open");
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onInventoryClose(InventoryCloseEvent event) {
        if (!running || wsServer == null) return;
        if (!event.getPlayer().equals(anchorPlayer)) return;
        emitInventoryBlockAction(event.getInventory(), false, "inventory-close");
        // Send containerClose via WS — packet interceptor doesn't work on
        // embedded connections (SP mode), so Bukkit event is the only source.
        String closeJson = ContainerPayloads.containerClose(0);
        nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(closeJson));
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPlayerInteract(PlayerInteractEvent event) {
        if (!running || wsServer == null) return;
        if (event.getAction() != Action.RIGHT_CLICK_BLOCK) return;
        org.bukkit.block.Block block = event.getClickedBlock();
        if (block == null) return;
        if (!(block.getBlockData() instanceof Openable)) return;
        scheduleBlockUpdate(block);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockRedstone(BlockRedstoneEvent event) {
        if (!running || wsServer == null) return;
        if (event.getOldCurrent() == event.getNewCurrent()) return;
        if (!(event.getBlock().getBlockData() instanceof Openable)) return;
        scheduleBlockUpdate(event.getBlock());
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
        // Clear tracked chunks — respawn re-sends all chunks from MC, and without
        // clearing, same-position respawns accumulate duplicate chunks (289→578+).
        for (Long existingKey : new ArrayList<>(trackedChunkKeys)) {
            trackedChunkKeys.remove(existingKey);
            sendChunkUnload(chunkXFromKey(existingKey), chunkZFromKey(existingKey), "respawn-clear");
        }
        trackedChunkViewportSignature = "";
        // Delay to allow respawn to complete, then re-sync everything
        Bukkit.getScheduler().runTaskLater(this, () -> {
            syncPositionMirrorFromPlayer(anchorPlayer, "respawn");
            broadcastStatusForce(anchorPlayer.getHealth(), anchorPlayer.getFoodLevel(),
                    anchorPlayer.getExp(), anchorPlayer.getLevel());
            sendInventoryState(anchorPlayer);
            sendEquipment(anchorPlayer);
            sendChunksAroundPlayer(anchorPlayer);
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
    public void onEntityRemoveFromWorld(EntityRemoveFromWorldEvent event) {
        if (!running || wsServer == null) return;
        Entity entity = event.getEntity();
        if (entity == null || entity.equals(anchorPlayer)) return;
        int entityId = entity.getEntityId();
        lastEntityState.remove(entityId);
        lastEntitySignature.remove(entityId);
        // Send entityGone — this event fires on actual world removal (pickup, despawn,
        // death), NOT on tracking range transitions. The packet interceptor (disabled)
        // was what caused flicker; this Bukkit event is safe.
        JsonObject gone = new JsonObject();
        gone.addProperty("type", "entityGone");
        gone.addProperty("id", entityId);
        String json = gone.toString();
        Bukkit.getScheduler().runTaskAsynchronously(MindAxisViewPlugin.this,
                () -> wsServer.broadcastLive(json));
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerAnimation(PlayerAnimationEvent event) {
        if (!running || wsServer == null) return;
        Location playerLoc = event.getPlayer().getLocation();
        if (!isTrackedLocation(playerLoc, ENTITY_RANGE)) return;
        sendEntityAnimation(event.getPlayer().getEntityId(), "swingArm");
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPlayerBedEnter(PlayerBedEnterEvent event) {
        if (!running || wsServer == null) return;
        Player player = event.getPlayer();
        if (!isTrackedLocation(player.getLocation(), ENTITY_RANGE)) return;
        Bukkit.getScheduler().runTaskLater(this, () -> {
            sendBlockUpdate(event.getBed());
            broadcastTrackedPlayerPosition(player, "bed-enter");
        }, 2L);
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerBedLeave(PlayerBedLeaveEvent event) {
        if (!running || wsServer == null) return;
        Player player = event.getPlayer();
        if (!isTrackedLocation(player.getLocation(), ENTITY_RANGE)) return;
        Bukkit.getScheduler().runTaskLater(this, () -> {
            sendBlockUpdate(event.getBed());
            broadcastTrackedPlayerPosition(player, "bed-leave");
        }, 2L);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockPistonExtend(BlockPistonExtendEvent event) {
        if (!running || wsServer == null) return;
        scheduleBlockUpdate(event.getBlock());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockPistonRetract(BlockPistonRetractEvent event) {
        if (!running || wsServer == null) return;
        scheduleBlockUpdate(event.getBlock(), event.getBlock().getBlockData());
    }

    /**
     * Send an entity animation event via WS broadcastLive and buffer for shared-data mirrors.
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
        nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));

        // Buffer for shared-data write (flushed in broadcastEntities every 4 ticks)
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

    private void scheduleBlockUpdate(org.bukkit.block.Block block, BlockData sourceData) {
        Bukkit.getScheduler().runTaskLater(this, () -> sendBlockUpdate(block, sourceData), 1L);
    }

    private void scheduleBlockEntityChunkUpdate(
            org.bukkit.block.Block block,
            boolean includeNeighborChunks,
            boolean allowEmptyPayload
    ) {
        Bukkit.getScheduler().runTaskLater(
                this,
                () -> sendBlockEntityChunkUpdate(block, includeNeighborChunks, allowEmptyPayload),
                1L
        );
    }

    private void scheduleChunkShmRefresh(Chunk chunk, long delayTicks, String reason) {
        if (!shmEnabled || chunk == null) return;
        long key = chunkKey(chunk.getX(), chunk.getZ());
        if (!pendingShmChunkRefreshes.add(key)) return;
        Bukkit.getScheduler().runTaskLater(this, () -> {
            try {
                if (!running) return;
                if (!chunk.getWorld().isChunkLoaded(chunk.getX(), chunk.getZ())) return;
                refreshChunkShm(chunk, reason);
            } finally {
                pendingShmChunkRefreshes.remove(key);
            }
        }, delayTicks);
    }

    private void scheduleTrackedChunksShmRefresh(Player focusPlayer, long delayTicks, String reason) {
        if (!shmEnabled) return;
        Bukkit.getScheduler().runTaskLater(this, () -> refreshTrackedChunksForShm(focusPlayer, reason), delayTicks);
    }

    private static final int SHM_REFRESH_BATCH_SIZE = 8;

    private void refreshTrackedChunksForShm(Player focusPlayer, String reason) {
        if (!running || !shmEnabled) return;
        if (!isActiveTrackedPlayer(focusPlayer)) return;
        List<Chunk> chunks = collectLoadedChunksAroundTrackedPlayers(focusPlayer);
        if (chunks.isEmpty()) return;
        getLogger().info("[MindAxisView] SHM refresh reason=" + reason + " chunks=" + chunks.size());
        // Batch to avoid main-thread lag from mass ChunkSnapshot calls
        for (int i = 0; i < chunks.size(); i += SHM_REFRESH_BATCH_SIZE) {
            int start = i;
            int end = Math.min(i + SHM_REFRESH_BATCH_SIZE, chunks.size());
            long delayTicks = (long) (i / SHM_REFRESH_BATCH_SIZE);
            Bukkit.getScheduler().runTaskLater(this, () -> {
                if (!running || !shmEnabled) return;
                for (int j = start; j < end; j++) {
                    Chunk chunk = chunks.get(j);
                    if (chunk.getWorld().isChunkLoaded(chunk.getX(), chunk.getZ())) {
                        refreshChunkShm(chunk, reason);
                    }
                }
            }, delayTicks);
        }
    }

    private void refreshChunkShm(Chunk chunk, String reason) {
        if (!shmEnabled || chunk == null) return;
        ChunkSnapshot snapshot = chunk.getChunkSnapshot();
        long mutationSequence = ChunkSerializer.reserveShmMutation(chunk.getX(), chunk.getZ());
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            try {
                writeHeightmapToShm(snapshot);
                ChunkSerializer.serializeToShm(snapshot, shmManifestEntries, mutationSequence);
            } catch (Exception e) {
                getLogger().warning("[MindAxisView] Failed SHM refresh for chunk "
                        + chunk.getX() + "," + chunk.getZ() + " reason=" + reason + ": " + e.getMessage());
            }
        });
    }

    private void sendBlockEntityChunkUpdate(
            org.bukkit.block.Block block,
            boolean includeNeighborChunks,
            boolean allowEmptyPayload
    ) {
        if (wsServer == null || block == null) return;

        Chunk sourceChunk = block.getChunk();
        for (Chunk chunk : collectBlockEntityChunks(block, includeNeighborChunks)) {
            if (!isTrackedChunk(chunk)) continue;
            String json = filterBlockEntitiesPayload(
                    ChunkSerializer.serializeBlockEntities(chunk, true),
                    allowEmptyPayload
                            && chunk.getX() == sourceChunk.getX()
                            && chunk.getZ() == sourceChunk.getZ()
            );
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
        if (!trackedChunkKeys.add(key)) return;

        // Snapshot must be taken on the main thread
        ChunkSnapshot snapshot = chunk.getChunkSnapshot();
        // Full chunk syncs must not replay empty blockEntities payloads, or downstream
        // bridges can incorrectly clear cached block entity state during chunk reloads.
        String blockEntitiesJson = ChunkSerializer.serializeBlockEntities(chunk, false);
        long shmMutationSequence = shmEnabled ? ChunkSerializer.reserveShmMutation(chunk.getX(), chunk.getZ()) : 0L;

        // Serialize and broadcast async to avoid blocking the tick
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            try {
                sendSnapshotBroadcast(snapshot, blockEntitiesJson);
                if (shmEnabled) {
                    writeHeightmapToShm(snapshot);
                }
                if (shmEnabled) {
                    ChunkSerializer.serializeToShm(snapshot, shmManifestEntries, shmMutationSequence);
                }
            } catch (Exception e) {
                trackedChunkKeys.remove(key);
                getLogger().warning("Failed to serialize chunk " + chunk.getX() + "," + chunk.getZ() + ": " + e.getMessage());
            }
        });
    }

    /**
     * Send all loaded chunks around a player.
     */
    private void sendChunksAroundPlayer(Player player) {
        syncTrackedChunks(player, "player-sync");
    }

    private void sendInitialSyncToClient(WebSocket conn, Player player) {
        if (conn == null || !conn.isOpen() || !isActiveTrackedPlayer(player)) return;

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
        String entitySnapshotJson = buildTrackedEntityBatchJson(player, true);
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
                conn.send(entitySnapshotJson);
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

    private String buildTrackedEntityBatchJson(Player player, boolean replace) {
        JsonArray batch = new JsonArray();
        if (player != null) {
            List<Player> trackedPlayers = resolveTrackedPlayers(player);
            if (!trackedPlayers.isEmpty()) {
                World trackedWorld = trackedPlayers.get(0).getWorld();
                for (Entity entity : trackedWorld.getEntities()) {
                    if (!isTrackableEntity(entity)) continue;
                    Location entityLoc = entity.getLocation();
                    if (!isTrackedLocation(entityLoc, ENTITY_RANGE, trackedPlayers)) continue;

                    boolean isLiving = entity instanceof LivingEntity;
                    boolean isItem = entity instanceof Item;
                    float bodyYaw = isLiving ? ((LivingEntity) entity).getBodyYaw() : entityLoc.getYaw();
                    float headYaw = isLiving ? getHeadYaw((LivingEntity) entity) : bodyYaw;
                    batch.add(buildEntityJson(
                            entity,
                            entity.getEntityId(),
                            entityLoc.getX(),
                            entityLoc.getY(),
                            entityLoc.getZ(),
                            bodyYaw,
                            headYaw,
                            entityLoc.getPitch(),
                            isLiving,
                            isItem
                    ));
                }
            }
        }

        JsonObject batchMsg = new JsonObject();
        batchMsg.addProperty("type", "entityBatch");
        if (replace) {
            batchMsg.addProperty("replace", true);
        }
        batchMsg.add("entities", batch);
        return batchMsg.toString();
    }

    private boolean tracksAllPlayers() {
        if (trackedPlayerNames == null || trackedPlayerNames.isEmpty()) return true;
        for (String configured : trackedPlayerNames) {
            if ("*".equals(String.valueOf(configured).trim())) return true;
        }
        return false;
    }

    private boolean isActiveTrackedPlayer(Player player) {
        if (player == null) return false;
        try {
            if (player.isOnline()) return true;
        } catch (Exception ignored) {
        }
        try {
            // SP players are !isOnline() but have a valid world — accept them
            return player.getWorld() != null;
        } catch (Exception ignored) {
            return false;
        }
    }

    private List<Player> resolveTrackedPlayers(Player focusPlayer) {
        LinkedHashMap<UUID, Player> tracked = new LinkedHashMap<>();
        if (isActiveTrackedPlayer(focusPlayer)) {
            tracked.put(focusPlayer.getUniqueId(), focusPlayer);
        }
        if (isActiveTrackedPlayer(anchorPlayer)) {
            tracked.put(anchorPlayer.getUniqueId(), anchorPlayer);
        }
        if (tracksAllPlayers()) {
            for (Player onlinePlayer : Bukkit.getOnlinePlayers()) {
                if (isActiveTrackedPlayer(onlinePlayer)) {
                    tracked.put(onlinePlayer.getUniqueId(), onlinePlayer);
                }
            }
        } else {
            for (String configuredName : trackedPlayerNames) {
                String name = String.valueOf(configuredName).trim();
                if (name.isEmpty() || "*".equals(name)) continue;
                Player matched = Bukkit.getPlayerExact(name);
                if (matched == null) matched = Bukkit.getPlayer(name);
                if (isActiveTrackedPlayer(matched)) {
                    tracked.put(matched.getUniqueId(), matched);
                }
            }
        }

        World trackedWorld = null;
        if (isActiveTrackedPlayer(focusPlayer)) {
            trackedWorld = focusPlayer.getWorld();
        } else if (isActiveTrackedPlayer(anchorPlayer)) {
            trackedWorld = anchorPlayer.getWorld();
        } else {
            for (Player candidate : tracked.values()) {
                if (isActiveTrackedPlayer(candidate)) {
                    trackedWorld = candidate.getWorld();
                    break;
                }
            }
        }

        List<Player> players = new ArrayList<>();
        for (Player candidate : tracked.values()) {
            if (!isActiveTrackedPlayer(candidate)) continue;
            if (trackedWorld != null && candidate.getWorld() != trackedWorld) continue;
            players.add(candidate);
        }
        return players;
    }

    private boolean isTrackedLocation(Location location, double range, List<Player> trackedPlayers) {
        if (location == null || trackedPlayers == null || trackedPlayers.isEmpty()) return false;
        double rangeSquared = range * range;
        for (Player trackedPlayer : trackedPlayers) {
            if (!isActiveTrackedPlayer(trackedPlayer)) continue;
            Location trackedLoc = trackedPlayer.getLocation();
            if (trackedLoc.getWorld() != location.getWorld()) continue;
            if (trackedLoc.distanceSquared(location) <= rangeSquared) return true;
        }
        return false;
    }

    private boolean isTrackedLocation(Location location, double range) {
        return isTrackedLocation(location, range, resolveTrackedPlayers(anchorPlayer));
    }

    private boolean isTrackableEntity(Entity entity) {
        if (entity == null || entity.equals(anchorPlayer)) return false;
        // Invisible/non-renderable entities — skip
        if (entity instanceof org.bukkit.entity.AreaEffectCloud) return false;
        if (entity instanceof org.bukkit.entity.Marker) return false;
        // Track all visible entities (mineflayer equivalent: all destroy_entity targets)
        return true;
    }

    // wasTrackedEntity / scheduleTrackedEntityGoneIfRemoved / broadcastTrackedEntityGone
    // removed — entity removal is now handled by EntityRemoveChannelHandler intercepting
    // ClientboundRemoveEntitiesPacket directly from the MC server packet pipeline.

    private boolean isTrackedChunk(Chunk chunk) {
        if (chunk == null) return false;
        List<Player> trackedPlayers = resolveTrackedPlayers(anchorPlayer);
        if (trackedPlayers.isEmpty()) return false;
        for (Player trackedPlayer : trackedPlayers) {
            if (!isActiveTrackedPlayer(trackedPlayer)) continue;
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
        // Collect center position for distance sorting
        int sortCx = 0, sortCz = 0;
        for (Player trackedPlayer : resolveTrackedPlayers(focusPlayer)) {
            if (!isActiveTrackedPlayer(trackedPlayer)) continue;
            int cx = trackedPlayer.getLocation().getBlockX() >> 4;
            int cz = trackedPlayer.getLocation().getBlockZ() >> 4;
            sortCx = cx; sortCz = cz;
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
        // Sort by distance from anchor (nearest first)
        List<Chunk> result = new ArrayList<>(chunks.values());
        final int fcx = sortCx, fcz = sortCz;
        result.sort((a, b) -> {
            int da = (a.getX() - fcx) * (a.getX() - fcx) + (a.getZ() - fcz) * (a.getZ() - fcz);
            int db = (b.getX() - fcx) * (b.getX() - fcx) + (b.getZ() - fcz) * (b.getZ() - fcz);
            return Integer.compare(da, db);
        });
        return result;
    }

    private void syncTrackedChunksIfViewportChanged(Player focusPlayer, String reason) {
        String signature = buildTrackedChunkViewportSignature(focusPlayer);
        if (signature.equals(trackedChunkViewportSignature)) return;
        trackedChunkViewportSignature = signature;
        syncTrackedChunks(focusPlayer, reason);
    }

    private String buildTrackedChunkViewportSignature(Player focusPlayer) {
        List<Player> trackedPlayers = resolveTrackedPlayers(focusPlayer);
        if (trackedPlayers.isEmpty()) return "";
        List<String> entries = new ArrayList<>(trackedPlayers.size());
        for (Player trackedPlayer : trackedPlayers) {
            if (!isActiveTrackedPlayer(trackedPlayer)) continue;
            Location loc = trackedPlayer.getLocation();
            entries.add(
                    trackedPlayer.getWorld().getUID() + ":"
                            + trackedPlayer.getUniqueId() + ":"
                            + (loc.getBlockX() >> 4) + ":"
                            + (loc.getBlockZ() >> 4)
            );
        }
        Collections.sort(entries);
        return String.join("|", entries);
    }

    private void syncTrackedChunks(Player focusPlayer, String reason) {
        LinkedHashMap<Long, Chunk> nextChunks = new LinkedHashMap<>();
        for (Chunk chunk : collectLoadedChunksAroundTrackedPlayers(focusPlayer)) {
            nextChunks.put(chunkKey(chunk.getX(), chunk.getZ()), chunk);
        }

        // If no tracked players are online, keep existing chunks — don't unload
        if (nextChunks.isEmpty() && !trackedChunkKeys.isEmpty()) {
            return;
        }

        String nextWorldId = "";
        if (!nextChunks.isEmpty()) {
            nextWorldId = nextChunks.values().iterator().next().getWorld().getUID().toString();
        }

        if (!trackedChunkWorldId.equals(nextWorldId)) {
            for (Long existingKey : new ArrayList<>(trackedChunkKeys)) {
                trackedChunkKeys.remove(existingKey);
                sendChunkUnload(chunkXFromKey(existingKey), chunkZFromKey(existingKey), "world-change:" + reason);
            }
        } else {
            for (Long existingKey : new ArrayList<>(trackedChunkKeys)) {
                if (nextChunks.containsKey(existingKey)) continue;
                trackedChunkKeys.remove(existingKey);
                sendChunkUnload(chunkXFromKey(existingKey), chunkZFromKey(existingKey), reason);
            }
        }

        trackedChunkWorldId = nextWorldId;
        int newSent = 0;
        for (Map.Entry<Long, Chunk> entry : nextChunks.entrySet()) {
            if (trackedChunkKeys.contains(entry.getKey())) continue;
            snapshotAndSend(entry.getValue());
            newSent++;
        }

        // Count expected vs collected to detect unloaded chunks
        int expected = (viewDistance * 2 + 1) * (viewDistance * 2 + 1);
        int skipped = expected - nextChunks.size();
        if (skipped > 0 && pendingChunkRetryCount < CHUNK_RETRY_MAX) {
            if (pendingChunkRetryTask != null) pendingChunkRetryTask.cancel();
            pendingChunkRetryCount++;
            final int retryNum = pendingChunkRetryCount;
            pendingChunkRetryTask = Bukkit.getScheduler().runTaskLater(this, () -> {
                pendingChunkRetryTask = null;
                if (!running || !isActiveTrackedPlayer(focusPlayer)) return;
                syncTrackedChunks(focusPlayer, "retry-" + retryNum);
            }, CHUNK_RETRY_DELAY_TICKS);
        } else {
            pendingChunkRetryCount = 0;
        }
    }

    private record ChunkSyncData(ChunkSnapshot snapshot, String blockEntitiesJson) {
    }

    private static final class MirroredPositionState {
        private final double x;
        private final double y;
        private final double z;
        private final float yaw;
        private final float pitch;
        private final boolean sleeping;
        private final String sleepFacing;
        private final long ts;
        private final String source;

        private MirroredPositionState(
                double x,
                double y,
                double z,
                float yaw,
                float pitch,
                boolean sleeping,
                String sleepFacing,
                long ts,
                String source
        ) {
            this.x = x;
            this.y = y;
            this.z = z;
            this.yaw = yaw;
            this.pitch = pitch;
            this.sleeping = sleeping;
            this.sleepFacing = sleepFacing;
            this.ts = ts;
            this.source = source;
        }

        private double x() {
            return x;
        }

        private double y() {
            return y;
        }

        private double z() {
            return z;
        }

        private float yaw() {
            return yaw;
        }

        private float pitch() {
            return pitch;
        }

        private boolean sleeping() {
            return sleeping;
        }

        private String sleepFacing() {
            return sleepFacing;
        }

        private long ts() {
            return ts;
        }

        private String source() {
            return source;
        }
    }

    static String filterBlockEntitiesPayload(String blockEntitiesJson, boolean allowEmptyPayload) {
        if (blockEntitiesJson == null) {
            return null;
        }

        JsonObject msg = JsonParser.parseString(blockEntitiesJson).getAsJsonObject();
        JsonArray entities = msg.getAsJsonArray("entities");
        if (!allowEmptyPayload && (entities == null || entities.isEmpty())) {
            return null;
        }
        return blockEntitiesJson;
    }

    static String buildChunkLoadJson(ChunkSnapshot snapshot) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "chunkLoad");
        JsonArray chunkCoords = new JsonArray();
        chunkCoords.add(snapshot.getX());
        chunkCoords.add(snapshot.getZ());
        msg.add("chunk", chunkCoords);
        return msg.toString();
    }

    static String buildChunkUnloadJson(int chunkX, int chunkZ) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "chunkUnload");
        JsonArray chunkCoords = new JsonArray();
        chunkCoords.add(chunkX);
        chunkCoords.add(chunkZ);
        msg.add("chunk", chunkCoords);
        return msg.toString();
    }

    private void sendChunkUnload(int chunkX, int chunkZ, String reason) {
        if (wsServer == null) return;
        String json = buildChunkUnloadJson(chunkX, chunkZ);
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            if (!running || wsServer == null) return;
            wsServer.broadcastMessage(json);
        });
        getLogger().fine("[MindAxisView] chunkUnload " + chunkX + "," + chunkZ + " reason=" + reason);
    }

    private void sendSnapshotBroadcast(ChunkSnapshot snapshot, String blockEntitiesJson) {
        String snapshotBlockEntitiesJson = filterBlockEntitiesPayload(blockEntitiesJson, false);
        wsServer.broadcastMessage(buildChunkLoadJson(snapshot));
        byte[] fullChunkDump = ChunkSerializer.serializeFullChunkDump(snapshot);
        if (fullChunkDump != null) {
            wsServer.broadcastBinary(fullChunkDump);
        }
        wsServer.broadcastMessage(ChunkSerializer.serializeHeightmap(snapshot));
        if (snapshotBlockEntitiesJson != null) {
            wsServer.broadcastMessage(snapshotBlockEntitiesJson);
        }
    }

    private void sendSnapshotToClient(WebSocket conn, ChunkSnapshot snapshot, String blockEntitiesJson) {
        String snapshotBlockEntitiesJson = filterBlockEntitiesPayload(blockEntitiesJson, false);
        if (conn.isOpen()) {
            conn.send(buildChunkLoadJson(snapshot));
        }
        byte[] fullChunkDump = ChunkSerializer.serializeFullChunkDump(snapshot);
        if (fullChunkDump != null) {
            if (!conn.isOpen()) return;
            conn.send(fullChunkDump);
        }

        if (conn.isOpen()) {
            conn.send(ChunkSerializer.serializeHeightmap(snapshot));
            if (snapshotBlockEntitiesJson != null) {
                conn.send(snapshotBlockEntitiesJson);
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
     * Write chunk heightmaps to {shared-data-root}/heightmaps/{cx}_{cz} as 512 bytes
     * (256 × int16 little-endian).
     * Index order: x + z*16 (x changes fastest, outer loop z).
     */
    private void writeHeightmapToShm(ChunkSnapshot snapshot) {
        if (!shmEnabled) return;
        int cx = snapshot.getX();
        int cz = snapshot.getZ();
        try {
            java.nio.file.Files.createDirectories(sharedHeightmapDir);
            java.nio.file.Path file = sharedHeightmapDir.resolve(cx + "_" + cz);
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(512);
            buf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
            for (int z = 0; z < 16; z++) {
                for (int x = 0; x < 16; x++) {
                    buf.putShort((short) (snapshot.getHighestBlockYAt(x, z) + 1));
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
    private JsonObject buildBlockUpdatePayload(org.bukkit.block.Block block, boolean syncShm) {
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
            if (syncShm) {
                syncShmBlockUpdate(block, sid);
            }
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

        return msg;
    }

    private void sendBlockUpdate(org.bukkit.block.Block block) {
        sendBlockUpdate(block, block == null ? null : block.getBlockData());
    }

    @Override
    public void sendBlockUpdateForBlock(org.bukkit.block.Block block) {
        sendBlockUpdate(block);
    }

    private void sendBlockUpdate(org.bukkit.block.Block block, BlockData sourceData) {
        if (wsServer == null || block == null) return;

        List<org.bukkit.block.Block> targets = collectBlockUpdateTargets(block, sourceData);
        for (org.bukkit.block.Block target : targets) {
            wsServer.broadcastLive(buildBlockUpdatePayload(target, true).toString());
            scheduleChunkShmRefresh(target.getChunk(), 1L, "block-update");
        }
    }

    private List<org.bukkit.block.Block> collectBlockUpdateTargets(org.bukkit.block.Block block, BlockData sourceData) {
        List<org.bukkit.block.Block> targets = new ArrayList<>(2);
        appendUniqueBlockTarget(targets, block);
        appendUniqueBlockTarget(targets, findLinkedBlockUpdateTarget(block, sourceData));
        return targets;
    }

    private org.bukkit.block.Block findLinkedBlockUpdateTarget(org.bukkit.block.Block block, BlockData sourceData) {
        if (block == null || sourceData == null) return null;
        if (sourceData instanceof org.bukkit.block.data.type.Door door) {
            return findLinkedDoorHalf(block, door);
        }
        if (sourceData instanceof org.bukkit.block.data.type.Bed bed) {
            return block.getRelative(
                    bed.getPart() == org.bukkit.block.data.type.Bed.Part.HEAD
                            ? bed.getFacing().getOppositeFace()
                            : bed.getFacing()
            );
        }
        if (sourceData instanceof org.bukkit.block.data.type.Chest chest) {
            return findLinkedChestHalf(block, chest);
        }
        if (sourceData instanceof org.bukkit.block.data.type.PistonHead pistonHead) {
            return block.getRelative(pistonHead.getFacing().getOppositeFace());
        }
        if (sourceData instanceof org.bukkit.block.data.type.Piston piston && piston.isExtended()) {
            return block.getRelative(piston.getFacing());
        }
        if (sourceData instanceof org.bukkit.block.data.Bisected bisected) {
            return block.getRelative(
                    bisected.getHalf() == org.bukkit.block.data.Bisected.Half.TOP ? BlockFace.DOWN : BlockFace.UP
            );
        }
        return null;
    }

    private org.bukkit.block.Block findLinkedChestHalf(
            org.bukkit.block.Block block,
            org.bukkit.block.data.type.Chest chest
    ) {
        org.bukkit.block.data.type.Chest.Type chestType = chest.getType();
        if (chestType == org.bukkit.block.data.type.Chest.Type.SINGLE) return null;

        BlockFace partnerDirection = switch (chest.getFacing()) {
            case NORTH -> chestType == org.bukkit.block.data.type.Chest.Type.LEFT ? BlockFace.EAST : BlockFace.WEST;
            case SOUTH -> chestType == org.bukkit.block.data.type.Chest.Type.LEFT ? BlockFace.WEST : BlockFace.EAST;
            case EAST -> chestType == org.bukkit.block.data.type.Chest.Type.LEFT ? BlockFace.SOUTH : BlockFace.NORTH;
            case WEST -> chestType == org.bukkit.block.data.type.Chest.Type.LEFT ? BlockFace.NORTH : BlockFace.SOUTH;
            default -> null;
        };
        return partnerDirection == null ? null : block.getRelative(partnerDirection);
    }

    private void appendUniqueBlockTarget(List<org.bukkit.block.Block> targets, org.bukkit.block.Block block) {
        if (block == null) return;
        for (org.bukkit.block.Block existing : targets) {
            if (existing.equals(block)) return;
        }
        targets.add(block);
    }

    private String resolveSleepingFacing(Player player) {
        if (player == null || !player.isSleeping()) return null;

        Location loc = player.getLocation();
        if (loc == null || loc.getWorld() == null) return null;

        int baseX = loc.getBlockX();
        int baseY = loc.getBlockY();
        int baseZ = loc.getBlockZ();
        World world = loc.getWorld();
        for (int dy = -1; dy <= 0; dy++) {
            for (int dx = -1; dx <= 1; dx++) {
                for (int dz = -1; dz <= 1; dz++) {
                    org.bukkit.block.Block candidate = world.getBlockAt(baseX + dx, baseY + dy, baseZ + dz);
                    if (candidate.getBlockData() instanceof org.bukkit.block.data.type.Bed bed) {
                        return bed.getFacing().name().toLowerCase(Locale.ROOT);
                    }
                }
            }
        }
        return null;
    }

    private void syncShmBlockUpdate(org.bukkit.block.Block block, int stateId) {
        if (!shmEnabled || block == null || stateId < 0) return;

        Chunk chunk = block.getChunk();
        long mutationSequence = ChunkSerializer.reserveShmMutation(chunk.getX(), chunk.getZ());
        ChunkSerializer.ShmUpdateResult result = ChunkSerializer.updateShmBlock(
                block.getX(),
                block.getY(),
                block.getZ(),
                stateId,
                mutationSequence
        );
        if (result != ChunkSerializer.ShmUpdateResult.REWRITE_REQUIRED) return;

        // Full rewrite is only needed when the section is missing from the sparse SHM file
        // or when an in-place air update would leave an all-air section behind.
        ChunkSnapshot snapshot = chunk.getChunkSnapshot();
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            try {
                ChunkSerializer.serializeToShm(snapshot, shmManifestEntries, mutationSequence);
            } catch (Exception e) {
                getLogger().warning("Failed to rewrite shm chunk " + chunk.getX() + "," + chunk.getZ()
                        + " after block update: " + e.getMessage());
            }
        });
    }

    private JsonObject buildPositionPayload(
            double x,
            double y,
            double z,
            float yaw,
            float pitch,
            boolean sleeping,
            String sleepFacing
    ) {
        return buildPositionPayload(x, y, z, yaw, pitch, yaw, sleeping, sleepFacing);
    }

    private JsonObject buildPositionPayload(
            double x,
            double y,
            double z,
            float yaw,
            float pitch,
            float headYaw,
            boolean sleeping,
            String sleepFacing
    ) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "position");
        msg.addProperty("x", (float) x);
        msg.addProperty("y", (float) y);
        msg.addProperty("z", (float) z);
        msg.addProperty("yaw", yaw);
        msg.addProperty("pitch", pitch);
        msg.addProperty("headYaw", headYaw);
        msg.addProperty("sleeping", sleeping);
        if (sleepFacing != null && !sleepFacing.isBlank()) {
            msg.addProperty("sleepFacing", sleepFacing);
        }
        return msg;
    }

    private String buildPositionJson(Location loc) {
        boolean sleeping = anchorPlayer != null && anchorPlayer.isSleeping();
        String sleepFacing = sleeping ? resolveSleepingFacing(anchorPlayer) : null;
        float headYaw = (anchorPlayer instanceof LivingEntity living) ? getHeadYaw(living) : loc.getYaw();
        return buildPositionPayload(
                loc.getX(),
                loc.getY(),
                loc.getZ(),
                loc.getYaw(),
                loc.getPitch(),
                headYaw,
                sleeping,
                sleepFacing
        ).toString();
    }

    private String buildPositionJson(MirroredPositionState state) {
        return buildPositionPayload(
                state.x(),
                state.y(),
                state.z(),
                state.yaw(),
                state.pitch(),
                state.sleeping(),
                state.sleepFacing()
        ).toString();
    }

    private String buildTeleportJson(
            MirroredPositionState previous,
            MirroredPositionState current,
            String reason,
            String source
    ) {
        JsonObject msg = buildPositionPayload(
                current.x(),
                current.y(),
                current.z(),
                current.yaw(),
                current.pitch(),
                current.sleeping(),
                current.sleepFacing()
        );
        msg.addProperty("type", "teleport");
        msg.addProperty("ts", current.ts());
        msg.addProperty("reason", reason);
        if (source != null && !source.isBlank()) {
            msg.addProperty("source", source);
        }
        if (previous != null) {
            double dx = current.x() - previous.x();
            double dy = current.y() - previous.y();
            double dz = current.z() - previous.z();
            msg.addProperty("distance", (float) Math.sqrt(dx * dx + dy * dy + dz * dz));
            msg.addProperty("previousX", (float) previous.x());
            msg.addProperty("previousY", (float) previous.y());
            msg.addProperty("previousZ", (float) previous.z());
            msg.addProperty("previousYaw", previous.yaw());
            msg.addProperty("previousPitch", previous.pitch());
        }
        return msg.toString();
    }

    private MirroredPositionState syncPositionMirrorFromPlayer(Player player, String source) {
        if (!isActiveTrackedPlayer(player)) {
            anchorEntityId = Integer.MIN_VALUE;
            anchorPositionMirror = null;
            return null;
        }
        Location loc = player.getLocation();
        anchorEntityId = player.getEntityId();
        return updatePositionMirror(
                loc.getX(),
                loc.getY(),
                loc.getZ(),
                loc.getYaw(),
                loc.getPitch(),
                player.isSleeping(),
                resolveSleepingFacing(player),
                System.currentTimeMillis(),
                source
        );
    }

    private void broadcastTrackedPlayerPosition(Player player, String source) {
        if (!running || wsServer == null || !isActiveTrackedPlayer(player)) return;
        MirroredPositionState state = syncPositionMirrorFromPlayer(player, source);
        if (state != null) {
            broadcastMirroredPosition(state, false);
        }
    }

    /**
     * Test hook for synthetic ServerPlayer E2E scenarios where Paper does not emit
     * the normal clientbound packets for sleep state transitions.
     */
    public void broadcastTrackedPlayerPositionForTest(Player player, String source) {
        broadcastTrackedPlayerPosition(player, source);
    }

    private MirroredPositionState updatePositionMirror(
            double x,
            double y,
            double z,
            float yaw,
            float pitch,
            boolean sleeping,
            String sleepFacing,
            long ts,
            String source
    ) {
        MirroredPositionState state = new MirroredPositionState(x, y, z, yaw, pitch, sleeping, sleepFacing, ts, source);
        anchorPositionMirror = state;
        return state;
    }

    private void broadcastMirroredPosition(MirroredPositionState state, boolean intercepted) {
        if (state == null || !running || wsServer == null) return;
        if (intercepted) {
            lastPositionInterceptBroadcastAt = state.ts();
        }
        wsServer.broadcastLive(buildPositionJson(state));
    }

    private void broadcastServerCorrection(
            MirroredPositionState previous,
            MirroredPositionState current,
            String reason,
            String source
    ) {
        if (current == null || !running || wsServer == null) return;

        double distance = Double.POSITIVE_INFINITY;
        if (previous != null) {
            double dx = current.x() - previous.x();
            double dy = current.y() - previous.y();
            double dz = current.z() - previous.z();
            distance = Math.sqrt(dx * dx + dy * dy + dz * dz);
        }

        if (previous == null || distance >= SERVER_CORRECTION_MIN_DISTANCE) {
            long ts = current.ts();
            String correctionKey = reason
                    + "|" + Math.round(current.x() * 1000.0)
                    + "|" + Math.round(current.y() * 1000.0)
                    + "|" + Math.round(current.z() * 1000.0);
            boolean duplicate = correctionKey.equals(lastServerCorrectionKey)
                    && (ts - lastServerCorrectionAt) <= SERVER_CORRECTION_DUPLICATE_WINDOW_MS;
            if (!duplicate) {
                lastServerCorrectionKey = correctionKey;
                lastServerCorrectionAt = ts;
                wsServer.broadcastLive(buildTeleportJson(previous, current, reason, source));
                writeServerCorrectionFlag(current, reason, source);
            }
        }

        broadcastMirroredPosition(current, true);
    }

    private void writeServerCorrectionFlag(MirroredPositionState state, String reason, String source) {
        if (!shmEnabled) return;
        JsonObject root = new JsonObject();
        root.addProperty("ts", state.ts());
        root.addProperty("x", (float) state.x());
        root.addProperty("y", (float) state.y());
        root.addProperty("z", (float) state.z());
        root.addProperty("yaw", state.yaw());
        root.addProperty("pitch", state.pitch());
        root.addProperty("reason", reason);
        if (source != null && !source.isBlank()) {
            root.addProperty("source", source);
        }

        String json = root.toString();
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            try {
                Files.writeString(sharedServerCorrectionTmpPath, json);
                Files.move(sharedServerCorrectionTmpPath, sharedServerCorrectionPath,
                        StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception e) {
                getLogger().warning("[MindAxisView] Failed to write server correction flag: " + e.getMessage());
            }
        });
    }

    private String buildTimeJson(World world) {
        JsonObject timeMsg = new JsonObject();
        long fullTime = world.getFullTime();
        timeMsg.addProperty("type", "time");
        timeMsg.addProperty("timeOfDay", world.getTime());
        timeMsg.addProperty("time", fullTime);
        timeMsg.addProperty("fullTime", fullTime);
        timeMsg.addProperty("age", resolveWorldAge(world));
        return timeMsg.toString();
    }

    private long resolveWorldAge(World world) {
        if (world == null) return 0L;
        try {
            Object value = world.getClass().getMethod("getGameTime").invoke(world);
            if (value instanceof Number number) {
                return number.longValue();
            }
        } catch (Exception ignored) {
        }
        return world.getFullTime();
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
            if (!isActiveTrackedPlayer(trackedPlayer)) continue;
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
        if (isActiveTrackedPlayer(anchorPlayer)) {
            msg.addProperty("air", anchorPlayer.getRemainingAir());
            msg.addProperty("maxAir", anchorPlayer.getMaximumAir());
        }
        return msg.toString();
    }

    private void broadcastTimeNow(World world) {
        if (!running || wsServer == null || world == null) return;
        String timeJson = buildTimeJson(world);
        nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(timeJson));
    }

    private void broadcastWeatherNow(World world) {
        if (!running || wsServer == null || world == null) return;
        String weatherJson = buildWeatherJson(world);
        nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(weatherJson));
    }

    // ---- Position Broadcasting ----

    /**
     * Broadcast anchor player position (called by scheduled task).
     */
    private void broadcastPosition() {
        if (!running || wsServer == null || !isActiveTrackedPlayer(anchorPlayer)) return;

        // Re-resolve player reference each tick to avoid stale objects after reload
        Player fresh = null;
        if (anchorPlayer.isOnline()) {
            fresh = Bukkit.getPlayer(anchorPlayer.getUniqueId());
        }
        if (!isActiveTrackedPlayer(fresh) && isActiveTrackedPlayer(anchorPlayer)) {
            fresh = anchorPlayer;
        }
        if (!isActiveTrackedPlayer(fresh)) return;
        anchorPlayer = fresh;
        anchorEntityId = fresh.getEntityId();
        syncTrackedChunksIfViewportChanged(fresh, "position-tick");

        String positionJson = null;
        long now = System.currentTimeMillis();
        if (anchorPositionMirror == null || (now - lastPositionInterceptBroadcastAt) > POSITION_POLL_FALLBACK_GRACE_MS) {
            MirroredPositionState polledState = syncPositionMirrorFromPlayer(fresh, "poll");
            if (polledState != null) {
                positionJson = buildPositionJson(polledState);
            }
        }

        // Also send time update with each position tick
        String timeJson = buildTimeJson(fresh.getWorld());

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
        final String positionJsonFinal = positionJson;
        final String weatherJsonFinal = weatherJson;
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            if (positionJsonFinal != null) {
                wsServer.broadcastLive(positionJsonFinal);
            }
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
        if (!running || wsServer == null || !isActiveTrackedPlayer(anchorPlayer)) return;

        List<Player> trackedPlayers = resolveTrackedPlayers(anchorPlayer);
        if (trackedPlayers.isEmpty()) return;
        World trackedWorld = trackedPlayers.get(0).getWorld();

        JsonArray batch = new JsonArray();
        // Full entity list for shared-data mirrors (all current entities, not just changed)
        JsonArray shmEntities = new JsonArray();
        Set<Integer> currentEntityIds = new HashSet<>();

        for (Entity entity : trackedWorld.getEntities()) {
            if (!isTrackableEntity(entity)) continue;
            boolean isLiving = entity instanceof LivingEntity;
            boolean isItem = entity instanceof Item;

            // Range check
            Location entityLoc = entity.getLocation();
            if (!isTrackedLocation(entityLoc, ENTITY_RANGE, trackedPlayers)) continue;

            int entityId = entity.getEntityId();
            currentEntityIds.add(entityId);

            double ex = entityLoc.getX();
            double ey = entityLoc.getY();
            double ez = entityLoc.getZ();
            float bodyYaw = isLiving ? ((LivingEntity) entity).getBodyYaw() : entityLoc.getYaw();
            float headYaw = isLiving ? getHeadYaw((LivingEntity) entity) : bodyYaw;
            float epitch = entityLoc.getPitch();

            // Build entity JSON for shm (always includes ALL entities)
            JsonObject shmObj = buildEntityJson(entity, entityId, ex, ey, ez, bodyYaw, headYaw, epitch, isLiving, isItem);
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
                double bodyYawDiff = Math.abs(bodyYaw - lastState[3]);
                if (bodyYawDiff > 180) bodyYawDiff = 360 - bodyYawDiff;
                double headYawDiff = Math.abs(headYaw - lastState[4]);
                if (headYawDiff > 180) headYawDiff = 360 - headYawDiff;
                positionChanged = dist >= ENTITY_POS_THRESHOLD ||
                    bodyYawDiff >= ENTITY_YAW_THRESHOLD ||
                    headYawDiff >= ENTITY_YAW_THRESHOLD;
            }
            boolean stateChanged = !signature.equals(lastSignature);
            if (!positionChanged && !stateChanged) continue;

            // Update last known state
            lastEntityState.put(entityId, new double[]{ex, ey, ez, bodyYaw, headYaw, epitch});
            lastEntitySignature.put(entityId, signature);

            // Re-use the same JSON object for the WS batch
            batch.add(shmObj);
        }

        // Detect despawned entities — send entityGone via WS.
        // Packet interceptor (EntityRemoveChannelHandler) doesn't work for SP mode
        // because FakeConnection's send() is no-op and packets never reach Netty pipeline.
        JsonArray shmGone = new JsonArray();
        for (int prevId : previousEntityIds) {
            if (!currentEntityIds.contains(prevId)) {
                shmGone.add(prevId);
                lastEntityState.remove(prevId);
                lastEntitySignature.remove(prevId);
                // Send entityGone via WS
                JsonObject gone = new JsonObject();
                gone.addProperty("type", "entityGone");
                gone.addProperty("id", prevId);
                String json = gone.toString();
                Bukkit.getScheduler().runTaskAsynchronously(MindAxisViewPlugin.this,
                        () -> wsServer.broadcastLive(json));
            }
        }
        previousEntityIds.clear();
        previousEntityIds.addAll(currentEntityIds);

        // Write full entity snapshot for same-host consumers
        writeShmEntities(shmEntities, shmGone);

        // Flush buffered entity animation events to shared-data mirrors
        flushEntityAnimationEvents();

        // Send batched update if non-empty
        if (batch.size() > 0) {
            JsonObject batchMsg = new JsonObject();
            batchMsg.addProperty("type", "entityBatch");
            batchMsg.add("entities", batch);
            String json = batchMsg.toString();
            nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
        }
    }

    /**
     * Get head yaw via NMS reflection. Falls back to body yaw if reflection fails.
     */
    private float getHeadYaw(LivingEntity entity) {
        try {
            Object handle = entity.getClass().getMethod("getHandle").invoke(entity);
            // NMS LivingEntity.yHeadRot field
            java.lang.reflect.Field f = null;
            Class<?> cls = handle.getClass();
            while (cls != null && cls != Object.class) {
                try { f = cls.getDeclaredField("yHeadRot"); break; } catch (NoSuchFieldException ignored) {}
                // Mojang mapping alternative
                try { f = cls.getDeclaredField("headRotation"); break; } catch (NoSuchFieldException ignored) {}
                cls = cls.getSuperclass();
            }
            if (f != null) {
                f.setAccessible(true);
                return f.getFloat(handle);
            }
            getLogger().warning("[headYaw] yHeadRot field not found for " + handle.getClass().getName()
                    + " hierarchy: " + getClassHierarchy(handle));
        } catch (Exception e) {
            getLogger().warning("[headYaw] reflection failed: " + e.getMessage());
        }
        return entity.getLocation().getYaw(); // fallback to body yaw
    }

    private String getClassHierarchy(Object obj) {
        StringBuilder sb = new StringBuilder();
        Class<?> cls = obj.getClass();
        while (cls != null && cls != Object.class) {
            if (sb.length() > 0) sb.append(" -> ");
            sb.append(cls.getSimpleName());
            cls = cls.getSuperclass();
        }
        return sb.toString();
    }

    /**
     * Build entity JSON object shared by WS broadcast and shared-data mirrors.
     */
    private JsonObject buildEntityJson(Entity entity, int entityId,
            double ex, double ey, double ez, float bodyYaw, float headYaw, float epitch,
            boolean isLiving, boolean isItem) {
        JsonObject obj = new JsonObject();
        obj.addProperty("type", "entity");
        obj.addProperty("id", entityId);
        obj.addProperty("uuid", entity.getUniqueId().toString());

        // Entity type name (lowercase)
        String typeName = typeName(entity);
        obj.addProperty("name", typeName);

        // Determine entity category
        String category = categoryName(entity, isItem);
        obj.addProperty("entityType", category);

        // For players, include username
        if (entity instanceof Player otherPlayer) {
            obj.addProperty("username", otherPlayer.getName());
            String skinUrl = resolvePlayerSkinUrl(otherPlayer, false);
            if (skinUrl != null) {
                obj.addProperty("skinUrl", skinUrl);
            }
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

        obj.addProperty("yaw", bodyYaw);
        obj.addProperty("pitch", epitch);
        if (isLiving) {
            obj.addProperty("headYaw", headYaw);
        }

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
        boolean isSneaking = entity instanceof Player p && p.isSneaking();
        boolean isSprinting = entity instanceof Player p2 && p2.isSprinting();
        boolean isSwimming = entity instanceof LivingEntity le && le.isSwimming();
        boolean isGliding = entity instanceof LivingEntity le2 && le2.isGliding();
        if (entity.getFireTicks() > 0) flags |= 0x01;
        if (isSneaking) flags |= 0x02;
        if (isSprinting) flags |= 0x08;
        if (isSwimming) flags |= 0x10;
        if (entity.isInvisible()) flags |= 0x20;
        if (entity.isGlowing()) flags |= 0x40;
        if (isGliding) flags |= (byte) 0x80;
        metadata.add(flags);
        obj.add("metadata", metadata);
        obj.addProperty("isSneaking", isSneaking);
        obj.addProperty("isSprinting", isSprinting);
        obj.addProperty("isSwimming", isSwimming);
        obj.addProperty("isGliding", isGliding);

        // Equipment (for LivingEntity)
        if (isLiving) {
            org.bukkit.inventory.EntityEquipment eq = ((LivingEntity) entity).getEquipment();
            JsonArray equipment = new JsonArray();
            if (eq != null) {
                equipment.add(serializeTrackedEquipmentItem(eq.getItemInMainHand(), 0));
                equipment.add(serializeTrackedEquipmentItem(eq.getItemInOffHand(), 1));
                equipment.add(serializeTrackedEquipmentItem(eq.getBoots(), 2));
                equipment.add(serializeTrackedEquipmentItem(eq.getLeggings(), 3));
                equipment.add(serializeTrackedEquipmentItem(eq.getChestplate(), 4));
                equipment.add(serializeTrackedEquipmentItem(eq.getHelmet(), 5));
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
            obj.addProperty("sleeping", living.isSleeping());
            obj.addProperty("airSupply", living.getRemainingAir());
            obj.addProperty("maxAirSupply", living.getMaximumAir());
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
        if (obj.has("airSupply")) {
            signature.append('|').append(obj.get("airSupply").getAsInt());
        }
        if (obj.has("isUsingItem")) {
            signature.append('|').append(obj.get("isUsingItem").getAsBoolean());
        }
        if (obj.has("sleeping")) {
            signature.append('|').append(obj.get("sleeping").getAsBoolean());
        }

        org.bukkit.util.Vector vel = entity.getVelocity();
        signature.append('|').append(Math.round(vel.getX() * 100.0) / 100.0);
        signature.append('|').append(Math.round(vel.getY() * 100.0) / 100.0);
        signature.append('|').append(Math.round(vel.getZ() * 100.0) / 100.0);
        return signature.toString();
    }

    /**
     * Write entity snapshots to {shared-data-root}/entities.json.
     * Atomic write: write to .tmp then rename.
     */
    private void writeShmEntities(JsonArray entities, JsonArray gone) {
        if (!shmEnabled) return;
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            try {
                JsonObject root = new JsonObject();
                root.addProperty("ts", System.currentTimeMillis());
                root.add("entities", entities);
                root.add("gone", gone);

                java.nio.file.Files.writeString(sharedEntitiesTmpPath, root.toString());
                java.nio.file.Files.move(sharedEntitiesTmpPath, sharedEntitiesPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception e) {
                // silently ignore — not critical, same-host consumers will retry next poll
            }
        });
    }

    /**
     * Flush buffered entity animation events to {shared-data-root}/entity-events.json.
     * Called from broadcastEntities (every 4 ticks). If no events are pending, does nothing.
     */
    private void flushEntityAnimationEvents() {
        if (!shmEnabled) {
            pendingAnimationEvents.clear();
            return;
        }
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
                Files.writeString(sharedEntityEventsTmpPath, json);
                Files.move(sharedEntityEventsTmpPath, sharedEntityEventsPath,
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

    // Title / Subtitle / Actionbar / TabList / Team
    private static Class<?> titlePacketClass;
    private static Class<?> subtitlePacketClass;
    private static Class<?> titleTimesPacketClass;
    private static Class<?> clearTitlePacketClass;
    private static Class<?> actionbarPacketClass;
    private static Class<?> tabListPacketClass;
    private static Class<?> teamPacketClass;

    private static Method titleGetText;
    private static Method subtitleGetText;
    private static Method titleTimesGetFadeIn;
    private static Method titleTimesGetStay;
    private static Method titleTimesGetFadeOut;
    private static Method actionbarGetText;
    private static Method tabListGetHeader;
    private static Method tabListGetFooter;

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
            Class<?> bossBattleClass = tryLoadClass("net.minecraft.world.BossEvent", "net.minecraft.world.BossBattle");
            if (bossBattleClass != null) {
                bossBattleGetId = requireMethod(bossBattleClass, 0, "getId", "i");
            }

            // Title / Subtitle / Actionbar
            titlePacketClass = tryLoadClass(
                    "net.minecraft.network.protocol.game.ClientboundSetTitleTextPacket",
                    "net.minecraft.network.packet.s2c.play.TitleS2CPacket"
            );
            if (titlePacketClass != null) {
                titleGetText = requireMethod(titlePacketClass, 0, "text", "getText", "b");
            }
            subtitlePacketClass = tryLoadClass(
                    "net.minecraft.network.protocol.game.ClientboundSetSubtitleTextPacket",
                    "net.minecraft.network.packet.s2c.play.SubtitleS2CPacket"
            );
            if (subtitlePacketClass != null) {
                subtitleGetText = requireMethod(subtitlePacketClass, 0, "text", "getText", "b");
            }
            titleTimesPacketClass = tryLoadClass(
                    "net.minecraft.network.protocol.game.ClientboundSetTitlesAnimationPacket",
                    "net.minecraft.network.packet.s2c.play.TitleFadeS2CPacket"
            );
            if (titleTimesPacketClass != null) {
                titleTimesGetFadeIn = requireMethod(titleTimesPacketClass, 0, "getFadeIn", "b");
                titleTimesGetStay = requireMethod(titleTimesPacketClass, 0, "getStay", "e");
                titleTimesGetFadeOut = requireMethod(titleTimesPacketClass, 0, "getFadeOut", "f");
            }
            clearTitlePacketClass = tryLoadClass(
                    "net.minecraft.network.protocol.game.ClientboundClearTitlesPacket",
                    "net.minecraft.network.packet.s2c.play.ClearTitleS2CPacket"
            );
            actionbarPacketClass = tryLoadClass(
                    "net.minecraft.network.protocol.game.ClientboundSetActionBarTextPacket",
                    "net.minecraft.network.packet.s2c.play.OverlayMessageS2CPacket"
            );
            if (actionbarPacketClass != null) {
                actionbarGetText = requireMethod(actionbarPacketClass, 0, "text", "getText", "b");
            }
            // TabList header/footer
            tabListPacketClass = tryLoadClass(
                    "net.minecraft.network.protocol.game.ClientboundTabListPacket",
                    "net.minecraft.network.packet.s2c.play.PlayerListHeaderS2CPacket"
            );
            if (tabListPacketClass != null) {
                tabListGetHeader = requireMethod(tabListPacketClass, 0, "header", "getHeader", "b");
                tabListGetFooter = requireMethod(tabListPacketClass, 0, "footer", "getFooter", "e");
            }
            // Team packet (for nametag colors)
            teamPacketClass = tryLoadClass(
                    "net.minecraft.network.protocol.game.ClientboundSetPlayerTeamPacket",
                    "net.minecraft.network.packet.s2c.play.TeamS2CPacket"
            );
            getLogger().info("[MindAxisView] Title/Subtitle/Actionbar/TabList packet classes: "
                    + (titlePacketClass != null ? "title " : "")
                    + (subtitlePacketClass != null ? "subtitle " : "")
                    + (actionbarPacketClass != null ? "actionbar " : "")
                    + (tabListPacketClass != null ? "tablist " : "")
                    + (teamPacketClass != null ? "team " : ""));

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
            OverlayChannelHandler handler = new OverlayChannelHandler(
                    this,
                    scoreboardObjectivePacketClass,
                    scoreboardScorePacketClass,
                    scoreboardResetScorePacketClass,
                    scoreboardDisplayPacketClass,
                    bossEventPacketClass,
                    titlePacketClass,
                    subtitlePacketClass,
                    titleTimesPacketClass,
                    clearTitlePacketClass,
                    actionbarPacketClass,
                    tabListPacketClass,
                    teamPacketClass
            );
            installPlayerChannelHandler(player, OVERLAY_HANDLER_NAME, handler);

            getLogger().info("[MindAxisView] Overlay interceptor installed for " + player.getName());
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Failed to install overlay interceptor: " + e.getMessage());
        }
    }

    private void removeOverlayInterceptor(Player player) {
        if (!overlayReflectionAvailable) return;
        try {
            if (removePlayerChannelHandler(player, OVERLAY_HANDLER_NAME)) {
                getLogger().info("[MindAxisView] Overlay interceptor removed");
            }
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
            ContainerChannelHandler handler = new ContainerChannelHandler(
                    this,
                    openScreenPacketClass,
                    containerClosePacketClass,
                    containerContentPacketClass,
                    containerSlotPacketClass
            );
            installPlayerChannelHandler(player, CONTAINER_HANDLER_NAME, handler);

            getLogger().info("[MindAxisView] Container interceptor installed for " + player.getName());
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Failed to install container interceptor: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void removeContainerInterceptor(Player player) {
        if (!containerReflectionAvailable) return;
        try {
            if (removePlayerChannelHandler(player, CONTAINER_HANDLER_NAME)) {
                getLogger().info("[MindAxisView] Container interceptor removed");
            }
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

    // ── Title / Subtitle / Actionbar / TabList / Team packet handlers ──

    void onTitlePacketIntercepted(Object packet) {
        if (wsServer == null || titleGetText == null) return;
        try {
            String text = extractComponentJson(titleGetText.invoke(packet));
            broadcastOverlayMessage(OverlayPayloads.title(text));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Title extract failed: " + e.getMessage());
        }
    }

    void onSubtitlePacketIntercepted(Object packet) {
        if (wsServer == null || subtitleGetText == null) return;
        try {
            String text = extractComponentJson(subtitleGetText.invoke(packet));
            broadcastOverlayMessage(OverlayPayloads.subtitle(text));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Subtitle extract failed: " + e.getMessage());
        }
    }

    void onTitleTimesPacketIntercepted(Object packet) {
        if (wsServer == null || titleTimesGetFadeIn == null) return;
        try {
            int fadeIn = numberValue(titleTimesGetFadeIn.invoke(packet)).intValue();
            int stay = numberValue(titleTimesGetStay.invoke(packet)).intValue();
            int fadeOut = numberValue(titleTimesGetFadeOut.invoke(packet)).intValue();
            broadcastOverlayMessage(OverlayPayloads.titleTimes(fadeIn, stay, fadeOut));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] TitleTimes extract failed: " + e.getMessage());
        }
    }

    void onClearTitlePacketIntercepted(Object packet) {
        if (wsServer == null) return;
        broadcastOverlayMessage(OverlayPayloads.clearTitle());
    }

    void onActionbarPacketIntercepted(Object packet) {
        if (wsServer == null || actionbarGetText == null) return;
        try {
            String text = extractComponentJson(actionbarGetText.invoke(packet));
            broadcastOverlayMessage(OverlayPayloads.actionbar(text));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Actionbar extract failed: " + e.getMessage());
        }
    }

    void onTabListPacketIntercepted(Object packet) {
        if (wsServer == null || tabListGetHeader == null) return;
        try {
            String header = extractComponentJson(tabListGetHeader.invoke(packet));
            String footer = extractComponentJson(tabListGetFooter.invoke(packet));
            broadcastOverlayMessage(OverlayPayloads.tablist(header, footer));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] TabList extract failed: " + e.getMessage());
        }
    }

    void onTeamPacketIntercepted(Object packet) {
        // Team packet handling — forward raw for viewer nametag coloring
        // TODO: extract team name, color, members for entity nametag rendering
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

    private static final java.util.concurrent.ExecutorService nettyDispatchExecutor =
            java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "MindAxisView-NettyDispatch");
                t.setDaemon(true);
                return t;
            });

    private void broadcastOverlayMessage(String json) {
        if (json == null || wsServer == null) return;
        nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
    }

    private void broadcastOverlayMessages(List<String> messages) {
        if (messages == null || messages.isEmpty() || wsServer == null) return;
        nettyDispatchExecutor.execute(() -> {
            for (String json : messages) wsServer.broadcastLive(json);
        });
    }

    private String resolveBossBarId(BossBar bossBar) {
        if (nmsAdapter != null) {
            try {
                UUID id = nmsAdapter.getBossBarId(bossBar);
                if (id != null) {
                    return id.toString();
                }
            } catch (Exception ignored) {
                // Fall back to legacy inline reflection below.
            }
        }
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

    // Payload utilities -- delegated to PayloadUtil

    private static String extractOptionalPlainText(Object value) {
        return PayloadUtil.extractOptionalPlainText(value);
    }

    private static String extractPlainText(Object value) {
        return PayloadUtil.extractPlainText(value);
    }

    private static String extractComponentJson(Object value) {
        return PayloadUtil.extractComponentJson(value);
    }

    private static Number numberValue(Object value) {
        return PayloadUtil.numberValue(value);
    }

    private static boolean booleanValue(Object value) {
        return PayloadUtil.booleanValue(value);
    }

    private static String stringValue(Object value) {
        return PayloadUtil.stringValue(value);
    }

    private static String blankToNull(String value) {
        return PayloadUtil.blankToNull(value);
    }

    private static String normalizeDisplaySlot(Object value) {
        return PayloadUtil.normalizeDisplaySlot(value);
    }

    private static String normalizeNamedValue(Object value) {
        return PayloadUtil.normalizeNamedValue(value);
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

    // ---- Position Packet Interceptor (reflection-based) ----

    private static volatile boolean positionReflectionInitialized = false;
    private static volatile boolean positionReflectionAvailable = false;
    private static Class<?> serverboundMovePlayerPacketClass;
    private static Class<?> clientboundMoveEntityPacketClass;
    private static Class<?> clientboundTeleportEntityPacketClass;
    private static Class<?> clientboundPlayerPositionPacketClass;
    private static Class<?> clientboundEntityPositionSyncPacketClass;
    private static Class<?> positionMoveRotationClass;
    private static Class<?> vec3Class;
    private static Class<?> vecDeltaCodecClass;
    private static Method movePlayerGetX;
    private static Method movePlayerGetY;
    private static Method movePlayerGetZ;
    private static Method movePlayerGetYRot;
    private static Method movePlayerGetXRot;
    private static Method moveEntityGetXa;
    private static Method moveEntityGetYa;
    private static Method moveEntityGetZa;
    private static Method moveEntityGetYRot;
    private static Method moveEntityGetXRot;
    private static Method moveEntityHasPosition;
    private static Method moveEntityHasRotation;
    private static Field moveEntityIdField;
    private static Method teleportEntityGetId;
    private static Method teleportEntityGetChange;
    private static Method teleportEntityGetRelatives;
    private static Method playerPositionGetChange;
    private static Method playerPositionGetRelatives;
    private static Method entityPositionSyncGetId;
    private static Method entityPositionSyncGetValues;
    private static Method positionMoveRotationGetPosition;
    private static Method positionMoveRotationGetYRot;
    private static Method positionMoveRotationGetXRot;
    private static Method vec3GetX;
    private static Method vec3GetY;
    private static Method vec3GetZ;
    private static Method vecDeltaCodecSetBase;
    private static Method vecDeltaCodecDecode;
    private static Constructor<?> vec3Ctor;
    private static Constructor<?> vecDeltaCodecCtor;

    private void initPositionReflection() {
        if (positionReflectionInitialized) return;
        positionReflectionInitialized = true;
        try {
            serverboundMovePlayerPacketClass = loadClass("net.minecraft.network.protocol.game.ServerboundMovePlayerPacket");
            clientboundMoveEntityPacketClass = loadClass("net.minecraft.network.protocol.game.ClientboundMoveEntityPacket");
            clientboundTeleportEntityPacketClass = loadClass("net.minecraft.network.protocol.game.ClientboundTeleportEntityPacket");
            clientboundPlayerPositionPacketClass = loadClass("net.minecraft.network.protocol.game.ClientboundPlayerPositionPacket");
            clientboundEntityPositionSyncPacketClass = loadClass("net.minecraft.network.protocol.game.ClientboundEntityPositionSyncPacket");
            positionMoveRotationClass = loadClass("net.minecraft.world.entity.PositionMoveRotation");
            vec3Class = loadClass("net.minecraft.world.phys.Vec3");
            vecDeltaCodecClass = loadClass("net.minecraft.network.protocol.game.VecDeltaCodec");

            movePlayerGetX = requireMethod(serverboundMovePlayerPacketClass, 1, "getX");
            movePlayerGetY = requireMethod(serverboundMovePlayerPacketClass, 1, "getY");
            movePlayerGetZ = requireMethod(serverboundMovePlayerPacketClass, 1, "getZ");
            movePlayerGetYRot = requireMethod(serverboundMovePlayerPacketClass, 1, "getYRot");
            movePlayerGetXRot = requireMethod(serverboundMovePlayerPacketClass, 1, "getXRot");

            moveEntityGetXa = requireMethod(clientboundMoveEntityPacketClass, 0, "getXa");
            moveEntityGetYa = requireMethod(clientboundMoveEntityPacketClass, 0, "getYa");
            moveEntityGetZa = requireMethod(clientboundMoveEntityPacketClass, 0, "getZa");
            moveEntityGetYRot = requireMethod(clientboundMoveEntityPacketClass, 0, "getYRot");
            moveEntityGetXRot = requireMethod(clientboundMoveEntityPacketClass, 0, "getXRot");
            moveEntityHasPosition = requireMethod(clientboundMoveEntityPacketClass, 0, "hasPosition");
            moveEntityHasRotation = requireMethod(clientboundMoveEntityPacketClass, 0, "hasRotation");
            moveEntityIdField = findFieldAny(clientboundMoveEntityPacketClass, "entityId", "id");
            if (moveEntityIdField == null) {
                throw new NoSuchFieldException("ClientboundMoveEntityPacket.entityId");
            }
            moveEntityIdField.setAccessible(true);

            teleportEntityGetId = requireMethod(clientboundTeleportEntityPacketClass, 0, "id");
            teleportEntityGetChange = requireMethod(clientboundTeleportEntityPacketClass, 0, "change");
            teleportEntityGetRelatives = requireMethod(clientboundTeleportEntityPacketClass, 0, "relatives");

            playerPositionGetChange = requireMethod(clientboundPlayerPositionPacketClass, 0, "change");
            playerPositionGetRelatives = requireMethod(clientboundPlayerPositionPacketClass, 0, "relatives");

            entityPositionSyncGetId = requireMethod(clientboundEntityPositionSyncPacketClass, 0, "id");
            entityPositionSyncGetValues = requireMethod(clientboundEntityPositionSyncPacketClass, 0, "values");

            positionMoveRotationGetPosition = requireMethod(positionMoveRotationClass, 0, "position");
            positionMoveRotationGetYRot = requireMethod(positionMoveRotationClass, 0, "yRot");
            positionMoveRotationGetXRot = requireMethod(positionMoveRotationClass, 0, "xRot");

            vec3GetX = requireMethod(vec3Class, 0, "x");
            vec3GetY = requireMethod(vec3Class, 0, "y");
            vec3GetZ = requireMethod(vec3Class, 0, "z");
            vec3Ctor = vec3Class.getDeclaredConstructor(double.class, double.class, double.class);
            vec3Ctor.setAccessible(true);

            vecDeltaCodecCtor = vecDeltaCodecClass.getDeclaredConstructor();
            vecDeltaCodecCtor.setAccessible(true);
            vecDeltaCodecSetBase = requireMethod(vecDeltaCodecClass, 1, "setBase");
            vecDeltaCodecDecode = requireMethod(vecDeltaCodecClass, 3, "decode");

            positionReflectionAvailable = true;
            getLogger().info("[MindAxisView] Position packet reflection initialized successfully");
        } catch (Exception e) {
            positionReflectionAvailable = false;
            getLogger().warning("[MindAxisView] Position packet reflection not available: " + e.getMessage());
            getLogger().warning("[MindAxisView] Position interception will be disabled");
        }
    }

    private Object resolvePlayerChannel(Player player) throws Exception {
        if (player == null) return null;
        if (nmsAdapter != null) {
            return nmsAdapter.getPlayerChannel(player);
        }
        Object craftPlayer = player;
        Method getHandle = craftPlayer.getClass().getMethod("getHandle");
        Object serverPlayer = getHandle.invoke(craftPlayer);
        Field connectionField = serverPlayer.getClass().getField("connection");
        Object gamePacketListener = connectionField.get(serverPlayer);

        Field networkField = findField(gamePacketListener.getClass(), "connection");
        if (networkField == null && gamePacketListener.getClass().getSuperclass() != null) {
            networkField = findField(gamePacketListener.getClass().getSuperclass(), "connection");
        }
        if (networkField == null) return null;
        networkField.setAccessible(true);
        Object networkConnection = networkField.get(gamePacketListener);

        Field channelField = findField(networkConnection.getClass(), "channel");
        if (channelField == null) return null;
        channelField.setAccessible(true);
        return channelField.get(networkConnection);
    }

    private void installPlayerChannelHandler(Player player, String handlerName, ChannelHandler handler) throws Exception {
        if (player == null) {
            return;
        }
        if (nmsAdapter != null) {
            nmsAdapter.addChannelHandler(player, handlerName, handler);
            return;
        }
        Object channelObj = resolvePlayerChannel(player);
        if (channelObj == null) {
            throw new IllegalStateException("Cannot resolve player channel");
        }
        removePipelineHandler(channelObj, handlerName);
        Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
        installPipelineHandler(pipeline, handlerName, handler);
    }

    private boolean removePlayerChannelHandler(Player player, String handlerName) throws Exception {
        if (player == null) {
            return false;
        }
        if (nmsAdapter != null) {
            return nmsAdapter.removeChannelHandler(player, handlerName);
        }
        Object channelObj = resolvePlayerChannel(player);
        return channelObj != null && removePipelineHandler(channelObj, handlerName);
    }

    private boolean removePipelineHandler(Object channelObj, String handlerName) throws Exception {
        Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
        Object existing = pipeline.getClass().getMethod("get", String.class).invoke(pipeline, handlerName);
        if (existing == null) {
            return false;
        }
        pipeline.getClass().getMethod("remove", String.class).invoke(pipeline, handlerName);
        return true;
    }

    private void installPositionInterceptor(Player player) {
        initPositionReflection();
        if (!positionReflectionAvailable || player == null) return;

        try {
            PositionChannelHandler handler = new PositionChannelHandler(
                    this,
                    serverboundMovePlayerPacketClass,
                    clientboundMoveEntityPacketClass,
                    clientboundTeleportEntityPacketClass,
                    clientboundPlayerPositionPacketClass,
                    clientboundEntityPositionSyncPacketClass
            );
            installPlayerChannelHandler(player, POSITION_HANDLER_NAME, handler);

            getLogger().info("[MindAxisView] Position interceptor installed for " + player.getName());
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Failed to install position interceptor: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void removePositionInterceptor(Player player) {
        if (player == null) return;
        try {
            if (removePlayerChannelHandler(player, POSITION_HANDLER_NAME)) {
                getLogger().info("[MindAxisView] Position interceptor removed");
            }
        } catch (Exception e) {
            // Silently ignore — player may have disconnected
        }
    }

    private void removePositionInterceptorFromChannel(Object channelObj) {
        try {
            Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
            Object existing = pipeline.getClass().getMethod("get", String.class).invoke(pipeline, POSITION_HANDLER_NAME);
            if (existing != null) {
                pipeline.getClass().getMethod("remove", String.class).invoke(pipeline, POSITION_HANDLER_NAME);
                getLogger().info("[MindAxisView] Position interceptor removed");
            }
        } catch (Exception e) {
            // Ignore
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

    // Cached reflection state for block break progress (destruction) packet interception.
    private static volatile boolean blockBreakReflectionInitialized = false;
    private static volatile boolean blockBreakReflectionAvailable = false;
    private static Class<?> blockDestructionPacketClass;  // ClientboundBlockDestructionPacket
    private static Method bdpGetEntityId;                 // int
    private static Method bdpGetPos;                      // BlockPos
    private static Method bdpGetProgress;                 // int (0-9 stage, or -1 cancel)

    // Reflection state for polling ServerPlayerGameMode break progress (self-break).
    // MC doesn't send ClientboundBlockDestructionPacket to the breaker themselves,
    // so we poll the gameMode fields directly.
    private static volatile boolean breakPollReflectionInitialized = false;
    private static volatile boolean breakPollReflectionAvailable = false;
    private static volatile Field gameModeField;              // ServerPlayer.gameMode
    private static volatile Field isDestroyingBlockField;     // ServerPlayerGameMode.isDestroyingBlock
    private static volatile Field destroyPosField;            // ServerPlayerGameMode.destroyPos
    private static volatile Field lastSentStateField;         // ServerPlayerGameMode.lastSentState
    private volatile int lastPolledBreakStage = -1;
    private volatile String lastPolledBreakKey = "";

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
     * Initialize reflection handles for NMS block destruction (break progress) packet.
     * Safe to call multiple times; only runs once.
     * Reuses blockPosGetX/Y/Z from initBlockEventReflection if already initialized.
     */
    private void initBlockBreakReflection() {
        if (blockBreakReflectionInitialized) return;
        blockBreakReflectionInitialized = true;
        try {
            blockDestructionPacketClass = loadClass(
                    "net.minecraft.network.protocol.game.ClientboundBlockDestructionPacket"
            );
            bdpGetEntityId = requireMethod(blockDestructionPacketClass, 0, "getEntityId", "getId", "a");
            bdpGetPos = requireMethod(blockDestructionPacketClass, 0, "getPos", "b");
            bdpGetProgress = requireMethod(blockDestructionPacketClass, 0, "getProgress", "c");

            // Ensure blockPos accessors are available (may already be initialized by block event reflection)
            if (blockPosGetX == null) {
                Class<?> blockPosClass = loadClass("net.minecraft.core.BlockPos");
                blockPosGetX = requireMethod(blockPosClass, 0, "getX");
                blockPosGetY = requireMethod(blockPosClass, 0, "getY");
                blockPosGetZ = requireMethod(blockPosClass, 0, "getZ");
            }

            blockBreakReflectionAvailable = true;
            getLogger().info("[MindAxisView] Block break progress reflection initialized successfully");
        } catch (Exception e) {
            blockBreakReflectionAvailable = false;
            getLogger().warning("[MindAxisView] Block break progress reflection not available: " + e.getMessage());
            getLogger().warning("[MindAxisView] Block break progress interception will be disabled");
        }
    }

    /**
     * Initialize reflection handles for polling ServerPlayerGameMode break progress.
     * Safe to call multiple times; only runs once.
     */
    private void initBreakPollReflection() {
        if (breakPollReflectionInitialized) return;
        breakPollReflectionInitialized = true;
        try {
            Class<?> serverPlayerClass = loadClass("net.minecraft.server.level.ServerPlayer");
            gameModeField = serverPlayerClass.getField("gameMode");
            gameModeField.setAccessible(true);

            Class<?> gameModeClass = loadClass("net.minecraft.server.level.ServerPlayerGameMode");
            isDestroyingBlockField = gameModeClass.getDeclaredField("isDestroyingBlock");
            isDestroyingBlockField.setAccessible(true);
            destroyPosField = gameModeClass.getDeclaredField("destroyPos");
            destroyPosField.setAccessible(true);
            lastSentStateField = gameModeClass.getDeclaredField("lastSentState");
            lastSentStateField.setAccessible(true);

            // Ensure blockPos accessors are available
            if (blockPosGetX == null) {
                Class<?> blockPosClass = loadClass("net.minecraft.core.BlockPos");
                blockPosGetX = requireMethod(blockPosClass, 0, "getX");
                blockPosGetY = requireMethod(blockPosClass, 0, "getY");
                blockPosGetZ = requireMethod(blockPosClass, 0, "getZ");
            }

            breakPollReflectionAvailable = true;
            getLogger().info("[MindAxisView] Block break poll reflection initialized successfully");
        } catch (Exception e) {
            breakPollReflectionAvailable = false;
            getLogger().warning("[MindAxisView] Block break poll reflection not available: " + e.getMessage());
        }
    }

    /**
     * Poll the anchor player's ServerPlayerGameMode for active block breaking state.
     * MC doesn't send ClientboundBlockDestructionPacket to the player who is breaking,
     * so we read the progress directly and broadcast it to the viewer.
     */
    private void pollBlockBreakProgress() {
        if (!breakPollReflectionAvailable || wsServer == null) return;
        Player player = anchorPlayer;
        if (player == null || !player.isOnline()) return;

        try {
            Object craftPlayer = player;
            java.lang.reflect.Method getHandle = craftPlayer.getClass().getMethod("getHandle");
            Object serverPlayer = getHandle.invoke(craftPlayer);
            Object gameMode = gameModeField.get(serverPlayer);
            if (gameMode == null) return;

            boolean isDestroying = isDestroyingBlockField.getBoolean(gameMode);
            if (!isDestroying) {
                // If we were tracking a break, send stage=-1 to clear the overlay
                if (!lastPolledBreakKey.isEmpty()) {
                    String[] parts = lastPolledBreakKey.split(",");
                    if (parts.length == 3) {
                        broadcastBlockBreakProgressDirect(
                                Integer.parseInt(parts[0]),
                                Integer.parseInt(parts[1]),
                                Integer.parseInt(parts[2]),
                                -1);
                    }
                    lastPolledBreakKey = "";
                    lastPolledBreakStage = -1;
                }
                return;
            }

            Object pos = destroyPosField.get(gameMode);
            if (pos == null) return;
            int x = ((Number) blockPosGetX.invoke(pos)).intValue();
            int y = ((Number) blockPosGetY.invoke(pos)).intValue();
            int z = ((Number) blockPosGetZ.invoke(pos)).intValue();
            int stage = lastSentStateField.getInt(gameMode);

            String breakKey = x + "," + y + "," + z;
            // Only broadcast when stage changes or block changes
            if (stage != lastPolledBreakStage || !breakKey.equals(lastPolledBreakKey)) {
                // If switching to a new block, clear the old one first
                if (!lastPolledBreakKey.isEmpty() && !breakKey.equals(lastPolledBreakKey)) {
                    String[] parts = lastPolledBreakKey.split(",");
                    if (parts.length == 3) {
                        broadcastBlockBreakProgressDirect(
                                Integer.parseInt(parts[0]),
                                Integer.parseInt(parts[1]),
                                Integer.parseInt(parts[2]),
                                -1);
                    }
                }
                lastPolledBreakStage = stage;
                lastPolledBreakKey = breakKey;
                broadcastBlockBreakProgressDirect(x, y, z, stage);
            }
        } catch (Exception e) {
            // Silently ignore — reflection may fail on edge cases
        }
    }

    private void broadcastBlockBreakProgressDirect(int x, int y, int z, int stage) {
        if (wsServer == null) return;
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "blockBreakProgress");
        msg.addProperty("x", x);
        msg.addProperty("y", y);
        msg.addProperty("z", z);
        msg.addProperty("stage", stage);
        msg.addProperty("entityId", anchorEntityId);
        String json = msg.toString();
        nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
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
            SoundChannelHandler handler = new SoundChannelHandler(this, soundPacketClass, entitySoundPacketClass);
            installPlayerChannelHandler(player, SOUND_HANDLER_NAME, handler);

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
            if (removePlayerChannelHandler(player, SOUND_HANDLER_NAME)) {
                getLogger().info("[MindAxisView] Sound interceptor removed");
            }
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
            BlockEventChannelHandler handler = new BlockEventChannelHandler(this, blockEventPacketClass);
            installPlayerChannelHandler(player, BLOCK_EVENT_HANDLER_NAME, handler);

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
            if (removePlayerChannelHandler(player, BLOCK_EVENT_HANDLER_NAME)) {
                getLogger().info("[MindAxisView] Block event interceptor removed");
            }
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
     * to intercept outgoing block destruction (break progress) packets.
     * Uses reflection for all NMS access.
     */
    private void installBlockBreakInterceptor(Player player) {
        initBlockBreakReflection();
        if (!blockBreakReflectionAvailable) return;

        try {
            BlockBreakChannelHandler handler = new BlockBreakChannelHandler(this, blockDestructionPacketClass);
            installPlayerChannelHandler(player, BLOCK_BREAK_HANDLER_NAME, handler);

            getLogger().info("[MindAxisView] Block break progress interceptor installed for " + player.getName());
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Failed to install block break progress interceptor: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Remove the block break progress interceptor from a player's Netty pipeline.
     */
    private void removeBlockBreakInterceptor(Player player) {
        if (!blockBreakReflectionAvailable) return;
        try {
            if (removePlayerChannelHandler(player, BLOCK_BREAK_HANDLER_NAME)) {
                getLogger().info("[MindAxisView] Block break progress interceptor removed");
            }
        } catch (Exception e) {
            // Silently ignore — player may have disconnected
        }
    }

    private void removeBlockBreakInterceptorFromChannel(Object channelObj) {
        try {
            Object pipeline = channelObj.getClass().getMethod("pipeline").invoke(channelObj);
            Object existing = pipeline.getClass().getMethod("get", String.class).invoke(pipeline, BLOCK_BREAK_HANDLER_NAME);
            if (existing != null) {
                pipeline.getClass().getMethod("remove", String.class).invoke(pipeline, BLOCK_BREAK_HANDLER_NAME);
                getLogger().info("[MindAxisView] Block break progress interceptor removed");
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    // ---- Entity Remove Packet Interceptor ----

    private static volatile boolean entityRemoveReflectionInitialized = false;
    private static volatile boolean entityRemoveReflectionAvailable = false;
    private static volatile Class<?> removeEntitiesPacketClass;
    private static volatile java.lang.reflect.Method getEntityIdsMethod;

    private void initEntityRemoveReflection() {
        if (entityRemoveReflectionInitialized) return;
        entityRemoveReflectionInitialized = true;
        String[] classNames = {
                "net.minecraft.network.protocol.game.ClientboundRemoveEntitiesPacket",
                "net.minecraft.network.packet.s2c.play.EntitiesDestroyS2CPacket"
        };
        for (String className : classNames) {
            try {
                removeEntitiesPacketClass = Class.forName(className);
                getEntityIdsMethod = removeEntitiesPacketClass.getMethod("getEntityIds");
                entityRemoveReflectionAvailable = true;
                getLogger().info("[MindAxisView] Entity remove interceptor reflection initialized: " + className);
                return;
            } catch (Exception ignored) {}
        }
        getLogger().warning("[MindAxisView] Entity remove packet class not found — interceptor unavailable");
        entityRemoveReflectionAvailable = false;
    }

    private void installEntityRemoveInterceptor(Player player) {
        // Disabled — with real mineflayer connection, MC sends remove/add packets
        // for normal entity tracking (range in/out), causing viewer entity flicker.
        // Entity lifecycle is handled by entityBatch replace prune instead.
    }

    private void removeEntityRemoveInterceptor(Player player) {
        try {
            removePlayerChannelHandler(player, ENTITY_REMOVE_HANDLER_NAME);
        } catch (Exception e) {
            // Silently ignore
        }
    }

    void onEntityRemovePacketIntercepted(Object packet) {
        if (!running || wsServer == null || getEntityIdsMethod == null) return;
        try {
            Object entityIds = getEntityIdsMethod.invoke(packet);
            if (entityIds instanceof Iterable<?> iterable) {
                com.google.gson.JsonArray batch = new com.google.gson.JsonArray();
                for (Object idObj : iterable) {
                    if (idObj instanceof Number num) {
                        com.google.gson.JsonObject gone = new com.google.gson.JsonObject();
                        gone.addProperty("type", "entityGone");
                        gone.addProperty("id", num.intValue());
                        batch.add(gone);
                        lastEntityState.remove(num.intValue());
                        lastEntitySignature.remove(num.intValue());
                    }
                }
                if (batch.size() > 0) {
                    String json = batch.toString();
                    nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
                }
            }
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Entity remove intercept error: " + e.getMessage());
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
            ParticleChannelHandler handler = new ParticleChannelHandler(this, particlePacketClass);
            installPlayerChannelHandler(player, PARTICLE_HANDLER_NAME, handler);

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
            if (removePlayerChannelHandler(player, PARTICLE_HANDLER_NAME)) {
                getLogger().info("[MindAxisView] Particle interceptor removed");
            }
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

    // Position interception is currently not wired into the tracked plugin flow.
    // Keep no-op callbacks so optional helper classes in the workspace do not break clean builds.
    void onServerboundMovePlayerPacketIntercepted(Object packet) {}
    void onClientboundMoveEntityPacketIntercepted(Object packet) {}
    void onClientboundTeleportEntityPacketIntercepted(Object packet) {}
    void onClientboundPlayerPositionPacketIntercepted(Object packet) {
        // TP packet received — clear tracked chunks so syncTrackedChunks re-sends all.
        // Without clearing, chunks already in trackedChunkKeys are skipped even though
        // the viewer client needs them at the new position.
        if (!running || anchorPlayer == null) return;
        // Send chunkUnload for all previously tracked chunks before clearing
        for (Long key : new java.util.ArrayList<>(trackedChunkKeys)) {
            sendChunkUnload(chunkXFromKey(key), chunkZFromKey(key), "tp-unload");
        }
        trackedChunkKeys.clear();
        final int[] delays = {5, 15, 30, 60, 100}; // 0.25s, 0.75s, 1.5s, 3s, 5s
        for (int delay : delays) {
            Bukkit.getScheduler().runTaskLater(this, () -> {
                if (!running || !isActiveTrackedPlayer(anchorPlayer)) return;
                sendChunksAroundPlayer(anchorPlayer);
                scheduleTrackedChunksShmRefresh(anchorPlayer, 2L, "tp-refresh");
            }, delay);
        }
    }
    void onClientboundEntityPositionSyncPacketIntercepted(Object packet) {}

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

    public void mirrorSyntheticContainerOpen(Player player, org.bukkit.inventory.InventoryView view, String source) {
        if (!running || wsServer == null || !isActiveTrackedPlayer(player)) {
            return;
        }

        int windowId = resolveActiveContainerWindowId(player);
        BukkitContainerMirror.ContainerSnapshot snapshot = BukkitContainerMirror.capture(view, windowId);
        if (snapshot == null) {
            getLogger().warning("[MindAxisView] Synthetic container open mirror skipped: windowId=" + windowId
                    + " topType=" + (view != null && view.getTopInventory() != null ? view.getTopInventory().getType() : "null")
                    + " topSize=" + (view != null && view.getTopInventory() != null ? view.getTopInventory().getSize() : -1)
                    + " title=" + (view != null ? view.getTitle() : "null"));
            return;
        }

        emitInventoryBlockAction(snapshot.topInventory(), true, source);
        String openJson = ContainerPayloads.containerOpen(
                snapshot.windowId(),
                snapshot.containerType(),
                snapshot.title(),
                snapshot.slots()
        );
        String contentJson = ContainerPayloads.containerContent(snapshot.windowId(), snapshot.items());
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            wsServer.broadcastLive(openJson);
            wsServer.broadcastLive(contentJson);
        });
    }

    public void mirrorSyntheticContainerClose(Player player, org.bukkit.inventory.InventoryView view, String source) {
        if (!running || wsServer == null || !isActiveTrackedPlayer(player)) {
            return;
        }

        int windowId = resolveActiveContainerWindowId(player);
        BukkitContainerMirror.ContainerSnapshot snapshot = BukkitContainerMirror.capture(view, windowId);
        if (snapshot == null) {
            getLogger().warning("[MindAxisView] Synthetic container close mirror skipped: windowId=" + windowId
                    + " topType=" + (view != null && view.getTopInventory() != null ? view.getTopInventory().getType() : "null")
                    + " topSize=" + (view != null && view.getTopInventory() != null ? view.getTopInventory().getSize() : -1)
                    + " title=" + (view != null ? view.getTitle() : "null"));
            return;
        }

        emitInventoryBlockAction(snapshot.topInventory(), false, source);
        String closeJson = ContainerPayloads.containerClose(snapshot.windowId());
        nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(closeJson));
    }

    private int resolveActiveContainerWindowId(Player player) {
        if (player == null) {
            return -1;
        }
        try {
            Object handle = nmsAdapter != null ? nmsAdapter.getPlayerHandle(player) : null;
            if (handle == null) {
                return -1;
            }
            Field menuField = findFieldAny(handle.getClass(), "containerMenu", "currentScreenHandler");
            if (menuField == null) {
                return -1;
            }
            menuField.setAccessible(true);
            Object menu = menuField.get(handle);
            if (menu == null) {
                return -1;
            }
            Field windowIdField = findFieldAny(menu.getClass(), "containerId", "syncId");
            if (windowIdField == null) {
                return -1;
            }
            windowIdField.setAccessible(true);
            Object rawWindowId = windowIdField.get(menu);
            return rawWindowId instanceof Number number ? number.intValue() : -1;
        } catch (Exception ignored) {
            return -1;
        }
    }

    /**
     * Callback from BlockEventChannelHandler when a block event packet is intercepted.
     * @param packet the NMS packet object
     */
    void onBlockEventPacketIntercepted(Object packet) {
        broadcastBlockActionPacketReflective(packet);
    }

    /**
     * Callback from BlockBreakChannelHandler when a block destruction packet is intercepted.
     * @param packet the NMS ClientboundBlockDestructionPacket object
     */
    void onBlockBreakPacketIntercepted(Object packet) {
        broadcastBlockBreakProgressReflective(packet);
    }

    void onBlockEventPacketChecked(Object packet, boolean matched) {
        if (packet == null) return;
        String className = packet.getClass().getName();
        if (!matched && !className.contains("BlockEvent")) return;
        String key = className + "|" + matched;
        if (seenBlockEventPacketChecks.add(key)) {
            getLogger().info("[BlockEventInterceptor] packetClass=" + className + " matched=" + matched);
        }
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
            nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Container open packet extract failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void broadcastContainerClosePacketReflective(Object packet) {
        if (wsServer == null) return;
        try {
            int windowId = ((Number) containerCloseGetContainerId.invoke(packet)).intValue();
            String json = ContainerPayloads.containerClose(windowId);
            nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
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
            nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
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
            nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
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
            nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
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

            broadcastBlockAction(buildBlockActionMessage(x, y, z, actionId, actionParam, blockId), "packet");
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Block event packet extract failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void broadcastBlockBreakProgressReflective(Object packet) {
        if (wsServer == null) return;
        try {
            int entityId = ((Number) bdpGetEntityId.invoke(packet)).intValue();
            Object pos = bdpGetPos.invoke(packet);
            int x = ((Number) blockPosGetX.invoke(pos)).intValue();
            int y = ((Number) blockPosGetY.invoke(pos)).intValue();
            int z = ((Number) blockPosGetZ.invoke(pos)).intValue();
            int stage = ((Number) bdpGetProgress.invoke(packet)).intValue();

            JsonObject msg = new JsonObject();
            msg.addProperty("type", "blockBreakProgress");
            msg.addProperty("x", x);
            msg.addProperty("y", y);
            msg.addProperty("z", z);
            msg.addProperty("stage", stage);
            msg.addProperty("entityId", entityId);

            { String _msg = msg.toString(); nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(_msg)); }
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Block break progress packet extract failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    static JsonObject buildBlockActionMessage(int x, int y, int z, int actionId, int actionParam, int blockId) {
        return buildBlockActionMessage(x, y, z, actionId, actionParam, blockId, null, null);
    }

    static JsonObject buildBlockActionMessage(int x, int y, int z, int actionId, int actionParam, String kind, boolean open) {
        return buildBlockActionMessage(x, y, z, actionId, actionParam, null, open, kind);
    }

    private static JsonObject buildBlockActionMessage(
            int x,
            int y,
            int z,
            Integer actionId,
            Integer actionParam,
            Integer blockId,
            Boolean open,
            String kind
    ) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "blockAction");
        msg.addProperty("x", x);
        msg.addProperty("y", y);
        msg.addProperty("z", z);
        if (actionId != null) msg.addProperty("actionId", actionId);
        if (actionParam != null) msg.addProperty("actionParam", actionParam);
        if (blockId != null) msg.addProperty("blockId", blockId);
        if (open != null) msg.addProperty("open", open);
        if (kind != null && !kind.isEmpty()) msg.addProperty("kind", kind);
        return msg;
    }

    private void broadcastBlockAction(JsonObject msg, String source) {
        if (wsServer == null || msg == null) return;
        if (isDuplicateBlockAction(msg)) return;
        String json = msg.toString();
        getLogger().info("[MindAxisView] blockAction source=" + source + " payload=" + json);
        nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
    }

    private boolean isDuplicateBlockAction(JsonObject msg) {
        String key = blockActionDedupKey(msg);
        long now = System.currentTimeMillis();
        Long previous = recentBlockActionKeys.put(key, now);
        recentBlockActionKeys.entrySet().removeIf(entry -> now - entry.getValue() > BLOCK_ACTION_DUPLICATE_WINDOW_MS);
        return previous != null && now - previous <= BLOCK_ACTION_DUPLICATE_WINDOW_MS;
    }

    private String blockActionDedupKey(JsonObject msg) {
        int x = msg.get("x").getAsInt();
        int y = msg.get("y").getAsInt();
        int z = msg.get("z").getAsInt();
        String actionId = msg.has("actionId") ? msg.get("actionId").getAsString() : "";
        String actionParam = msg.has("actionParam") ? msg.get("actionParam").getAsString() : "";
        String open = msg.has("open")
                ? Boolean.toString(msg.get("open").getAsBoolean())
                : ("1".equals(actionId) && msg.has("actionParam")
                ? Boolean.toString(msg.get("actionParam").getAsInt() > 0)
                : "");
        return x + ":" + y + ":" + z + ":" + actionId + ":" + actionParam + ":" + open;
    }

    private void emitInventoryBlockAction(Inventory inventory, boolean opening, String source) {
        for (BlockActionTarget target : resolveInventoryBlockActionTargets(inventory)) {
            int actionParam = blockActionViewerCount(inventory, opening);
            broadcastBlockAction(
                    buildBlockActionMessage(
                            target.block().getX(),
                            target.block().getY(),
                            target.block().getZ(),
                            1,
                            actionParam,
                            target.kind(),
                            actionParam > 0
                    ),
                    source
            );
        }
    }

    private List<BlockActionTarget> resolveInventoryBlockActionTargets(Inventory inventory) {
        if (inventory == null) return List.of();
        InventoryHolder holder = inventory.getHolder(false);
        if (holder == null) return List.of();
        List<BlockActionTarget> targets = new ArrayList<>();
        if (holder instanceof DoubleChest doubleChest) {
            addInventoryBlockActionTarget(targets, doubleChest.getLeftSide(false));
            addInventoryBlockActionTarget(targets, doubleChest.getRightSide(false));
            return targets;
        }
        addInventoryBlockActionTarget(targets, holder);
        return targets;
    }

    private void addInventoryBlockActionTarget(List<BlockActionTarget> targets, InventoryHolder holder) {
        if (holder instanceof Chest chest) {
            targets.add(new BlockActionTarget(chest.getBlock(), "chest"));
        } else if (holder instanceof ShulkerBox shulkerBox) {
            targets.add(new BlockActionTarget(shulkerBox.getBlock(), "shulker"));
        } else if (holder instanceof EnderChest enderChest) {
            targets.add(new BlockActionTarget(enderChest.getBlock(), "chest"));
        }
    }

    private int blockActionViewerCount(Inventory inventory, boolean opening) {
        int viewers = inventory == null ? 0 : inventory.getViewers().size();
        return opening ? Math.max(1, viewers) : Math.max(0, viewers - 1);
    }

    private record BlockActionTarget(org.bukkit.block.Block block, String kind) {
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
            nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
        } catch (Exception e) {
            getLogger().warning("[MindAxisView] Particle packet extract failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private void broadcastFallbackBlockBreakParticle(org.bukkit.block.Block block, BlockData blockData) {
        if (block == null || blockData == null) return;
        JsonObject data = new JsonObject();
        int stateId = ChunkSerializer.getStateId(blockData);
        if (stateId >= 0) {
            data.addProperty("stateId", stateId);
            data.addProperty("blockStateId", stateId);
        }
        String json = ParticlePayloads.toJson(
                "minecraft:block",
                block.getX() + 0.5D,
                block.getY() + 0.5D,
                block.getZ() + 0.5D,
                0.25D,
                0.25D,
                0.25D,
                8,
                0.0D,
                data
        );
        nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
    }

    /**
     * Test hook for synthetic ServerPlayer E2E scenarios where Paper does not emit
     * clientbound particle packets for server-side block breaks.
     */
    public void broadcastBlockBreakParticleForTest(org.bukkit.block.Block block, BlockData blockData) {
        broadcastFallbackBlockBreakParticle(block, blockData);
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
            nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
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

    // Reflection utilities -- delegated to ReflectionUtil

    private static Field findField(Class<?> clazz, String name) {
        return ReflectionUtil.findField(clazz, name);
    }

    private static Field findFieldAny(Class<?> clazz, String... names) {
        return ReflectionUtil.findFieldAny(clazz, names);
    }

    private static Class<?> loadClass(String... names) throws ClassNotFoundException {
        return ReflectionUtil.loadClass(names);
    }

    private static Class<?> tryLoadClass(String... names) {
        return ReflectionUtil.tryLoadClass(names);
    }

    private static Method findMethod(Class<?> clazz, int parameterCount, String... names) {
        return ReflectionUtil.findMethod(clazz, parameterCount, names);
    }

    private static Method requireMethod(Class<?> clazz, int parameterCount, String... names) throws NoSuchMethodException {
        return ReflectionUtil.requireMethod(clazz, parameterCount, names);
    }

    private JsonElement serializeTrackedEquipmentItem(ItemStack item, int slot) {
        if (item == null || item.getType() == Material.AIR) return JsonNull.INSTANCE;

        JsonObject obj = serializeItem(item, slot);
        obj.addProperty("metadata", 0);

        ItemMeta meta = item.hasItemMeta() ? item.getItemMeta() : null;
        if (meta instanceof Damageable damageable) {
            obj.addProperty("damage", damageable.getDamage());
        }

        JsonArray enchantments = serializeInventoryEnchantments(meta);
        if (enchantments.size() > 0) {
            obj.add("enchantments", enchantments);
        }

        return obj;
    }

    private void sendInventoryState(Player player) {
        sendInventory(player);
        sendFullInventory(player);
    }

    // ---- Inventory Broadcasting ----

    /**
     * Send hotbar inventory (slots 0-8) and selected slot.
     * The viewer reads bot.inventory.slots[36..44] for hotbar display.
     */
    private void sendInventory(Player player) {
        if (wsServer == null || !isActiveTrackedPlayer(player)) return;

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

        nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
    }

    /**
     * Send the full 46-slot player inventory for plugin/mineflayer parity validation.
     */
    private void sendFullInventory(Player player) {
        if (wsServer == null || !isActiveTrackedPlayer(player)) return;

        PlayerInventory inv = player.getInventory();
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "fullInventory");

        JsonArray slots = new JsonArray();
        for (int slot = 0; slot < FULL_INVENTORY_SLOT_COUNT; slot++) {
            slots.add(serializeFullInventoryItem(getFullInventoryItem(inv, slot), slot));
        }
        msg.add("slots", slots);
        msg.addProperty("selectedSlot", inv.getHeldItemSlot());

        String json = msg.toString();
        String hash = json.hashCode() + "";
        if (hash.equals(lastFullInventoryHash)) return;
        lastFullInventoryHash = hash;

        nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
    }

    private ItemStack getFullInventoryItem(PlayerInventory inv, int slot) {
        if (inv == null || slot < 0 || slot >= FULL_INVENTORY_SLOT_COUNT) return null;
        if (slot >= 36 && slot <= 44) return inv.getItem(slot - 36);
        if (slot >= 9 && slot <= 35) return inv.getItem(slot);
        return switch (slot) {
            case 5 -> inv.getHelmet();
            case 6 -> inv.getChestplate();
            case 7 -> inv.getLeggings();
            case 8 -> inv.getBoots();
            case 45 -> inv.getItemInOffHand();
            default -> null;
        };
    }

    private JsonElement serializeFullInventoryItem(ItemStack item, int slot) {
        if (item == null || item.getType() == Material.AIR) return JsonNull.INSTANCE;

        JsonObject obj = serializeItem(item, slot);
        obj.addProperty("metadata", 0);

        ItemMeta meta = item.hasItemMeta() ? item.getItemMeta() : null;
        if (meta instanceof Damageable damageable) {
            obj.addProperty("damage", damageable.getDamage());
        }

        JsonArray enchantments = serializeInventoryEnchantments(meta);
        if (enchantments.size() > 0) {
            obj.add("enchantments", enchantments);
        }

        return obj;
    }

    private JsonArray serializeInventoryEnchantments(ItemMeta meta) {
        JsonArray enchantments = new JsonArray();
        if (meta == null || meta.getEnchants().isEmpty()) return enchantments;

        List<Map.Entry<Enchantment, Integer>> entries = new ArrayList<>(meta.getEnchants().entrySet());
        entries.sort((left, right) -> left.getKey().getKey().toString().compareTo(right.getKey().getKey().toString()));
        for (Map.Entry<Enchantment, Integer> entry : entries) {
            JsonObject enchantment = new JsonObject();
            enchantment.addProperty("name", entry.getKey().getKey().toString());
            enchantment.addProperty("level", entry.getValue());
            enchantments.add(enchantment);
        }

        return enchantments;
    }

    // ---- Equipment Broadcasting ----

    /**
     * Send full equipment state: main hand, off hand, armor.
     */
    private void sendEquipment(Player player) {
        if (wsServer == null || !isActiveTrackedPlayer(player)) return;

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

        nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
    }

    // ---- Status Broadcasting ----

    /**
     * Broadcast HP, food, experience periodically (called by scheduled task).
     */
    private void broadcastStatus() {
        if (!running || wsServer == null || !isActiveTrackedPlayer(anchorPlayer)) return;

        double health = anchorPlayer.getHealth();
        int food = anchorPlayer.getFoodLevel();
        float exp = anchorPlayer.getExp();
        int level = anchorPlayer.getLevel();

        int air = anchorPlayer.getRemainingAir();
        // Only send if changed
        if (health == lastHealth && food == lastFood && exp == lastExp && level == lastLevel && air == lastAir) return;
        lastAir = air;
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
        if (isActiveTrackedPlayer(anchorPlayer)) {
            msg.addProperty("air", anchorPlayer.getRemainingAir());
            msg.addProperty("maxAir", anchorPlayer.getMaximumAir());
        }

        String json = msg.toString();
        nettyDispatchExecutor.execute(() -> wsServer.broadcastLive(json));
    }

    // ---- Skin Broadcasting ----

    /**
     * Send player skin texture URL. The viewer uses this to render the player model.
     * Tries PlayerTextures API first, falls back to raw profile properties.
     */
    private void sendSkinWithRetries(Player player) {
        if (player == null) return;
        if (sendSkin(player)) return;
        scheduleSkinRetry(player, 20L);
        scheduleSkinRetry(player, 40L);
    }

    private void scheduleSkinRetry(Player player, long delayTicks) {
        if (player == null) return;
        Bukkit.getScheduler().runTaskLater(this, () -> {
            if (!running || wsServer == null || !player.equals(anchorPlayer) || !isActiveTrackedPlayer(player)) return;
            sendSkin(player);
        }, delayTicks);
    }

    private String resolvePlayerSkinUrl(Player player, boolean logParseFailure) {
        if (player == null) return null;

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
                if (logParseFailure) {
                    getLogger().warning("[MindAxisView] Failed to parse skin textures for " + player.getName() + ": " + e.getMessage());
                }
            }
        }

        return skinUrl;
    }

    private boolean sendSkin(Player player) {
        if (wsServer == null || !isActiveTrackedPlayer(player)) return false;

        String skinUrl = resolvePlayerSkinUrl(player, true);

        JsonObject msg = new JsonObject();
        msg.addProperty("type", "skin");
        msg.addProperty("username", player.getName());
        if (skinUrl != null) {
            msg.addProperty("skinUrl", skinUrl);
        }

        String json = msg.toString();
        getLogger().info("[MindAxisView] Skin for " + player.getName() + ": " + (skinUrl != null ? skinUrl : "(none)"));
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> wsServer.broadcastMessage(json));
        return skinUrl != null;
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
     * Poll {shared-data-root}/action-request.json every tick.
     * If a new request is found (different id from last processed), execute it
     * via Bukkit API and write the result to {shared-data-root}/action-result.json.
     *
     * Runs on the Bukkit main thread so all Bukkit API calls are thread-safe.
     */
    private void processActionRequest() {
        if (!running || !isActiveTrackedPlayer(anchorPlayer)) return;

        try {
            if (shmEnabled && Files.exists(sharedActionRequestPath)) {
                String content = Files.readString(sharedActionRequestPath);
                if (content != null && !content.isBlank()) {
                    JsonObject request = com.google.gson.JsonParser.parseString(content).getAsJsonObject();
                    String id = request.has("id") ? request.get("id").getAsString() : "";

                    // Skip if we already processed this request
                    if (!id.equals(lastProcessedActionId)) {
                        lastProcessedActionId = id;

                        String action = request.has("action") ? request.get("action").getAsString() : "";
                        JsonObject params = request.has("params") ? request.getAsJsonObject("params") : new JsonObject();

                        getLogger().info("[MindAxisAction] Processing action: " + action + " id=" + id);

                        if ("dig".equals(action)) {
                            executeDigAction(params, result -> writeActionResult(id, result));
                        } else {
                            // Execute action and capture result
                            ActionResult result;
                            switch (action) {
                                case "place" -> result = executePlaceAction(params);
                                case "attack" -> result = executeAttackAction(params);
                                case "activate" -> result = executeActivateAction(params);
                                default -> result = new ActionResult(false, "Unknown action: " + action);
                            }

                            // Write result
                            writeActionResult(id, result);
                        }
                    }
                }
            }
        } catch (Exception e) {
            getLogger().warning("[MindAxisAction] Error processing action request: " + e.getMessage());
        } finally {
            // Always drain the WS queue even when no shared-data action file exists.
            processWsActionQueue();
        }
    }

    /**
     * Drain queued WS action requests and execute them on the main thread.
     * Results are sent back to the originating WS client.
     */
    private void processWsActionQueue() {
        if (wsServer == null) return;
        var queue = wsServer.getWsActionQueue();
        WSServer.WsActionRequest wsReq;
        while ((wsReq = queue.poll()) != null) {
            try {
                WSServer.WsActionRequest currentReq = wsReq;
                JsonObject request = currentReq.getJson();
                String id = request.has("id") ? request.get("id").getAsString() : "";
                String action = request.has("action") ? request.get("action").getAsString() : "";
                JsonObject params = request.has("params") ? request.getAsJsonObject("params") : new JsonObject();

                getLogger().info("[MindAxisAction-WS] Processing action: " + action + " id=" + id);

                if ("dig".equals(action)) {
                    executeDigAction(params, result -> sendWsActionResult(currentReq, id, action, params, result));
                    continue;
                }

                ActionResult result;
                switch (action) {
                    case "place" -> result = executePlaceAction(params);
                    case "attack" -> result = executeAttackAction(params);
                    case "activate" -> result = executeActivateAction(params);
                    default -> result = new ActionResult(false, "Unknown action: " + action);
                }

                sendWsActionResult(currentReq, id, action, params, result);
            } catch (Exception e) {
                getLogger().warning("[MindAxisAction-WS] Error processing WS action: " + e.getMessage());
            }
        }
    }

    /**
     * Execute a dig (block break) action via Bukkit API.
     * Simulates the client-side mining delay before calling Player.breakBlock().
     */
    private void executeDigAction(JsonObject params, Consumer<ActionResult> onComplete) {
        if (!params.has("x") || !params.has("y") || !params.has("z")) {
            onComplete.accept(new ActionResult(false, "Missing x/y/z parameters"));
            return;
        }

        int x = params.get("x").getAsInt();
        int y = params.get("y").getAsInt();
        int z = params.get("z").getAsInt();

        Player player = anchorPlayer;
        int entityId = player.getEntityId();
        org.bukkit.World world = player.getWorld();
        org.bukkit.block.Block block = world.getBlockAt(x, y, z);
        ActionResult reachResult = validateInteractionReach(player, block.getLocation());
        if (reachResult != null) {
            onComplete.accept(reachResult);
            return;
        }

        if (isAir(block.getType())) {
            onComplete.accept(new ActionResult(true, null, buildDigResultExtras(0)));
            return;
        }

        try {
            int breakTicks = calculateBreakTicks(player, block);
            if (player.getGameMode() == GameMode.CREATIVE || breakTicks <= 0) {
                onComplete.accept(finishDigAction(player, block, 0));
                return;
            }

            broadcastBlockBreakProgress(block, 0, entityId);

            BukkitTask[] progressTaskRef = new BukkitTask[1];
            int[] lastStageRef = {0};
            if (breakTicks > 1) {
                final int totalTicks = breakTicks;
                progressTaskRef[0] = Bukkit.getScheduler().runTaskTimer(this, new Runnable() {
                    private int elapsedTicks = 1;

                    @Override
                    public void run() {
                        if (elapsedTicks >= totalTicks) {
                            if (progressTaskRef[0] != null) {
                                progressTaskRef[0].cancel();
                            }
                            return;
                        }

                        int stage = Math.min(9, (int) Math.floor((elapsedTicks * 10.0D) / totalTicks));
                        if (stage > lastStageRef[0]) {
                            for (int currentStage = lastStageRef[0] + 1; currentStage <= stage; currentStage++) {
                                broadcastBlockBreakProgress(block, currentStage, entityId);
                            }
                            lastStageRef[0] = stage;
                        }
                        elapsedTicks++;
                    }
                }, 1L, 1L);
            }

            Bukkit.getScheduler().runTaskLater(this, () -> {
                if (progressTaskRef[0] != null) {
                    progressTaskRef[0].cancel();
                }
                for (int currentStage = lastStageRef[0] + 1; currentStage <= 9; currentStage++) {
                    broadcastBlockBreakProgress(block, currentStage, entityId);
                }
                ActionResult result = finishDigAction(player, block, breakTicks);
                broadcastBlockBreakProgress(block, -1, entityId);
                onComplete.accept(result);
            }, breakTicks);
        } catch (Exception e) {
            onComplete.accept(new ActionResult(false, "break timing exception: " + e.getMessage()));
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
        ActionResult reachResult = validateInteractionReach(anchorPlayer, block.getLocation());
        if (reachResult != null) {
            return reachResult;
        }

        // Resolve face parameter (0=bottom,1=top,2=north,3=south,4=west,5=east)
        org.bukkit.block.BlockFace placeFace = org.bukkit.block.BlockFace.UP;
        if (params.has("face")) {
            int faceInt = params.get("face").getAsInt();
            placeFace = switch (faceInt) {
                case 0 -> org.bukkit.block.BlockFace.DOWN;
                case 1 -> org.bukkit.block.BlockFace.UP;
                case 2 -> org.bukkit.block.BlockFace.NORTH;
                case 3 -> org.bukkit.block.BlockFace.SOUTH;
                case 4 -> org.bukkit.block.BlockFace.WEST;
                case 5 -> org.bukkit.block.BlockFace.EAST;
                default -> org.bukkit.block.BlockFace.UP;
            };
        }

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

            // Consume held item from player inventory (1 unit of the placed block)
            org.bukkit.inventory.ItemStack mainHand = anchorPlayer.getInventory().getItemInMainHand();
            if (mainHand != null && mainHand.getType() == material && mainHand.getAmount() > 0) {
                if (mainHand.getAmount() == 1) {
                    anchorPlayer.getInventory().setItemInMainHand(null);
                } else {
                    mainHand.setAmount(mainHand.getAmount() - 1);
                }
            }

            getLogger().info("[MindAxisAction] Placed " + material + " at " + x + "," + y + "," + z + " face=" + placeFace);
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
        ActionResult reachResult = validateInteractionReach(anchorPlayer, target.getLocation());
        if (reachResult != null) {
            return reachResult;
        }

        try {
            // Capture health before attack for damage feedback
            double healthBefore = (target instanceof LivingEntity lt) ? lt.getHealth() : -1;

            anchorPlayer.attack(target);

            // Capture health after attack (synchronous on main thread — immediate)
            double healthAfter = (target instanceof LivingEntity lt2) ? lt2.getHealth() : -1;
            boolean killed = (target instanceof LivingEntity lt3) && lt3.isDead();

            JsonObject extras = new JsonObject();
            extras.addProperty("entityId", target.getEntityId());
            if (healthBefore >= 0) {
                double damage = Math.max(0, healthBefore - healthAfter);
                extras.addProperty("damage", damage);
                extras.addProperty("targetHealth", healthAfter);
                extras.addProperty("killed", killed);
            }

            getLogger().info("[MindAxisAction] Attacked entity " + target.getType()
                    + " (id=" + target.getEntityId() + ")"
                    + (healthBefore >= 0 ? " dmg=" + String.format("%.1f", healthBefore - healthAfter)
                            + " hp=" + String.format("%.1f", healthAfter) : ""));
            return new ActionResult(true, null, extras);
        } catch (Exception e) {
            return new ActionResult(false, "attack exception: " + e.getMessage());
        }
    }

    /**
     * Execute an activate (right-click interaction) action via Bukkit API.
     * Handles doors, trapdoors, fence gates, chests, crafting tables, furnaces,
     * buttons, levers, and beds.
     *
     * Params: x, y, z
     */
    private ActionResult executeActivateAction(JsonObject params) {
        if (!params.has("x") || !params.has("y") || !params.has("z")) {
            return new ActionResult(false, "Missing x/y/z parameters");
        }

        int x = params.get("x").getAsInt();
        int y = params.get("y").getAsInt();
        int z = params.get("z").getAsInt();

        org.bukkit.World world = anchorPlayer.getWorld();
        org.bukkit.block.Block block = world.getBlockAt(x, y, z);
        ActionResult reachResult = validateInteractionReach(anchorPlayer, block.getLocation());
        if (reachResult != null) {
            return reachResult;
        }
        Material type = block.getType();

        if (type == Material.AIR || type == Material.CAVE_AIR || type == Material.VOID_AIR) {
            return new ActionResult(false, "not_interactable");
        }

        try {
            org.bukkit.block.data.BlockData data = block.getBlockData();

            // 1. Openable blocks: doors, trapdoors, fence gates
            if (data instanceof org.bukkit.block.data.Openable openable) {
                openable.setOpen(!openable.isOpen());
                block.setBlockData(data, true);
                org.bukkit.block.Block linkedBlock = findLinkedDoorHalf(block, type);
                JsonObject extras = buildActivateResultExtras(block, linkedBlock);
                sendBlockUpdate(block);
                getLogger().info("[MindAxisAction] Toggled openable at " + x + "," + y + "," + z + " (" + type + ") open=" + openable.isOpen());
                return new ActionResult(true, null, extras);
            }

            // 2. Buttons and levers (Powerable)
            if (data instanceof org.bukkit.block.data.Powerable powerable) {
                powerable.setPowered(!powerable.isPowered());
                block.setBlockData(data, true);
                JsonObject extras = buildActivateResultExtras(block, null);
                sendBlockUpdate(block);
                // For buttons, schedule un-power after 1 second (20 ticks)
                String typeName = type.name();
                if (typeName.contains("BUTTON")) {
                    Bukkit.getScheduler().runTaskLater(this, () -> {
                        org.bukkit.block.data.BlockData currentData = block.getBlockData();
                        if (currentData instanceof org.bukkit.block.data.Powerable btn) {
                            btn.setPowered(false);
                            block.setBlockData(currentData, true);
                            sendBlockUpdate(block);
                        }
                    }, 20L);
                }
                getLogger().info("[MindAxisAction] Toggled powerable at " + x + "," + y + "," + z + " (" + type + ") powered=" + powerable.isPowered());
                return new ActionResult(true, null, extras);
            }

            // 3. Containers: chests, furnaces, crafting tables, etc.
            org.bukkit.block.BlockState state = block.getState();
            if (state instanceof org.bukkit.inventory.InventoryHolder holder) {
                anchorPlayer.openInventory(holder.getInventory());
                getLogger().info("[MindAxisAction] Opened container at " + x + "," + y + "," + z + " (" + type + ")");
                return new ActionResult(true, null);
            }

            // 4. Crafting tables (not InventoryHolder — need workbench view)
            if (type == Material.CRAFTING_TABLE) {
                anchorPlayer.openWorkbench(block.getLocation(), true);
                getLogger().info("[MindAxisAction] Opened crafting table at " + x + "," + y + "," + z);
                return new ActionResult(true, null);
            }

            // 5. Beds
            String typeName = type.name();
            if (typeName.contains("BED")) {
                // Bed usage only works at night or during thunderstorms
                long time = world.getTime();
                boolean isNight = time >= 12541 && time <= 23458;
                boolean isThunder = world.isThundering();
                if (!isNight && !isThunder) {
                    return new ActionResult(false, "Cannot sleep — not night or thunderstorm");
                }
                // Try to set the player's bed spawn and make them sleep.
                // Bukkit Player.sleep() does NOT call trySleep/setSpawnPoint, so we
                // must set the respawn location explicitly.
                anchorPlayer.setSleepingIgnored(false);
                boolean sleeping = anchorPlayer.sleep(block.getLocation(), true);
                if (sleeping) {
                    try { anchorPlayer.setRespawnLocation(block.getLocation(), true); } catch (NoSuchMethodError ignored) {
                        try { anchorPlayer.setBedSpawnLocation(block.getLocation(), true); } catch (Exception ignored2) {}
                    }
                    getLogger().info("[MindAxisAction] Player sleeping at bed " + x + "," + y + "," + z + " (spawn set)");
                    return new ActionResult(true, null);
                } else {
                    return new ActionResult(false, "sleep failed (bed obstructed or monsters nearby)");
                }
            }

            return new ActionResult(false, "not_interactable");
        } catch (Exception e) {
            return new ActionResult(false, "activate exception: " + e.getMessage());
        }
    }

    private ActionResult validateInteractionReach(Player anchorPlayer, Location target) {
        Location eye = anchorPlayer.getEyeLocation();
        if (target == null || eye.getWorld() != target.getWorld()) {
            return new ActionResult(false, "too_far");
        }
        if (!isWithinInteractionReach(anchorPlayer.getGameMode(), eye.distance(target))) {
            return new ActionResult(false, "too_far");
        }
        return null;
    }

    static boolean isWithinInteractionReach(GameMode gameMode, double distance) {
        return distance <= (gameMode == GameMode.CREATIVE ? 5.0 : 4.5);
    }

    private void sendWsActionResult(WSServer.WsActionRequest wsReq, String id, String action, JsonObject params, ActionResult result) {
        wsServer.sendToRequestClient(wsReq, buildWsActionResultJson(id, action, params, result).toString());
    }

    private JsonObject buildWsActionResultJson(String id, String action, JsonObject params, ActionResult result) {
        JsonObject resultJson = new JsonObject();
        resultJson.addProperty("type", "actionResult");
        resultJson.addProperty("id", id);
        resultJson.addProperty("success", result.success());
        if (result.error() != null) {
            resultJson.addProperty("error", result.error());
        } else {
            resultJson.add("error", JsonNull.INSTANCE);
        }
        resultJson.addProperty("action", action);
        if (params.has("x")) resultJson.addProperty("x", params.get("x").getAsInt());
        if (params.has("y")) resultJson.addProperty("y", params.get("y").getAsInt());
        if (params.has("z")) resultJson.addProperty("z", params.get("z").getAsInt());
        if (params.has("entityId")) resultJson.addProperty("entityId", params.get("entityId").getAsInt());
        if (result.extras() != null) {
            for (var entry : result.extras().entrySet()) {
                resultJson.add(entry.getKey(), entry.getValue());
            }
        }
        return resultJson;
    }

    private ActionResult finishDigAction(Player player, org.bukkit.block.Block block, int breakTicks) {
        org.bukkit.block.Block liveBlock = block.getWorld().getBlockAt(block.getX(), block.getY(), block.getZ());
        if (isAir(liveBlock.getType())) {
            return new ActionResult(true, null, buildDigResultExtras(breakTicks));
        }

        try {
            Material brokenType = liveBlock.getType();
            boolean success = player.breakBlock(liveBlock);
            if (success) {
                getLogger().info("[MindAxisAction] Broke block at "
                        + liveBlock.getX() + "," + liveBlock.getY() + "," + liveBlock.getZ()
                        + " (" + brokenType + ")"
                        + " breakTicks=" + breakTicks);
                return new ActionResult(true, null, buildDigResultExtras(breakTicks));
            }
            return new ActionResult(false, "breakBlock returned false (event cancelled or unbreakable)", buildDigResultExtras(breakTicks));
        } catch (Exception e) {
            return new ActionResult(false, "breakBlock exception: " + e.getMessage(), buildDigResultExtras(breakTicks));
        }
    }

    int calculateBreakTicks(Player player, org.bukkit.block.Block block) {
        if (player.getGameMode() == GameMode.CREATIVE || isAir(block.getType())) {
            return 0;
        }

        try {
            Object nmsPlayer = player.getClass().getMethod("getHandle").invoke(player);
            Object nmsWorld = player.getWorld().getClass().getMethod("getHandle").invoke(player.getWorld());
            Object blockPos = resolveBreakTimingBlockPosConstructor().newInstance(block.getX(), block.getY(), block.getZ());
            Object blockState = invokeCompatible(nmsWorld, List.of("getBlockState"), blockPos);

            double destroyProgress = ((Number) invokeCompatible(blockState, List.of("getDestroyProgress", "calcBlockBreakingDelta"), nmsPlayer, nmsWorld, blockPos)).doubleValue();
            if (!Double.isFinite(destroyProgress) || destroyProgress <= 0.0D) {
                return 0;
            }

            return Math.max(1, (int) Math.ceil(1.0D / destroyProgress));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("calculateBreakTicks reflection failed: " + e.getMessage(), e);
        }
    }

    private JsonObject buildDigResultExtras(int breakTicks) {
        JsonObject extras = new JsonObject();
        extras.addProperty("breakTicks", breakTicks);
        return extras;
    }

    private void broadcastBlockBreakProgress(org.bukkit.block.Block block, int stage, int entityId) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "blockBreakProgress");
        msg.addProperty("x", block.getX());
        msg.addProperty("y", block.getY());
        msg.addProperty("z", block.getZ());
        msg.addProperty("stage", stage);
        msg.addProperty("entityId", entityId);
        broadcastLive(msg.toString());
    }

    private Constructor<?> resolveBreakTimingBlockPosConstructor() throws ReflectiveOperationException {
        Constructor<?> cached = breakTimingBlockPosConstructor;
        if (cached != null) {
            return cached;
        }

        Constructor<?> resolved = findClass("net.minecraft.core.BlockPos", "net.minecraft.util.math.BlockPos")
                .getConstructor(int.class, int.class, int.class);
        resolved.setAccessible(true);
        breakTimingBlockPosConstructor = resolved;
        return resolved;
    }

    private Class<?> findClass(String... candidateNames) throws ClassNotFoundException {
        ClassNotFoundException lastError = null;
        for (String candidateName : candidateNames) {
            try {
                return Class.forName(candidateName);
            } catch (ClassNotFoundException e) {
                lastError = e;
            }
        }
        throw lastError != null ? lastError : new ClassNotFoundException("No class candidates provided");
    }

    private Object invokeCompatible(Object target, List<String> candidateNames, Object... args) throws ReflectiveOperationException {
        Method method = findCompatibleMethod(target.getClass(), candidateNames, args);
        return method.invoke(target, args);
    }

    private Method findCompatibleMethod(Class<?> type, List<String> candidateNames, Object... args) throws NoSuchMethodException {
        for (Method method : type.getMethods()) {
            if (!candidateNames.contains(method.getName())) continue;
            if (!areCompatibleParameterTypes(method.getParameterTypes(), args)) continue;
            method.setAccessible(true);
            return method;
        }

        Class<?> current = type;
        while (current != null) {
            for (Method method : current.getDeclaredMethods()) {
                if (!candidateNames.contains(method.getName())) continue;
                if (!areCompatibleParameterTypes(method.getParameterTypes(), args)) continue;
                method.setAccessible(true);
                return method;
            }
            current = current.getSuperclass();
        }

        throw new NoSuchMethodException("No compatible method on " + type.getName() + " for " + candidateNames);
    }

    private boolean areCompatibleParameterTypes(Class<?>[] parameterTypes, Object[] args) {
        if (parameterTypes.length != args.length) {
            return false;
        }

        for (int i = 0; i < parameterTypes.length; i++) {
            if (args[i] == null) {
                if (parameterTypes[i].isPrimitive()) {
                    return false;
                }
                continue;
            }

            Class<?> expected = wrapPrimitive(parameterTypes[i]);
            if (!expected.isAssignableFrom(args[i].getClass())) {
                return false;
            }
        }
        return true;
    }

    private Class<?> wrapPrimitive(Class<?> type) {
        if (!type.isPrimitive()) return type;
        if (type == boolean.class) return Boolean.class;
        if (type == byte.class) return Byte.class;
        if (type == short.class) return Short.class;
        if (type == int.class) return Integer.class;
        if (type == long.class) return Long.class;
        if (type == float.class) return Float.class;
        if (type == double.class) return Double.class;
        if (type == char.class) return Character.class;
        return type;
    }

    private boolean isAir(Material material) {
        return material == Material.AIR || material == Material.CAVE_AIR || material == Material.VOID_AIR;
    }

    private JsonObject buildActivateResultExtras(org.bukkit.block.Block primaryBlock, org.bukkit.block.Block secondaryBlock) {
        JsonObject extras = new JsonObject();
        JsonArray blockUpdates = new JsonArray();
        JsonObject primaryUpdate = buildActivateBlockUpdatePayload(primaryBlock);
        blockUpdates.add(primaryUpdate);
        if (secondaryBlock != null && !secondaryBlock.equals(primaryBlock)) {
            blockUpdates.add(buildActivateBlockUpdatePayload(secondaryBlock));
        }
        extras.addProperty("x", primaryBlock.getX());
        extras.addProperty("y", primaryBlock.getY());
        extras.addProperty("z", primaryBlock.getZ());
        if (primaryUpdate.has("stateId")) {
            extras.addProperty("stateId", primaryUpdate.get("stateId").getAsInt());
        }
        extras.add("blockUpdates", blockUpdates);
        return extras;
    }

    private JsonObject buildActivateBlockUpdatePayload(org.bukkit.block.Block block) {
        JsonObject update = buildBlockUpdatePayload(block, false);
        if (update.has("sid") && !update.has("stateId")) {
            update.addProperty("stateId", update.get("sid").getAsInt());
        }
        return update;
    }

    private org.bukkit.block.Block findLinkedDoorHalf(org.bukkit.block.Block block, org.bukkit.block.data.type.Door door) {
        if (block == null || door == null) return null;
        return block.getRelative(
                door.getHalf() == org.bukkit.block.data.Bisected.Half.TOP ? BlockFace.DOWN : BlockFace.UP
        );
    }

    private org.bukkit.block.Block findLinkedDoorHalf(org.bukkit.block.Block block, Material type) {
        org.bukkit.block.data.BlockData blockData = block.getBlockData();
        if (!(blockData instanceof org.bukkit.block.data.type.Door door)) return null;

        org.bukkit.block.Block linkedBlock = findLinkedDoorHalf(block, door);
        return linkedBlock.getType() == type ? linkedBlock : null;
    }

    /**
     * Write action results to {shared-data-root}/action-result.json atomically.
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
        // Merge action-specific extras (e.g. damage, targetHealth for attack)
        if (result.extras() != null) {
            for (var entry : result.extras().entrySet()) {
                obj.add(entry.getKey(), entry.getValue());
            }
        }

        // Atomic write: write to tmp, then rename
        try {
            Files.writeString(sharedActionResultTmpPath, obj.toString());
            Files.move(sharedActionResultTmpPath, sharedActionResultPath,
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            getLogger().warning("[MindAxisAction] Failed to write action result: " + e.getMessage());
        }
    }

    /**
     * Simple record for action execution results.
     * extras: optional JsonObject with action-specific fields (e.g. damage, targetHealth for attack).
     */
    private record ActionResult(boolean success, String error, JsonObject extras) {
        ActionResult(boolean success, String error) {
            this(success, error, null);
        }
    }

    // ---- Getters / Setters ----

    @Override
    public boolean isRunning() {
        return running;
    }

    public int getWsPort() {
        return wsPort;
    }

    @Override
    public WSServer getWsServer() {
        return wsServer;
    }

    @Override
    public Player getAnchorPlayer() {
        return anchorPlayer;
    }

    // ---- CraftLinkAPI delegate methods ----

    @Override
    public void broadcastMessage(String json) {
        if (wsServer != null) wsServer.broadcastMessage(json);
    }

    @Override
    public void broadcastLive(String json) {
        if (wsServer != null) wsServer.broadcastLive(json);
    }

    @Override
    public void broadcastBinary(byte[] data) {
        if (wsServer != null) wsServer.broadcastBinary(data);
    }

    @Override
    public int getClientCount() {
        return wsServer != null ? wsServer.getClientCount() : 0;
    }

    private void installPipelineHandler(Object pipeline, String handlerName, Object handler) throws Exception {
        Class<?> channelHandlerClass = Class.forName("io.netty.channel.ChannelHandler");
        Object packetHandlerContext = pipeline.getClass().getMethod("context", String.class).invoke(pipeline, "packet_handler");
        if (packetHandlerContext != null) {
            pipeline.getClass()
                    .getMethod("addBefore", String.class, String.class, channelHandlerClass)
                    .invoke(pipeline, "packet_handler", handlerName, handler);
            return;
        }
        pipeline.getClass()
                .getMethod("addLast", String.class, channelHandlerClass)
                .invoke(pipeline, handlerName, handler);
    }

    public void setAnchorPlayer(Player player) {
        // Remove packet interceptors from old anchor before switching
        if (running && isActiveTrackedPlayer(anchorPlayer)) {
            removePositionInterceptor(anchorPlayer);
            removeSoundInterceptor(anchorPlayer);
            removeOverlayInterceptor(anchorPlayer);
            removeBlockEventInterceptor(anchorPlayer);
            removeParticleInterceptor(anchorPlayer);
            removeContainerInterceptor(anchorPlayer);
            removeBlockBreakInterceptor(anchorPlayer);
        }
        this.anchorPlayer = player;
        anchorEntityId = player != null ? player.getEntityId() : Integer.MIN_VALUE;
        anchorPositionMirror = null;
        lastPositionInterceptBroadcastAt = 0L;
        lastServerCorrectionKey = "";
        lastServerCorrectionAt = 0L;
        // Reset change tracking
        lastInventoryHash = "";
        lastFullInventoryHash = "";
        lastEquipmentHash = "";
        lastHealth = -1;
        lastFood = -1;
        lastExp = -1;
        lastLevel = -1;
        trackedChunkViewportSignature = "";
        if (player == null) {
            for (Long existingKey : new ArrayList<>(trackedChunkKeys)) {
                trackedChunkKeys.remove(existingKey);
                sendChunkUnload(chunkXFromKey(existingKey), chunkZFromKey(existingKey), "anchor-cleared");
            }
            trackedChunkWorldId = "";
        }
        // Send chunks + player state around new anchor
        if (running && player != null) {
            sendChunksAroundPlayer(player);
            scheduleTrackedChunksShmRefresh(player, 2L, "anchor-switch");
            sendSkinWithRetries(player);
            sendInventoryState(player);
            sendEquipment(player);
            broadcastOverlayMessages(syncScoreboardStateFromPlayer(player));
            broadcastOverlayMessages(syncBossBarsFromPlayer(player));
            broadcastTimeNow(player.getWorld());
            broadcastWeatherNow(player.getWorld());
            // Install packet interceptors on new anchor
            syncPositionMirrorFromPlayer(player, "anchor-switch");
            installPositionInterceptor(player);
            installSoundInterceptor(player);
            installOverlayInterceptor(player);
            installBlockEventInterceptor(player);
            installParticleInterceptor(player);
            installContainerInterceptor(player);
            installBlockBreakInterceptor(player);
            installEntityRemoveInterceptor(player);
        }
    }

    private static long chunkKey(int x, int z) {
        return ((long) x << 32) | (z & 0xFFFFFFFFL);
    }

    private static int chunkXFromKey(long key) {
        return (int) (key >> 32);
    }

    private static int chunkZFromKey(long key) {
        return (int) key;
    }
}
