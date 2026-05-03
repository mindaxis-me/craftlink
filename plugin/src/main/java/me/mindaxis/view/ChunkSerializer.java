package me.mindaxis.view;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.bukkit.Chunk;
import org.bukkit.ChunkSnapshot;
import org.bukkit.Material;
import org.bukkit.block.Banner;
import org.bukkit.block.Biome;
import org.bukkit.block.BlockState;
import org.bukkit.block.Sign;
import org.bukkit.block.Skull;
import org.bukkit.block.banner.Pattern;
import org.bukkit.block.data.BlockData;
import org.bukkit.block.sign.Side;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Serializes chunk data for the CraftLink viewer pipeline.
 *
 * Primary serialization paths:
 * - {@link #serializeFullChunkDump} — prismarine-chunk dump() compatible binary frame
 * - {@link #serializeToShm} — SHM file per chunk column for bot JS pathfinder
 * - {@link #serializeHeightmap} — JSON heightmap message
 * - {@link #serializeBlockEntities} — JSON block-entity message
 *
 * When running on Paper, NMS stateId resolution is used (via reflection) so the
 * viewer can interpret block states without name-based translation.
 */
public class ChunkSerializer {

    private static final int SECTION_BLOCK_COUNT = 4096;
    static final int LIGHT_NIBBLE_ARRAY_SIZE = SECTION_BLOCK_COUNT / 2;

    private static volatile boolean stateIdResolverInitialized = false;
    private static volatile boolean stateIdAvailable = false;
    private static Method getStateMethod;      // CraftBlockData.getState() -> NMS BlockState
    private static Method getIdMethod;          // Block.BLOCK_STATE_REGISTRY.getId(BlockState)
    private static Object blockStateRegistry;   // Block.BLOCK_STATE_REGISTRY instance

    // Cache: BlockData.getAsString() -> stateId (avoids repeated reflection)
    private static final ConcurrentHashMap<String, Integer> stateIdCache = new ConcurrentHashMap<>();
    private static Logger pluginLogger;
    private static volatile StateIdProvider stateIdProviderOverride;

    @FunctionalInterface
    interface StateIdProvider {
        int resolve(BlockData blockData) throws Exception;
    }

    /**
     * Initialize the stateId resolver using reflection.
     * Call once during plugin enable. Safe to call multiple times.
     */
    public static void initStateIdResolver(Logger logger) {
        if (stateIdResolverInitialized) return;
        stateIdResolverInitialized = true;
        pluginLogger = logger;

        try {
            // Paper 1.20.5+ uses Mojang-mapped names directly (no relocation)
            // CraftBlockData wraps net.minecraft.world.level.block.state.BlockState
            Class<?> craftBlockDataClass = Class.forName("org.bukkit.craftbukkit.block.data.CraftBlockData");
            getStateMethod = craftBlockDataClass.getMethod("getState");

            // net.minecraft.world.level.block.Block has a static BLOCK_STATE_REGISTRY field
            Class<?> blockClass = Class.forName("net.minecraft.world.level.block.Block");
            java.lang.reflect.Field registryField = blockClass.getDeclaredField("BLOCK_STATE_REGISTRY");
            registryField.setAccessible(true);
            blockStateRegistry = registryField.get(null);

            // The registry is an IdMapper<BlockState> with getId(T) method
            getIdMethod = blockStateRegistry.getClass().getMethod("getId", Object.class);

            stateIdAvailable = true;
            logger.info("[ChunkSerializer] NMS stateId resolver initialized successfully");
        } catch (Exception e) {
            stateIdAvailable = false;
            logger.warning("[ChunkSerializer] NMS stateId resolver not available: " + e.getMessage());
            logger.warning("[ChunkSerializer] Falling back to name-only palette entries");
        }
    }

    /**
     * Get the Java protocol stateId for a BlockData, or -1 if unavailable.
     */
    public static int getStateId(BlockData blockData) {
        String key = blockData.getAsString();
        Integer cached = stateIdCache.get(key);
        if (cached != null) return cached;

        StateIdProvider override = stateIdProviderOverride;
        try {
            final int id;
            if (override != null) {
                id = override.resolve(blockData);
            } else {
                if (!stateIdAvailable) return -1;
                Object nmsState = getStateMethod.invoke(blockData);
                id = (int) getIdMethod.invoke(blockStateRegistry, nmsState);
            }
            if (id >= 0) {
                stateIdCache.put(key, id);
            }
            return id;
        } catch (Exception e) {
            return -1;
        }
    }

    // Per-section payload sizes (used by SHM serialization)
    public static final int BINARY_STATE_PAYLOAD_SIZE = SECTION_BLOCK_COUNT * 4;
    public static final int BINARY_LIGHT_PAYLOAD_SIZE = LIGHT_NIBBLE_ARRAY_SIZE * 2;
    public static final byte BINARY_TYPE_FULL_CHUNK_DUMP = 0x03;
    public static final int FULL_CHUNK_DUMP_HEADER_SIZE = 9;
    private static final int PRISMARINE_MIN_Y = -64;
    private static final int PRISMARINE_MAX_Y = 320;
    private static final int PRISMARINE_SECTION_COUNT = (PRISMARINE_MAX_Y - PRISMARINE_MIN_Y) / 16;
    private static final int PRISMARINE_BIOME_SAMPLE_COUNT = 64;
    private static final int PRISMARINE_MIN_BLOCK_BITS = 4;
    private static final int PRISMARINE_MAX_BLOCK_PALETTE_BITS = 8;
    private static final int PRISMARINE_MIN_BIOME_BITS = 1;
    private static final int PRISMARINE_MAX_BIOME_PALETTE_BITS = 3;
    // Matches the pinned viewer runtime (minecraft-data / prismarine-chunk 1.21.11+).
    private static final int PRISMARINE_DEFAULT_BIOME_ID = 40;
    private static final Map<String, Integer> PRISMARINE_BIOME_IDS = Map.ofEntries(
            Map.entry("badlands", 0),
            Map.entry("bamboo_jungle", 1),
            Map.entry("basalt_deltas", 2),
            Map.entry("beach", 3),
            Map.entry("birch_forest", 4),
            Map.entry("cherry_grove", 5),
            Map.entry("cold_ocean", 6),
            Map.entry("crimson_forest", 7),
            Map.entry("dark_forest", 8),
            Map.entry("deep_cold_ocean", 9),
            Map.entry("deep_dark", 10),
            Map.entry("deep_frozen_ocean", 11),
            Map.entry("deep_lukewarm_ocean", 12),
            Map.entry("deep_ocean", 13),
            Map.entry("desert", 14),
            Map.entry("dripstone_caves", 15),
            Map.entry("end_barrens", 16),
            Map.entry("end_highlands", 17),
            Map.entry("end_midlands", 18),
            Map.entry("eroded_badlands", 19),
            Map.entry("flower_forest", 20),
            Map.entry("forest", 21),
            Map.entry("frozen_ocean", 22),
            Map.entry("frozen_peaks", 23),
            Map.entry("frozen_river", 24),
            Map.entry("grove", 25),
            Map.entry("ice_spikes", 26),
            Map.entry("jagged_peaks", 27),
            Map.entry("jungle", 28),
            Map.entry("lukewarm_ocean", 29),
            Map.entry("lush_caves", 30),
            Map.entry("mangrove_swamp", 31),
            Map.entry("meadow", 32),
            Map.entry("mushroom_fields", 33),
            Map.entry("nether_wastes", 34),
            Map.entry("ocean", 35),
            Map.entry("old_growth_birch_forest", 36),
            Map.entry("old_growth_pine_taiga", 37),
            Map.entry("old_growth_spruce_taiga", 38),
            Map.entry("pale_garden", 39),
            Map.entry("plains", 40),
            Map.entry("river", 41),
            Map.entry("savanna", 42),
            Map.entry("savanna_plateau", 43),
            Map.entry("small_end_islands", 44),
            Map.entry("snowy_beach", 45),
            Map.entry("snowy_plains", 46),
            Map.entry("snowy_slopes", 47),
            Map.entry("snowy_taiga", 48),
            Map.entry("soul_sand_valley", 49),
            Map.entry("sparse_jungle", 50),
            Map.entry("stony_peaks", 51),
            Map.entry("stony_shore", 52),
            Map.entry("sunflower_plains", 53),
            Map.entry("swamp", 54),
            Map.entry("taiga", 55),
            Map.entry("the_end", 56),
            Map.entry("the_void", 57),
            Map.entry("warm_ocean", 58),
            Map.entry("warped_forest", 59),
            Map.entry("windswept_forest", 60),
            Map.entry("windswept_gravelly_hills", 61),
            Map.entry("windswept_hills", 62),
            Map.entry("windswept_savanna", 63),
            Map.entry("wooded_badlands", 64)
    );


    /**
     * Serialize a full chunk column as a single prismarine-chunk dump() compatible frame.
     * Frame format:
     *   byte 0:    type = 0x03 (full chunk dump)
     *   bytes 1-4: chunkX (int32 LE)
     *   bytes 5-8: chunkZ (int32 LE)
     *   bytes 9..: raw prismarine-chunk dump bytes
     */
    public static byte[] serializeFullChunkDump(ChunkSnapshot snapshot) {
        if (!stateIdAvailable) return null;

        byte[] dump = serializePrismarineChunkDump(snapshot);
        ByteBuffer buf = ByteBuffer.allocate(FULL_CHUNK_DUMP_HEADER_SIZE + dump.length).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(BINARY_TYPE_FULL_CHUNK_DUMP);
        buf.putInt(snapshot.getX());
        buf.putInt(snapshot.getZ());
        buf.put(dump);
        return buf.array();
    }

    private static byte[] serializePrismarineChunkDump(ChunkSnapshot snapshot) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(PRISMARINE_SECTION_COUNT * 512);
        for (int sectionY = PRISMARINE_MIN_Y >> 4; sectionY < (PRISMARINE_MAX_Y >> 4); sectionY++) {
            PrismarineSectionData sectionData = scanPrismarineSectionData(snapshot, sectionY);
            writeShortBE(out, sectionData.solidBlockCount);
            writePrismarinePaletteContainer(
                    out,
                    sectionData.stateIds,
                    PRISMARINE_MIN_BLOCK_BITS,
                    PRISMARINE_MAX_BLOCK_PALETTE_BITS
            );
            writePrismarinePaletteContainer(
                    out,
                    scanPrismarineBiomeIds(snapshot, sectionY),
                    PRISMARINE_MIN_BIOME_BITS,
                    PRISMARINE_MAX_BIOME_PALETTE_BITS
            );
        }
        return out.toByteArray();
    }

    private static PrismarineSectionData scanPrismarineSectionData(ChunkSnapshot snapshot, int sectionY) {
        int baseY = sectionY * 16;
        int[] stateIds = new int[SECTION_BLOCK_COUNT];
        int solidBlockCount = 0;
        int firstValue = Integer.MIN_VALUE;
        boolean singleValue = true;

        for (int localY = 0; localY < 16; localY++) {
            int blockY = baseY + localY;
            for (int localZ = 0; localZ < 16; localZ++) {
                for (int localX = 0; localX < 16; localX++) {
                    int stateId = 0;
                    try {
                        BlockData blockData = snapshot.getBlockData(localX, blockY, localZ);
                        if (blockData != null) {
                            int sid = getStateId(blockData);
                            if (sid >= 0) {
                                stateId = sid;
                            }
                        }
                    } catch (Exception ignored) {
                    }

                    int index = (localY << 8) | (localZ << 4) | localX;
                    stateIds[index] = stateId;
                    if (stateId != 0) solidBlockCount++;
                    if (firstValue == Integer.MIN_VALUE) {
                        firstValue = stateId;
                    } else if (stateId != firstValue) {
                        singleValue = false;
                    }
                }
            }
        }

        return new PrismarineSectionData(stateIds, solidBlockCount, firstValue == Integer.MIN_VALUE ? 0 : firstValue, singleValue);
    }

    private static int[] scanPrismarineBiomeIds(ChunkSnapshot snapshot, int sectionY) {
        int baseY = sectionY * 16;
        int[] biomeIds = new int[PRISMARINE_BIOME_SAMPLE_COUNT];
        for (int localBiomeY = 0; localBiomeY < 4; localBiomeY++) {
            int blockY = baseY + (localBiomeY << 2);
            for (int localBiomeZ = 0; localBiomeZ < 4; localBiomeZ++) {
                for (int localBiomeX = 0; localBiomeX < 4; localBiomeX++) {
                    String biomeName;
                    try {
                        biomeName = normalizeBiomeName(snapshot.getBiome(localBiomeX << 2, blockY, localBiomeZ << 2));
                    } catch (Exception ignored) {
                        biomeName = "minecraft:plains";
                    }
                    int index = (localBiomeY << 4) | (localBiomeZ << 2) | localBiomeX;
                    biomeIds[index] = resolvePrismarineBiomeId(biomeName);
                }
            }
        }
        return biomeIds;
    }

    private static void writePrismarinePaletteContainer(
            ByteArrayOutputStream out,
            int[] values,
            int minBitsPerValue,
            int maxPaletteBits
    ) {
        if (values.length == 0) {
            out.write(0);
            writeVarInt(out, 0);
            return;
        }

        int firstValue = values[0];
        boolean singleValue = true;
        int maxValue = firstValue;
        List<Integer> palette = new ArrayList<>();
        Map<Integer, Integer> paletteIndexes = new HashMap<>();
        int[] paletteData = new int[values.length];
        int maxPaletteSize = 1 << maxPaletteBits;
        boolean useIndirectPalette = true;

        palette.add(firstValue);
        paletteIndexes.put(firstValue, 0);
        paletteData[0] = 0;

        for (int i = 1; i < values.length; i++) {
            int value = values[i];
            if (value != firstValue) singleValue = false;
            if (value > maxValue) maxValue = value;
            if (!useIndirectPalette) continue;

            Integer paletteIndex = paletteIndexes.get(value);
            if (paletteIndex == null) {
                if (palette.size() >= maxPaletteSize) {
                    useIndirectPalette = false;
                    continue;
                }
                paletteIndex = palette.size();
                palette.add(value);
                paletteIndexes.put(value, paletteIndex);
            }
            paletteData[i] = paletteIndex;
        }

        if (singleValue) {
            out.write(0);
            writeVarInt(out, firstValue);
            return;
        }

        if (useIndirectPalette) {
            int bitsPerValue = Math.max(minBitsPerValue, neededBits(palette.size() - 1));
            out.write(bitsPerValue);
            writeVarInt(out, palette.size());
            for (int paletteValue : palette) {
                writeVarInt(out, paletteValue);
            }
            writePackedValues(out, paletteData, bitsPerValue);
            return;
        }

        int bitsPerValue = Math.max(minBitsPerValue, neededBits(maxValue));
        out.write(bitsPerValue);
        writePackedValues(out, values, bitsPerValue);
    }

    private static int resolvePrismarineBiomeId(String biomeName) {
        if (biomeName == null || biomeName.isBlank()) {
            return PRISMARINE_DEFAULT_BIOME_ID;
        }
        String normalized = biomeName.startsWith("minecraft:")
                ? biomeName.substring("minecraft:".length())
                : biomeName;
        return PRISMARINE_BIOME_IDS.getOrDefault(normalized, PRISMARINE_DEFAULT_BIOME_ID);
    }

    private static int neededBits(int value) {
        if (value <= 0) return 0;
        return Integer.SIZE - Integer.numberOfLeadingZeros(value);
    }

    private static void writeShortBE(ByteArrayOutputStream out, int value) {
        out.write((value >>> 8) & 0xFF);
        out.write(value & 0xFF);
    }

    private static void writeVarInt(ByteArrayOutputStream out, int value) {
        int remaining = value;
        do {
            int next = remaining & 0x7F;
            remaining >>>= 7;
            if (remaining != 0) {
                next |= 0x80;
            }
            out.write(next);
        } while (remaining != 0);
    }

    private static void writePackedValues(ByteArrayOutputStream out, int[] values, int bitsPerValue) {
        int valuesPerLong = Math.max(1, 64 / bitsPerValue);
        long valueMask = (1L << bitsPerValue) - 1L;

        for (int baseIndex = 0; baseIndex < values.length; baseIndex += valuesPerLong) {
            long packed = 0L;
            for (int offset = 0; offset < valuesPerLong; offset++) {
                int valueIndex = baseIndex + offset;
                if (valueIndex >= values.length) break;
                long value = Integer.toUnsignedLong(values[valueIndex]) & valueMask;
                packed |= value << (offset * bitsPerValue);
            }
            writeLongBE(out, packed);
        }
    }

    private static void writeLongBE(ByteArrayOutputStream out, long value) {
        out.write((int) ((value >>> 56) & 0xFF));
        out.write((int) ((value >>> 48) & 0xFF));
        out.write((int) ((value >>> 40) & 0xFF));
        out.write((int) ((value >>> 32) & 0xFF));
        out.write((int) ((value >>> 24) & 0xFF));
        out.write((int) ((value >>> 16) & 0xFF));
        out.write((int) ((value >>> 8) & 0xFF));
        out.write((int) (value & 0xFF));
    }


    private static String normalizeBiomeName(Biome biome) {
        if (biome == null) {
            return "minecraft:plains";
        }
        try {
            return biome.getKey().toString();
        } catch (Exception e) {
            return "minecraft:plains";
        }
    }


    /**
     * Serialize a heightmap message for a chunk column.
     * Uses ChunkSnapshot.getHighestBlockYAt(x, z) which returns the WORLD_SURFACE
     * heightmap value (highest non-air block Y) for each (x, z) position.
     *
     * Returns a JSON string:
     * {"type":"heightmap","x":chunkX,"z":chunkZ,"heights":[64,65,63,...]}
     * where heights has 256 values indexed as x + z*16.
     *
     * @param snapshot the chunk snapshot
     * @return JSON string for the heightmap message
     */
    public static String serializeHeightmap(ChunkSnapshot snapshot) {
        int chunkX = snapshot.getX();
        int chunkZ = snapshot.getZ();

        JsonObject msg = new JsonObject();
        msg.addProperty("type", "heightmap");
        msg.addProperty("x", chunkX);
        msg.addProperty("z", chunkZ);

        JsonArray heights = new JsonArray();
        for (int z = 0; z < 16; z++) {
            for (int x = 0; x < 16; x++) {
                // getHighestBlockYAt returns the Y of the highest non-air block.
                // MC heightmap convention (WORLD_SURFACE) stores blockY + 1,
                // i.e. the Y position above the top block. Add 1 to match.
                heights.add(snapshot.getHighestBlockYAt(x, z) + 1);
            }
        }
        msg.add("heights", heights);

        return msg.toString();
    }

    /**
     * Serialize tracked block entity data for a chunk.
     * For full chunk syncs, callers can skip empty chunks by passing includeEmpty=false.
     * For diff updates, includeEmpty=true lets the bridge clear removed entries.
     */
    public static String serializeBlockEntities(Chunk chunk, boolean includeEmpty) {
        if (chunk == null) return null;

        JsonArray entities = new JsonArray();
        for (BlockState state : chunk.getTileEntities()) {
            JsonObject entity = serializeBlockEntity(state);
            if (entity != null) {
                entities.add(entity);
            }
        }

        if (!includeEmpty && entities.isEmpty()) {
            return null;
        }

        JsonObject msg = new JsonObject();
        msg.addProperty("type", "blockEntities");
        JsonArray chunkCoords = new JsonArray();
        chunkCoords.add(chunk.getX());
        chunkCoords.add(chunk.getZ());
        msg.add("chunk", chunkCoords);
        msg.add("entities", entities);
        return msg.toString();
    }

    static JsonObject serializeBlockEntity(BlockState state) {
        if (state == null) return null;

        String blockId = materialToBlockId(state.getType());
        if (!isTrackedBlockEntityId(blockId)) {
            return null;
        }

        JsonObject data = parseBlockDataProperties(state.getBlockData());

        if (state instanceof Sign sign) {
            addSignData(data, sign);
        } else if (state instanceof Banner banner) {
            addBannerData(data, banner);
        } else if (state instanceof Skull skull) {
            addSkullData(data, skull);
        }

        if (isBedId(blockId)) {
            data.addProperty("color", extractColorPrefix(blockId, "_bed", "red"));
        }
        if (isBannerId(blockId) && !data.has("baseColor")) {
            data.addProperty("baseColor", extractBannerBaseColor(blockId));
        }

        JsonObject entity = new JsonObject();
        entity.addProperty("x", state.getX());
        entity.addProperty("y", state.getY());
        entity.addProperty("z", state.getZ());
        entity.addProperty("id", blockId);
        entity.add("data", data);
        return entity;
    }

    private static void addSignData(JsonObject data, Sign sign) {
        addSignSideData(data, "front_text", sign.getSide(Side.FRONT));
        addSignSideData(data, "back_text", sign.getSide(Side.BACK));

        String[] legacyLines = sign.getLines();
        for (int i = 0; i < legacyLines.length && i < 4; i++) {
            data.addProperty("Text" + (i + 1), legacyLines[i]);
        }
    }

    private static void addSignSideData(JsonObject data, String key, org.bukkit.block.sign.SignSide side) {
        if (side == null) return;

        JsonObject text = new JsonObject();
        JsonArray messages = new JsonArray();
        String[] lines = side.getLines();
        for (int i = 0; i < 4; i++) {
            messages.add(i < lines.length ? lines[i] : "");
        }
        text.add("messages", messages);
        text.addProperty("color", side.getColor().name().toLowerCase(Locale.ROOT));
        text.addProperty("has_glowing_text", side.isGlowingText());
        data.add(key, text);
    }

    private static void addBannerData(JsonObject data, Banner banner) {
        data.addProperty("baseColor", banner.getBaseColor().name().toLowerCase(Locale.ROOT));

        JsonArray patterns = new JsonArray();
        for (Pattern pattern : banner.getPatterns()) {
            JsonObject patternJson = new JsonObject();
            patternJson.addProperty("pattern", pattern.getPattern().getIdentifier());
            patternJson.addProperty("color", pattern.getColor().name().toLowerCase(Locale.ROOT));
            patterns.add(patternJson);
        }
        data.add("patterns", patterns);
    }

    private static void addSkullData(JsonObject data, Skull skull) {
        if (skull.getSkullType() != null) {
            data.addProperty("SkullType", skull.getSkullType().name().toLowerCase(Locale.ROOT));
        }
        if (skull.getRotation() != null) {
            data.addProperty("rotationFace", skull.getRotation().name().toLowerCase(Locale.ROOT));
        }
    }

    private static JsonObject parseBlockDataProperties(BlockData blockData) {
        JsonObject props = new JsonObject();
        if (blockData == null) return props;

        String blockDataString = blockData.getAsString();
        int bracketStart = blockDataString.indexOf('[');
        if (bracketStart == -1 || !blockDataString.endsWith("]")) {
            return props;
        }

        String statesStr = blockDataString.substring(bracketStart + 1, blockDataString.length() - 1);
        if (statesStr.isEmpty()) return props;

        for (String pair : statesStr.split(",")) {
            String[] kv = pair.split("=", 2);
            if (kv.length != 2) continue;
            addJsonPrimitive(props, kv[0], kv[1]);
        }
        return props;
    }

    private static void addJsonPrimitive(JsonObject target, String key, String rawValue) {
        if ("true".equals(rawValue) || "false".equals(rawValue)) {
            target.addProperty(key, Boolean.parseBoolean(rawValue));
            return;
        }
        try {
            target.addProperty(key, Integer.parseInt(rawValue));
            return;
        } catch (NumberFormatException ignored) {
        }
        target.addProperty(key, rawValue);
    }

    private static String materialToBlockId(Material material) {
        if (material == null) return "minecraft:air";
        return material.getKey().toString();
    }

    static boolean isTrackedBlockEntityMaterial(Material material) {
        return isTrackedBlockEntityId(materialToBlockId(material));
    }

    private static boolean isTrackedBlockEntityId(String blockId) {
        return isChestId(blockId)
            || "minecraft:enchanting_table".equals(blockId)
            || isSignId(blockId)
            || isBannerId(blockId)
            || isSkullId(blockId)
            || isBedId(blockId)
            || isShulkerBoxId(blockId);
    }

    private static boolean isChestId(String blockId) {
        return "minecraft:chest".equals(blockId) || "minecraft:trapped_chest".equals(blockId);
    }

    private static boolean isSignId(String blockId) {
        return blockId.endsWith("_sign") || blockId.endsWith("_hanging_sign");
    }

    private static boolean isBannerId(String blockId) {
        return blockId.endsWith("_banner");
    }

    private static boolean isSkullId(String blockId) {
        return blockId.endsWith("_head") || blockId.endsWith("_skull");
    }

    private static boolean isShulkerBoxId(String blockId) {
        return blockId.endsWith("shulker_box");
    }

    private static boolean isBedId(String blockId) {
        return blockId.endsWith("_bed");
    }

    private static String extractBannerBaseColor(String blockId) {
        if (blockId.endsWith("_wall_banner")) {
            return blockId.substring("minecraft:".length(), blockId.length() - "_wall_banner".length());
        }
        if (blockId.endsWith("_banner")) {
            return blockId.substring("minecraft:".length(), blockId.length() - "_banner".length());
        }
        return "white";
    }

    private static String extractColorPrefix(String blockId, String suffix, String fallback) {
        if (!blockId.startsWith("minecraft:") || !blockId.endsWith(suffix)) {
            return fallback;
        }
        String value = blockId.substring("minecraft:".length(), blockId.length() - suffix.length());
        return value.isEmpty() ? fallback : value;
    }

    // ---- Shared Memory (SHM) Chunk Writing ----

    private static final java.nio.file.Path DEFAULT_SHARED_DATA_DIR = java.nio.file.Paths.get(
            System.getProperty("java.io.tmpdir"),
            "craftlink",
            "chunks"
    );
    private static volatile java.nio.file.Path sharedDataDir = DEFAULT_SHARED_DATA_DIR;
    private static volatile boolean shmDirCreated = false;
    private static volatile java.nio.file.Path shmDirOverrideForTest;
    private static final ConcurrentHashMap<String, Object> shmChunkLocks = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicLong> shmMutationSequences =
            new ConcurrentHashMap<>();

    public enum ShmUpdateResult {
        UPDATED,
        NOOP,
        REWRITE_REQUIRED
    }

    /**
     * Serialize a chunk snapshot to {shared-data-root}/chunks/{cx}_{cz} as raw stateId + light arrays.
     *
     * Binary format (little-endian):
     *   4 bytes: number of non-air sections (int32)
     *   Per section:
     *     4 bytes: sectionY (int32)
     *     4096 x 4 bytes: stateIds (int32 LE)
     *     2048 bytes: sky light nibble array (4-bit per block)
     *     2048 bytes: block light nibble array (4-bit per block)
     *       Index order: for i in 0..4095, x=(i>>8)&0xF, z=(i>>4)&0xF, y=i&0xF (XZY)
     *
     * Also updates the manifest file atomically.
     *
     * @param snapshot the chunk snapshot
     * @param manifestEntries thread-safe set of all written chunk keys for manifest updates
     */
    public static void serializeToShm(ChunkSnapshot snapshot, Set<String> manifestEntries) {
        serializeToShm(snapshot, manifestEntries, reserveShmMutation(snapshot.getX(), snapshot.getZ()));
    }

    public static void serializeToShm(ChunkSnapshot snapshot, Set<String> manifestEntries, long mutationSequence) {
        if (!stateIdAvailable) return;

        int cx = snapshot.getX();
        int cz = snapshot.getZ();
        int minY = -64;
        int maxY = 320;
        String chunkKey = shmChunkKey(cx, cz);
        java.nio.file.Path shmDir = resolveShmDir();
        if (!isCurrentShmMutation(chunkKey, mutationSequence)) return;

        List<SectionBinaryData> sectionData = new ArrayList<>();

        for (int sectionY = minY >> 4; sectionY < (maxY >> 4); sectionY++) {
            SectionBinaryData data = scanSectionBinaryData(snapshot, sectionY);
            if (!data.allAir) {
                sectionData.add(data);
            }
        }

        if (sectionData.isEmpty()) {
            synchronized (shmChunkLock(chunkKey)) {
                if (!isCurrentShmMutation(chunkKey, mutationSequence)) return;
                try {
                    java.nio.file.Files.deleteIfExists(shmDir.resolve(chunkKey));
                } catch (Exception ignored) {
                }
                manifestEntries.remove(chunkKey);
                if (java.nio.file.Files.exists(shmDir)) {
                    updateManifest(manifestEntries);
                }
            }
            return;
        }

        // Ensure directory exists
        if (!shmDirCreated) {
            try {
                java.nio.file.Files.createDirectories(shmDir);
                if (shmDirOverrideForTest == null) {
                    shmDirCreated = true;
                }
            } catch (Exception e) {
                return; // Can't write
            }
        }

        // Build binary buffer
        int numSections = sectionData.size();
        int perSectionSize = 4 + BINARY_STATE_PAYLOAD_SIZE + BINARY_LIGHT_PAYLOAD_SIZE;
        int totalSize = 4 + numSections * perSectionSize;
        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        buf.putInt(numSections);
        for (int s = 0; s < numSections; s++) {
            SectionBinaryData data = sectionData.get(s);
            buf.putInt(data.sectionY);
            for (int i = 0; i < SECTION_BLOCK_COUNT; i++) {
                buf.putInt(data.stateIds[i]);
            }
            buf.put(data.skyLight);
            buf.put(data.blockLight);
        }

        // Write chunk file
        java.nio.file.Path chunkFile = shmDir.resolve(chunkKey);
        synchronized (shmChunkLock(chunkKey)) {
            if (!isCurrentShmMutation(chunkKey, mutationSequence)) return;
            try {
                java.nio.file.Files.write(chunkFile, buf.array());
            } catch (Exception e) {
                return;
            }

            // Update manifest atomically (write to tmp, rename)
            manifestEntries.add(chunkKey);
            updateManifest(manifestEntries);
        }
    }

    public static long reserveShmMutation(int chunkX, int chunkZ) {
        return shmMutationSequences
                .computeIfAbsent(shmChunkKey(chunkX, chunkZ), ignored -> new java.util.concurrent.atomic.AtomicLong())
                .incrementAndGet();
    }

    public static ShmUpdateResult updateShmBlock(int blockX, int blockY, int blockZ, int stateId) {
        return updateShmBlock(
                blockX,
                blockY,
                blockZ,
                stateId,
                reserveShmMutation(Math.floorDiv(blockX, 16), Math.floorDiv(blockZ, 16))
        );
    }

    static ShmUpdateResult updateShmBlock(int blockX, int blockY, int blockZ, int stateId, long mutationSequence) {
        if (stateId < 0) return ShmUpdateResult.NOOP;

        int chunkX = Math.floorDiv(blockX, 16);
        int chunkZ = Math.floorDiv(blockZ, 16);
        String chunkKey = shmChunkKey(chunkX, chunkZ);
        if (!isCurrentShmMutation(chunkKey, mutationSequence)) return ShmUpdateResult.NOOP;

        java.nio.file.Path chunkFile = resolveShmDir().resolve(chunkKey);
        if (!java.nio.file.Files.exists(chunkFile)) {
            return stateId == 0 ? ShmUpdateResult.NOOP : ShmUpdateResult.REWRITE_REQUIRED;
        }

        int sectionY = blockY >> 4;
        int localX = Math.floorMod(blockX, 16);
        int localY = blockY & 0xF;
        int localZ = Math.floorMod(blockZ, 16);
        int targetIndex = (localX << 8) | (localZ << 4) | localY;
        int perSectionSize = 4 + BINARY_STATE_PAYLOAD_SIZE + BINARY_LIGHT_PAYLOAD_SIZE;

        synchronized (shmChunkLock(chunkKey)) {
            if (!isCurrentShmMutation(chunkKey, mutationSequence)) return ShmUpdateResult.NOOP;
            try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(chunkFile.toFile(), "rw")) {
                if (raf.length() < 4) {
                    return stateId == 0 ? ShmUpdateResult.NOOP : ShmUpdateResult.REWRITE_REQUIRED;
                }

                int numSections = readLittleEndianInt(raf);
                if (numSections <= 0 || numSections > 24) {
                    return stateId == 0 ? ShmUpdateResult.NOOP : ShmUpdateResult.REWRITE_REQUIRED;
                }

                long offset = 4L;
                for (int sectionIndex = 0; sectionIndex < numSections; sectionIndex++) {
                    raf.seek(offset);
                    int currentSectionY = readLittleEndianInt(raf);
                    if (currentSectionY != sectionY) {
                        offset += perSectionSize;
                        continue;
                    }

                    long sectionStateOffset = offset + 4L;
                    long stateOffset = sectionStateOffset + (long) targetIndex * 4L;
                    raf.seek(stateOffset);
                    int currentStateId = readLittleEndianInt(raf);
                    if (currentStateId == stateId) {
                        return ShmUpdateResult.NOOP;
                    }
                    if (stateId == 0
                            && currentStateId != 0
                            && sectionWouldBecomeAllAir(raf, sectionStateOffset, targetIndex)) {
                        return ShmUpdateResult.REWRITE_REQUIRED;
                    }

                    raf.seek(stateOffset);
                    writeLittleEndianInt(raf, stateId);
                    return ShmUpdateResult.UPDATED;
                }
            } catch (Exception e) {
                return stateId == 0 ? ShmUpdateResult.NOOP : ShmUpdateResult.REWRITE_REQUIRED;
            }
        }

        return stateId == 0 ? ShmUpdateResult.NOOP : ShmUpdateResult.REWRITE_REQUIRED;
    }

    /**
     * Write the manifest file atomically (write tmp, rename).
     */
    private static void updateManifest(Set<String> manifestEntries) {
        try {
            String content = String.join("\n", manifestEntries) + "\n";
            java.nio.file.Path shmDir = resolveShmDir();
            java.nio.file.Path tmpFile = shmDir.resolve("_manifest.tmp");
            java.nio.file.Path manifestFile = shmDir.resolve("_manifest");
            java.nio.file.Files.writeString(tmpFile, content);
            java.nio.file.Files.move(tmpFile, manifestFile,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            // Non-critical — bot will fall back to readdir
        }
    }

    /**
     * Check if NMS stateId resolution is available.
     */
    public static boolean isStateIdAvailable() {
        return stateIdAvailable;
    }

    /**
     * Get the stateId cache size (for diagnostics).
     */
    public static int getStateIdCacheSize() {
        return stateIdCache.size();
    }

    static void setStateIdProvider(StateIdProvider provider) {
        stateIdProviderOverride = provider;
        stateIdAvailable = provider != null;
        stateIdCache.clear();
    }

    static void clearStateIdProvider() {
        stateIdProviderOverride = null;
        stateIdAvailable = getStateMethod != null && getIdMethod != null && blockStateRegistry != null;
        stateIdCache.clear();
    }

    static void setStateIdProviderOverrideForTest(StateIdProvider provider) {
        setStateIdProvider(provider);
    }

    static void clearStateIdProviderOverrideForTest() {
        clearStateIdProvider();
        stateIdCache.clear();
    }

    static void setShmDirOverrideForTest(java.nio.file.Path shmDir) {
        shmDirOverrideForTest = shmDir;
        shmDirCreated = false;
        shmChunkLocks.clear();
        shmMutationSequences.clear();
    }

    static void setSharedDataDir(java.nio.file.Path shmDir) {
        if (shmDir == null) return;
        sharedDataDir = shmDir;
        shmDirCreated = false;
        shmChunkLocks.clear();
        shmMutationSequences.clear();
    }

    static void clearShmDirOverrideForTest() {
        shmDirOverrideForTest = null;
        shmDirCreated = false;
        shmChunkLocks.clear();
        shmMutationSequences.clear();
    }

    private static SectionBinaryData scanSectionBinaryData(ChunkSnapshot snapshot, int sectionY) {
        int baseY = sectionY * 16;
        int[] stateIds = new int[SECTION_BLOCK_COUNT];
        byte[] skyLight = new byte[LIGHT_NIBBLE_ARRAY_SIZE];
        byte[] blockLight = new byte[LIGHT_NIBBLE_ARRAY_SIZE];
        boolean allAir = true;

        for (int i = 0; i < SECTION_BLOCK_COUNT; i++) {
            int x = (i >> 8) & 0xF;
            int z = (i >> 4) & 0xF;
            int y = i & 0xF;
            int blockY = baseY + y;

            BlockData blockData = null;
            try {
                blockData = snapshot.getBlockData(x, blockY, z);
            } catch (Exception ignored) {
            }

            if (blockData == null) {
                stateIds[i] = 0;
            } else {
                int sid = getStateId(blockData);
                if (sid < 0) {
                    stateIds[i] = 0;
                } else {
                    stateIds[i] = sid;
                    if (sid != 0) allAir = false;
                }
            }

            setNibble(skyLight, i, readLight(snapshot, x, blockY, z, true));
            setNibble(blockLight, i, readLight(snapshot, x, blockY, z, false));
        }

        return new SectionBinaryData(sectionY, stateIds, skyLight, blockLight, allAir);
    }

    private static int readLight(ChunkSnapshot snapshot, int x, int y, int z, boolean sky) {
        try {
            int light = sky ? snapshot.getBlockSkyLight(x, y, z) : snapshot.getBlockEmittedLight(x, y, z);
            return Math.max(0, Math.min(15, light));
        } catch (Exception ignored) {
            return 0;
        }
    }

    private static java.nio.file.Path resolveShmDir() {
        return shmDirOverrideForTest != null ? shmDirOverrideForTest : sharedDataDir;
    }

    private static Object shmChunkLock(String chunkKey) {
        return shmChunkLocks.computeIfAbsent(chunkKey, ignored -> new Object());
    }

    private static String shmChunkKey(int chunkX, int chunkZ) {
        return chunkX + "_" + chunkZ;
    }

    private static boolean isCurrentShmMutation(String chunkKey, long mutationSequence) {
        if (mutationSequence <= 0) return true;
        java.util.concurrent.atomic.AtomicLong current = shmMutationSequences.get(chunkKey);
        return current != null && current.get() == mutationSequence;
    }

    private static boolean sectionWouldBecomeAllAir(
            java.io.RandomAccessFile raf,
            long sectionStateOffset,
            int targetIndex
    ) throws java.io.IOException {
        byte[] stateBytes = new byte[BINARY_STATE_PAYLOAD_SIZE];
        raf.seek(sectionStateOffset);
        raf.readFully(stateBytes);
        ByteBuffer buf = ByteBuffer.wrap(stateBytes).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < SECTION_BLOCK_COUNT; i++) {
            int stateId = buf.getInt(i * 4);
            if (i != targetIndex && stateId != 0) {
                return false;
            }
        }
        return true;
    }

    private static int readLittleEndianInt(java.io.RandomAccessFile raf) throws java.io.IOException {
        return Integer.reverseBytes(raf.readInt());
    }

    private static void writeLittleEndianInt(java.io.RandomAccessFile raf, int value) throws java.io.IOException {
        raf.writeInt(Integer.reverseBytes(value));
    }

    private static void setNibble(byte[] target, int index, int value) {
        int byteIndex = index >> 1;
        int nibble = value & 0xF;
        if ((index & 1) == 0) {
            target[byteIndex] = (byte) ((target[byteIndex] & 0xF0) | nibble);
        } else {
            target[byteIndex] = (byte) ((target[byteIndex] & 0x0F) | (nibble << 4));
        }
    }

    private static class SectionBinaryData {
        final int sectionY;
        final int[] stateIds;
        final byte[] skyLight;
        final byte[] blockLight;
        final boolean allAir;

        SectionBinaryData(int sectionY, int[] stateIds, byte[] skyLight, byte[] blockLight, boolean allAir) {
            this.sectionY = sectionY;
            this.stateIds = stateIds;
            this.skyLight = skyLight;
            this.blockLight = blockLight;
            this.allAir = allAir;
        }
    }

    private static class PrismarineSectionData {
        final int[] stateIds;
        final int solidBlockCount;
        final int firstValue;
        final boolean singleValue;

        PrismarineSectionData(int[] stateIds, int solidBlockCount, int firstValue, boolean singleValue) {
            this.stateIds = stateIds;
            this.solidBlockCount = solidBlockCount;
            this.firstValue = firstValue;
            this.singleValue = singleValue;
        }
    }

}
