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

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Serializes a ChunkSnapshot into subchunk messages matching the Bedrock viewer
 * bridge format: palette + base64 indices per 16x16x16 section.
 *
 * When running on Paper, palette entries include "sid" (Java protocol stateId)
 * so the bridge can skip name-based translation entirely.
 *
 * Message format (per subchunk):
 * {
 *   "type": "subchunk",
 *   "x": chunkX, "y": sectionIndex, "z": chunkZ,
 *   "palette": [{"index": 0, "name": "minecraft:stone", "sid": 1, "states": {...}}],
 *   "indices": "<base64>"
 * }
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

    /**
     * Serialize all non-empty sections of a chunk snapshot into JSON subchunk messages.
     *
     * @param snapshot the chunk snapshot (must be obtained on the main thread)
     * @return list of JSON strings, one per non-empty subchunk
     */
    public static List<String> serialize(ChunkSnapshot snapshot) {
        List<String> messages = new ArrayList<>();
        int chunkX = snapshot.getX();
        int chunkZ = snapshot.getZ();

        // Paper/Spigot: world min height can be -64 (overworld) or 0 (nether/end)
        // Sections are indexed: sectionY = (blockY - minY) / 16
        // For overworld: minY = -64, sections from -4 to 19 (blockY -64 to 319)
        // We iterate using block Y coordinates and derive section index
        int minY = -64; // Overworld default; snapshot doesn't expose minY directly
        int maxY = 320;

        for (int sectionY = minY >> 4; sectionY < (maxY >> 4); sectionY++) {
            String json = serializeSection(snapshot, chunkX, chunkZ, sectionY);
            if (json != null) {
                messages.add(json);
            }
        }
        return messages;
    }

    /**
     * Serialize per-section 4x4x4 biome data for a chunk snapshot.
     *
     * Message format (per section):
     * {
     *   "type": "biomes",
     *   "x": chunkX, "y": sectionIndex, "z": chunkZ,
     *   "biomes": [{"index": 0, "name": "minecraft:plains"}],
     *   "biomeIndices": "<base64>"
     * }
     *
     * Indices are encoded in XZY order at 4x4x4 biome resolution:
     *   index = x + z*4 + y*16
     */
    public static List<String> serializeBiomes(ChunkSnapshot snapshot) {
        List<String> messages = new ArrayList<>();
        int chunkX = snapshot.getX();
        int chunkZ = snapshot.getZ();
        int minY = -64;
        int maxY = 320;

        for (int sectionY = minY >> 4; sectionY < (maxY >> 4); sectionY++) {
            String json = serializeBiomeSection(snapshot, chunkX, chunkZ, sectionY);
            if (json != null) {
                messages.add(json);
            }
        }
        return messages;
    }

    /**
     * Binary header format (13 bytes):
     *   byte 0:    type = 0x01 (subchunk binary)
     *   bytes 1-4: chunkX (int32 LE)
     *   bytes 5-8: chunkZ (int32 LE)
     *   bytes 9-12: sectionY (int32 LE)
     * Followed by:
     *   4096 × int32 LE stateIds (16,384 bytes)
     *   2048 bytes sky light nibble array (4-bit per block)
     *   2048 bytes block light nibble array (4-bit per block)
     * Total per section: 13 + 16384 + 2048 + 2048 = 20,493 bytes.
     *
     * Index order: for index i, x=(i>>8)&0xF, z=(i>>4)&0xF, y=i&0xF (XZY).
     */
    public static final int BINARY_HEADER_SIZE = 13;
    public static final int BINARY_STATE_PAYLOAD_SIZE = SECTION_BLOCK_COUNT * 4;
    public static final int BINARY_LIGHT_PAYLOAD_SIZE = LIGHT_NIBBLE_ARRAY_SIZE * 2;
    public static final int BINARY_PAYLOAD_SIZE = BINARY_STATE_PAYLOAD_SIZE + BINARY_LIGHT_PAYLOAD_SIZE;
    public static final byte BINARY_TYPE_SUBCHUNK = 0x01;
    public static final byte BINARY_TYPE_SUBCHUNK_GZIP = 0x02;

    /**
     * Serialize all non-empty sections of a chunk snapshot into binary subchunk messages.
     * Each message is a byte[] with a 13-byte header + 16,384 bytes of stateId data.
     * Requires NMS stateId resolution to be available.
     *
     * @param snapshot the chunk snapshot (must be obtained on the main thread)
     * @return list of byte arrays, one per non-empty subchunk; empty list if stateId not available
     */
    public static List<byte[]> serializeBinary(ChunkSnapshot snapshot) {
        if (!stateIdAvailable) return Collections.emptyList();

        List<byte[]> messages = new ArrayList<>();
        int chunkX = snapshot.getX();
        int chunkZ = snapshot.getZ();

        int minY = -64;
        int maxY = 320;

        for (int sectionY = minY >> 4; sectionY < (maxY >> 4); sectionY++) {
            byte[] binary = serializeSectionBinary(snapshot, chunkX, chunkZ, sectionY);
            if (binary != null) {
                messages.add(binary);
            }
        }
        return messages;
    }

    /**
     * Serialize a single 16x16x16 section as binary stateIds.
     * Returns null if the section is entirely air or stateId resolution fails.
     */
    private static byte[] serializeSectionBinary(ChunkSnapshot snapshot, int chunkX, int chunkZ, int sectionY) {
        SectionBinaryData sectionData = scanSectionBinaryData(snapshot, sectionY);
        if (sectionData.allAir) return null;

        // Build binary message: header + stateIds + light arrays
        ByteBuffer buf = ByteBuffer.allocate(BINARY_HEADER_SIZE + BINARY_PAYLOAD_SIZE);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        // Header
        buf.put(BINARY_TYPE_SUBCHUNK);
        buf.putInt(chunkX);
        buf.putInt(chunkZ);
        buf.putInt(sectionY);

        for (int i = 0; i < SECTION_BLOCK_COUNT; i++) {
            buf.putInt(sectionData.stateIds[i]);
        }
        buf.put(sectionData.skyLight);
        buf.put(sectionData.blockLight);

        return buf.array();
    }

    /**
     * Wrap a raw binary subchunk frame in a gzip-compressed transport frame.
     * Output format:
     *   byte 0: 0x02 (gzip-compressed subchunk)
     *   bytes 1..N: gzip payload of the original 0x01 frame
     */
    public static byte[] gzipBinaryFrame(byte[] rawFrame) {
        if (rawFrame == null || rawFrame.length == 0) {
            return rawFrame;
        }
        try {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            out.write(BINARY_TYPE_SUBCHUNK_GZIP);
            try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
                gzip.write(rawFrame);
            }
            return out.toByteArray();
        } catch (java.io.IOException e) {
            return rawFrame;
        }
    }

    /**
     * Inflate a gzip-compressed subchunk frame back to the original raw 0x01 frame.
     * If the frame is not gzip-wrapped, it is returned unchanged.
     */
    public static byte[] inflateBinaryFrame(byte[] frame) throws java.io.IOException {
        if (frame == null || frame.length == 0 || frame[0] != BINARY_TYPE_SUBCHUNK_GZIP) {
            return frame;
        }
        try (GZIPInputStream gzip = new GZIPInputStream(new java.io.ByteArrayInputStream(frame, 1, frame.length - 1));
             java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream()) {
            gzip.transferTo(out);
            return out.toByteArray();
        }
    }

    /**
     * Serialize a single 16x16x16 section.
     * Returns null if the section is entirely air.
     */
    private static String serializeSection(ChunkSnapshot snapshot, int chunkX, int chunkZ, int sectionY) {
        int baseY = sectionY * 16;

        // Build palette and indices in XZY order (matching Bedrock bridge)
        Map<String, Integer> paletteMap = new LinkedHashMap<>();
        List<PaletteEntry> paletteList = new ArrayList<>();
        int[] indices = new int[4096];
        boolean allAir = true;

        for (int i = 0; i < 4096; i++) {
            // XZY order matching bridge decodeSubChunkIndex: x=(i>>8), z=(i>>4), y=i&0xF
            int x = (i >> 8) & 0xF;
            int z = (i >> 4) & 0xF;
            int y = i & 0xF;
            int blockY = baseY + y;

            BlockData blockData;
            try {
                blockData = snapshot.getBlockData(x, blockY, z);
            } catch (Exception e) {
                // Out of bounds for this snapshot (e.g. nether ceiling)
                blockData = null;
            }

            String blockDataStr = blockData != null ? blockData.getAsString() : "minecraft:air";

            if (!"minecraft:air".equals(blockDataStr)) {
                allAir = false;
            }

            Integer paletteIndex = paletteMap.get(blockDataStr);
            if (paletteIndex == null) {
                paletteIndex = paletteMap.size();
                paletteMap.put(blockDataStr, paletteIndex);

                // Get stateId if available
                int sid = -1;
                if (blockData != null && stateIdAvailable) {
                    sid = getStateId(blockData);
                }
                paletteList.add(parseBlockData(blockDataStr, paletteIndex, sid));
            }
            indices[i] = paletteIndex;
        }

        if (allAir) {
            return null;
        }

        // Build JSON
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "subchunk");
        msg.addProperty("x", chunkX);
        msg.addProperty("y", sectionY);
        msg.addProperty("z", chunkZ);

        JsonArray paletteArray = new JsonArray();
        for (PaletteEntry entry : paletteList) {
            paletteArray.add(entry.toJson());
        }
        msg.add("palette", paletteArray);
        msg.addProperty("indices", indicesToBase64(indices, paletteList.size()));

        return msg.toString();
    }

    /**
     * Serialize a single 16x16x16 section's biome lattice (4x4x4 cells).
     * Returns null if the sampled section is entirely out of bounds.
     */
    private static String serializeBiomeSection(ChunkSnapshot snapshot, int chunkX, int chunkZ, int sectionY) {
        int baseY = sectionY * 16;

        Map<String, Integer> paletteMap = new LinkedHashMap<>();
        List<BiomePaletteEntry> paletteList = new ArrayList<>();
        int[] indices = new int[64];
        boolean hasBiomeData = false;

        for (int i = 0; i < 64; i++) {
            int x = i & 0x3;
            int z = (i >> 2) & 0x3;
            int y = (i >> 4) & 0x3;

            int blockX = x << 2;
            int blockY = baseY + (y << 2);
            int blockZ = z << 2;

            Biome biome;
            try {
                biome = snapshot.getBiome(blockX, blockY, blockZ);
                hasBiomeData = true;
            } catch (Exception e) {
                biome = null;
            }

            String biomeName = normalizeBiomeName(biome);
            Integer paletteIndex = paletteMap.get(biomeName);
            if (paletteIndex == null) {
                paletteIndex = paletteMap.size();
                paletteMap.put(biomeName, paletteIndex);
                paletteList.add(new BiomePaletteEntry(paletteIndex, biomeName));
            }
            indices[i] = paletteIndex;
        }

        if (!hasBiomeData) {
            return null;
        }

        JsonObject msg = new JsonObject();
        msg.addProperty("type", "biomes");
        msg.addProperty("x", chunkX);
        msg.addProperty("y", sectionY);
        msg.addProperty("z", chunkZ);

        JsonArray biomeArray = new JsonArray();
        for (BiomePaletteEntry entry : paletteList) {
            biomeArray.add(entry.toJson());
        }
        msg.add("biomes", biomeArray);
        msg.addProperty("biomeIndices", indicesToBase64(indices, paletteList.size()));

        return msg.toString();
    }

    /**
     * Parse a block data string like "minecraft:oak_stairs[facing=north,half=bottom]"
     * into a PaletteEntry with name, states, and optional stateId.
     */
    private static PaletteEntry parseBlockData(String blockData, int index, int stateId) {
        int bracketStart = blockData.indexOf('[');
        if (bracketStart == -1) {
            return new PaletteEntry(index, blockData, Collections.emptyMap(), stateId);
        }

        String name = blockData.substring(0, bracketStart);
        String statesStr = blockData.substring(bracketStart + 1, blockData.length() - 1);
        Map<String, String> states = new LinkedHashMap<>();
        for (String pair : statesStr.split(",")) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                states.put(kv[0], kv[1]);
            }
        }
        return new PaletteEntry(index, name, states, stateId);
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
     * Encode indices as base64 (uint8 if palette <= 256, uint16 LE otherwise).
     * Matches the Go bot's indicesToBase64 function.
     */
    private static String indicesToBase64(int[] indices, int paletteLen) {
        byte[] buf;
        if (paletteLen <= 256) {
            buf = new byte[indices.length];
            for (int i = 0; i < indices.length; i++) {
                buf[i] = (byte) indices[i];
            }
        } else {
            buf = new byte[indices.length * 2];
            for (int i = 0; i < indices.length; i++) {
                buf[i * 2] = (byte) (indices[i] & 0xFF);
                buf[i * 2 + 1] = (byte) ((indices[i] >> 8) & 0xFF);
            }
        }
        return Base64.getEncoder().encodeToString(buf);
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

    private static boolean isTrackedBlockEntityId(String blockId) {
        return isChestId(blockId)
            || "minecraft:enchanting_table".equals(blockId)
            || isSignId(blockId)
            || isBannerId(blockId)
            || isSkullId(blockId)
            || isBedId(blockId);
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

    private static final java.nio.file.Path SHM_DIR = java.nio.file.Paths.get("/dev/shm/mindaxis-chunks");
    private static volatile boolean shmDirCreated = false;

    /**
     * Serialize a chunk snapshot to /dev/shm/mindaxis-chunks/{cx}_{cz} as raw stateId + light arrays.
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
        if (!stateIdAvailable) return;

        int cx = snapshot.getX();
        int cz = snapshot.getZ();
        int minY = -64;
        int maxY = 320;

        List<SectionBinaryData> sectionData = new ArrayList<>();

        for (int sectionY = minY >> 4; sectionY < (maxY >> 4); sectionY++) {
            SectionBinaryData data = scanSectionBinaryData(snapshot, sectionY);
            if (!data.allAir) {
                sectionData.add(data);
            }
        }

        if (sectionData.isEmpty()) return;

        // Ensure directory exists
        if (!shmDirCreated) {
            try {
                java.nio.file.Files.createDirectories(SHM_DIR);
                shmDirCreated = true;
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
        String chunkKey = cx + "_" + cz;
        java.nio.file.Path chunkFile = SHM_DIR.resolve(chunkKey);
        try {
            java.nio.file.Files.write(chunkFile, buf.array());
        } catch (Exception e) {
            return;
        }

        // Update manifest atomically (write to tmp, rename)
        manifestEntries.add(chunkKey);
        updateManifest(manifestEntries);
    }

    /**
     * Write the manifest file atomically (write tmp, rename).
     */
    private static void updateManifest(Set<String> manifestEntries) {
        try {
            String content = String.join("\n", manifestEntries) + "\n";
            java.nio.file.Path tmpFile = SHM_DIR.resolve("_manifest.tmp");
            java.nio.file.Path manifestFile = SHM_DIR.resolve("_manifest");
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

    static void setStateIdProviderOverrideForTest(StateIdProvider provider) {
        stateIdProviderOverride = provider;
        stateIdAvailable = provider != null;
        stateIdCache.clear();
    }

    static void clearStateIdProviderOverrideForTest() {
        stateIdProviderOverride = null;
        stateIdCache.clear();
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

    private static void setNibble(byte[] target, int index, int value) {
        int byteIndex = index >> 1;
        int nibble = value & 0xF;
        if ((index & 1) == 0) {
            target[byteIndex] = (byte) ((target[byteIndex] & 0xF0) | nibble);
        } else {
            target[byteIndex] = (byte) ((target[byteIndex] & 0x0F) | (nibble << 4));
        }
    }

    /**
     * A single palette entry: index + block name + optional states + optional stateId.
     */
    private static class PaletteEntry {
        final int index;
        final String name;
        final Map<String, String> states;
        final int stateId; // -1 if not available

        PaletteEntry(int index, String name, Map<String, String> states, int stateId) {
            this.index = index;
            this.name = name;
            this.states = states;
            this.stateId = stateId;
        }

        JsonObject toJson() {
            JsonObject obj = new JsonObject();
            obj.addProperty("index", index);
            obj.addProperty("name", name);
            if (stateId >= 0) {
                obj.addProperty("sid", stateId);
            }
            if (!states.isEmpty()) {
                JsonObject statesObj = new JsonObject();
                for (Map.Entry<String, String> e : states.entrySet()) {
                    statesObj.addProperty(e.getKey(), e.getValue());
                }
                obj.add("states", statesObj);
            }
            return obj;
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

    private static class BiomePaletteEntry {
        final int index;
        final String name;

        BiomePaletteEntry(int index, String name) {
            this.index = index;
            this.name = name;
        }

        JsonObject toJson() {
            JsonObject obj = new JsonObject();
            obj.addProperty("index", index);
            obj.addProperty("name", name);
            return obj;
        }
    }
}
