package me.mindaxis.view;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.bukkit.ChunkSnapshot;
import org.bukkit.DyeColor;
import org.bukkit.Material;
import org.bukkit.NamespacedKey;
import org.bukkit.SkullType;
import org.bukkit.block.Banner;
import org.bukkit.block.Biome;
import org.bukkit.block.BlockState;
import org.bukkit.block.Sign;
import org.bukkit.block.Skull;
import org.bukkit.block.data.BlockData;
import org.bukkit.block.sign.Side;
import org.bukkit.block.sign.SignSide;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkSerializerTest {

    @AfterEach
    void tearDown() {
        ChunkSerializer.clearStateIdProviderOverrideForTest();
    }

    @Test
    void gzipRoundTripPreservesRawBinaryFrame() throws Exception {
        byte[] raw = new byte[ChunkSerializer.BINARY_HEADER_SIZE + ChunkSerializer.BINARY_PAYLOAD_SIZE];
        ByteBuffer buf = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(ChunkSerializer.BINARY_TYPE_SUBCHUNK);
        buf.putInt(12);
        buf.putInt(-4);
        buf.putInt(4);
        for (int i = 0; i < 4096; i++) {
            buf.putInt(i % 8 == 0 ? 1 : 0);
        }

        byte[] compressed = ChunkSerializer.gzipBinaryFrame(raw);
        assertEquals(ChunkSerializer.BINARY_TYPE_SUBCHUNK_GZIP, compressed[0]);
        assertTrue(compressed.length < raw.length, "gzip frame should shrink repetitive chunk payloads");

        byte[] inflated = ChunkSerializer.inflateBinaryFrame(compressed);
        assertArrayEquals(raw, inflated);
    }

    @Test
    void inflateLeavesRawFramesUntouched() throws Exception {
        byte[] raw = new byte[ChunkSerializer.BINARY_HEADER_SIZE + ChunkSerializer.BINARY_PAYLOAD_SIZE];
        raw[0] = ChunkSerializer.BINARY_TYPE_SUBCHUNK;

        byte[] inflated = ChunkSerializer.inflateBinaryFrame(raw);
        assertArrayEquals(raw, inflated);
    }

    @Test
    @org.junit.jupiter.api.Disabled("Biome is a Bukkit enum — requires server runtime, not available in unit tests")
    void serializeBiomesEncodesPaletteAndIndices() {
        ChunkSnapshot snapshot = biomeSnapshot((x, y, z) ->
                x < 8 ? biome("minecraft:plains") : biome("minecraft:forest"));

        List<String> messages = ChunkSerializer.serializeBiomes(snapshot);
        assertFalse(messages.isEmpty(), "biome serialization should emit section messages");

        JsonObject msg = JsonParser.parseString(messages.get(0)).getAsJsonObject();
        assertEquals("biomes", msg.get("type").getAsString());
        assertEquals(12, msg.get("x").getAsInt());
        assertEquals(-4, msg.get("z").getAsInt());

        JsonArray palette = msg.getAsJsonArray("biomes");
        assertEquals(2, palette.size());
        assertEquals("minecraft:plains", palette.get(0).getAsJsonObject().get("name").getAsString());
        assertEquals("minecraft:forest", palette.get(1).getAsJsonObject().get("name").getAsString());

        byte[] indices = Base64.getDecoder().decode(msg.get("biomeIndices").getAsString());
        assertEquals(64, indices.length, "4x4x4 biome lattice should encode 64 indices");
        assertEquals(0, Byte.toUnsignedInt(indices[0]));
        assertEquals(0, Byte.toUnsignedInt(indices[1]));
        assertEquals(1, Byte.toUnsignedInt(indices[2]));
        assertEquals(1, Byte.toUnsignedInt(indices[3]));
    }

    @Test
    void serializeBinaryIncludesSkyAndBlockLightNibbleArrays() {
        ChunkSerializer.setStateIdProviderOverrideForTest(blockData -> switch (blockData.getAsString()) {
            case "minecraft:stone" -> 42;
            case "minecraft:air" -> 0;
            default -> -1;
        });

        ChunkSnapshot snapshot = blockSnapshot(
                Map.of(
                        blockKey(0, 64, 0), "minecraft:stone",
                        blockKey(0, 65, 0), "minecraft:stone"
                ),
                Map.of(
                        blockKey(0, 64, 0), 3,
                        blockKey(0, 65, 0), 12
                ),
                Map.of(
                        blockKey(0, 64, 0), 4,
                        blockKey(0, 65, 0), 9
                )
        );

        byte[] frame = ChunkSerializer.serializeBinary(snapshot).getFirst();
        assertEquals(ChunkSerializer.BINARY_HEADER_SIZE + ChunkSerializer.BINARY_PAYLOAD_SIZE, frame.length);

        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(ChunkSerializer.BINARY_TYPE_SUBCHUNK, buf.get(0));
        assertEquals(3, buf.getInt(1));
        assertEquals(-2, buf.getInt(5));
        assertEquals(4, buf.getInt(9));
        assertEquals(42, buf.getInt(ChunkSerializer.BINARY_HEADER_SIZE));
        assertEquals(42, buf.getInt(ChunkSerializer.BINARY_HEADER_SIZE + 4));

        int skyOffset = ChunkSerializer.BINARY_HEADER_SIZE + ChunkSerializer.BINARY_STATE_PAYLOAD_SIZE;
        int blockOffset = skyOffset + ChunkSerializer.LIGHT_NIBBLE_ARRAY_SIZE;
        assertEquals(0xC3, Byte.toUnsignedInt(frame[skyOffset]));
        assertEquals(0x94, Byte.toUnsignedInt(frame[blockOffset]));
    }

    @Test
    void serializeBlockEntityHandlesSigns() {
        SignSide front = proxy(SignSide.class, (proxy, method, args) -> switch (method.getName()) {
            case "getLines" -> new String[]{"Hello", "MindAxis", "", ""};
            case "getColor" -> DyeColor.BLUE;
            case "isGlowingText" -> true;
            default -> defaultValue(method.getReturnType());
        });
        SignSide back = proxy(SignSide.class, (proxy, method, args) -> switch (method.getName()) {
            case "getLines" -> new String[]{"Back", "", "", ""};
            case "getColor" -> DyeColor.BLACK;
            case "isGlowingText" -> false;
            default -> defaultValue(method.getReturnType());
        });
        Sign sign = proxy(Sign.class, blockStateHandler(
                Material.OAK_SIGN,
                blockData("minecraft:oak_sign[rotation=4,waterlogged=false]"),
                113,
                70,
                486,
                (proxy, method, args) -> switch (method.getName()) {
                    case "getSide" -> args[0] == Side.FRONT ? front : back;
                    case "getLines" -> new String[]{"Hello", "MindAxis", "", ""};
                    default -> defaultValue(method.getReturnType());
                }
        ));

        JsonObject entity = ChunkSerializer.serializeBlockEntity(sign);
        assertNotNull(entity);
        assertEquals("minecraft:oak_sign", entity.get("id").getAsString());
        JsonObject data = entity.getAsJsonObject("data");
        assertEquals(4, data.get("rotation").getAsInt());
        assertEquals("Hello", data.getAsJsonObject("front_text").getAsJsonArray("messages").get(0).getAsString());
        assertEquals("MindAxis", data.getAsJsonObject("front_text").getAsJsonArray("messages").get(1).getAsString());
        assertEquals("blue", data.getAsJsonObject("front_text").get("color").getAsString());
        assertTrue(data.getAsJsonObject("front_text").get("has_glowing_text").getAsBoolean());
        assertEquals("Back", data.getAsJsonObject("back_text").getAsJsonArray("messages").get(0).getAsString());
        assertEquals("Hello", data.get("Text1").getAsString());
    }

    @Test
    void serializeBlockEntityHandlesBanners() {
        Banner banner = proxy(Banner.class, blockStateHandler(
                Material.RED_BANNER,
                blockData("minecraft:red_banner[rotation=8]"),
                114,
                71,
                487,
                (proxy, method, args) -> switch (method.getName()) {
                    case "getBaseColor" -> DyeColor.RED;
                    case "getPatterns" -> List.of();
                    default -> defaultValue(method.getReturnType());
                }
        ));

        JsonObject entity = ChunkSerializer.serializeBlockEntity(banner);
        assertNotNull(entity);
        assertEquals("minecraft:red_banner", entity.get("id").getAsString());
        JsonObject data = entity.getAsJsonObject("data");
        assertEquals("red", data.get("baseColor").getAsString());
        assertEquals(8, data.get("rotation").getAsInt());
        JsonArray patterns = data.getAsJsonArray("patterns");
        assertEquals(0, patterns.size());
    }

    @Test
    void serializeBlockEntityHandlesSkulls() {
        Skull skull = proxy(Skull.class, blockStateHandler(
                Material.CREEPER_HEAD,
                blockData("minecraft:creeper_head[rotation=3]"),
                115,
                72,
                488,
                (proxy, method, args) -> switch (method.getName()) {
                    case "getSkullType" -> SkullType.CREEPER;
                    default -> defaultValue(method.getReturnType());
                }
        ));

        JsonObject entity = ChunkSerializer.serializeBlockEntity(skull);
        assertNotNull(entity);
        assertEquals("minecraft:creeper_head", entity.get("id").getAsString());
        JsonObject data = entity.getAsJsonObject("data");
        assertEquals(3, data.get("rotation").getAsInt());
        assertEquals("creeper", data.get("SkullType").getAsString());
    }

    private static InvocationHandler blockStateHandler(
            Material material,
            BlockData blockData,
            int x,
            int y,
            int z,
            InvocationHandler delegate
    ) {
        return (proxy, method, args) -> switch (method.getName()) {
            case "getType" -> material;
            case "getBlockData" -> blockData;
            case "getX" -> x;
            case "getY" -> y;
            case "getZ" -> z;
            default -> delegate.invoke(proxy, method, args);
        };
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        Class<?>[] interfaces = BlockState.class.isAssignableFrom(type)
                ? new Class<?>[]{type}
                : new Class<?>[]{type, BlockState.class};
        return (T) Proxy.newProxyInstance(
                ChunkSerializerTest.class.getClassLoader(),
                interfaces,
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> type.getSimpleName() + "Proxy";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> null;
                        };
                    }
                    return handler.invoke(proxy, method, args);
                }
        );
    }

    private static Object defaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) return null;
        if (returnType == boolean.class) return false;
        if (returnType == byte.class) return (byte) 0;
        if (returnType == short.class) return (short) 0;
        if (returnType == int.class) return 0;
        if (returnType == long.class) return 0L;
        if (returnType == float.class) return 0f;
        if (returnType == double.class) return 0d;
        if (returnType == char.class) return '\0';
        return null;
    }

    private static ChunkSnapshot biomeSnapshot(BiomeResolver resolver) {
        return (ChunkSnapshot) Proxy.newProxyInstance(
                ChunkSerializerTest.class.getClassLoader(),
                new Class<?>[]{ChunkSnapshot.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getX" -> 12;
                    case "getZ" -> -4;
                    case "getWorldName" -> "test";
                    case "getBlockType" -> Material.AIR;
                    case "getBlockData" -> null;
                    case "getData", "getBlockSkyLight", "getBlockEmittedLight", "getHighestBlockYAt" -> 0;
                    case "getBiome" -> args.length == 3
                            ? resolver.resolve((int) args[0], (int) args[1], (int) args[2])
                            : resolver.resolve((int) args[0], 0, (int) args[1]);
                    case "getRawBiomeTemperature" -> 0.8d;
                    case "getCaptureFullTime" -> 0L;
                    case "isSectionEmpty", "contains" -> false;
                    default -> throw new UnsupportedOperationException("Unexpected call: " + method.getName());
                });
    }

    private static ChunkSnapshot blockSnapshot(Map<String, String> blocks, Map<String, Integer> skyLights, Map<String, Integer> blockLights) {
        return (ChunkSnapshot) Proxy.newProxyInstance(
                ChunkSerializerTest.class.getClassLoader(),
                new Class<?>[]{ChunkSnapshot.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getX" -> 3;
                    case "getZ" -> -2;
                    case "getWorldName" -> "test";
                    case "getBlockType" -> Material.AIR;
                    case "getBlockData" -> blockData(blocks.getOrDefault(blockKey(args), "minecraft:air"));
                    case "getData", "getHighestBlockYAt" -> 0;
                    case "getBlockSkyLight" -> skyLights.getOrDefault(blockKey(args), 0);
                    case "getBlockEmittedLight" -> blockLights.getOrDefault(blockKey(args), 0);
                    case "getBiome" -> biome("minecraft:plains");
                    case "getRawBiomeTemperature" -> 0.8d;
                    case "getCaptureFullTime" -> 0L;
                    case "isSectionEmpty", "contains" -> false;
                    default -> throw new UnsupportedOperationException("Unexpected call: " + method.getName());
                });
    }

    private static Biome biome(String key) {
        // NamespacedKey is final — create via sun.misc.Unsafe to skip Bukkit static init
        String fullKey = key.contains(":") ? key : "minecraft:" + key;
        NamespacedKey namespacedKey;
        try {
            var unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            var unsafe = (sun.misc.Unsafe) unsafeField.get(null);
            namespacedKey = (NamespacedKey) unsafe.allocateInstance(NamespacedKey.class);
            var nsField = NamespacedKey.class.getDeclaredField("namespace");
            nsField.setAccessible(true);
            nsField.set(namespacedKey, "minecraft");
            var keyField = NamespacedKey.class.getDeclaredField("key");
            keyField.setAccessible(true);
            keyField.set(namespacedKey, fullKey.substring(fullKey.indexOf(':') + 1));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create mock NamespacedKey", e);
        }
        return (Biome) Proxy.newProxyInstance(
                ChunkSerializerTest.class.getClassLoader(),
                new Class<?>[]{Biome.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getKey" -> namespacedKey;
                    case "translationKey" -> "biome." + namespacedKey.getNamespace() + "." + namespacedKey.getKey();
                    case "key" -> namespacedKey;
                    case "toString" -> key;
                    case "hashCode" -> key.hashCode();
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException("Unexpected biome call: " + method.getName());
                });
    }

    private static BlockData blockData(String asString) {
        return (BlockData) Proxy.newProxyInstance(
                ChunkSerializerTest.class.getClassLoader(),
                new Class<?>[]{BlockData.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getAsString", "toString" -> asString;
                    default -> throw new UnsupportedOperationException("Unexpected call: " + method.getName());
                });
    }

    private static String blockKey(Object[] args) {
        return blockKey((int) args[0], (int) args[1], (int) args[2]);
    }

    private static String blockKey(int x, int y, int z) {
        return x + "," + y + "," + z;
    }

    @FunctionalInterface
    private interface BiomeResolver {
        Biome resolve(int x, int y, int z);
    }
}
