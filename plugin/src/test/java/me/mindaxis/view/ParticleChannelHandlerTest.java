package me.mindaxis.view;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ParticleChannelHandlerTest {

    private static final Logger TEST_LOGGER = Logger.getLogger("ParticleChannelHandlerTest");

    @Test
    void forwardsDirectParticlePackets() throws Exception {
        CapturingSink sink = new CapturingSink();
        Method subPacketsMethod = FakeBundlePacket.class.getMethod("subPackets");
        ParticleChannelHandler handler = new ParticleChannelHandler(
                sink,
                FakeParticlePacket.class,
                FakeBundlePacket.class,
                subPacketsMethod
        );

        FakeParticlePacket packet = new FakeParticlePacket("direct");
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.writeOutbound(packet);

        assertEquals(1, sink.packets.size());
        assertSame(packet, sink.packets.getFirst());
    }

    @Test
    void forwardsBundledParticlePackets() throws Exception {
        CapturingSink sink = new CapturingSink();
        Method subPacketsMethod = FakeBundlePacket.class.getMethod("subPackets");
        ParticleChannelHandler handler = new ParticleChannelHandler(
                sink,
                FakeParticlePacket.class,
                FakeBundlePacket.class,
                subPacketsMethod
        );

        FakeParticlePacket particlePacket = new FakeParticlePacket("bundled");
        FakeBundlePacket bundlePacket = new FakeBundlePacket(List.of(new Object(), particlePacket, new Object()));
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.writeOutbound(bundlePacket);

        assertEquals(1, sink.packets.size());
        assertSame(particlePacket, sink.packets.getFirst());
    }

    @Test
    void serializesBlockParticlePayloads() {
        JsonObject data = new JsonObject();
        data.addProperty("stateId", 1234);
        data.addProperty("blockStateId", 1234);

        JsonObject json = JsonParser.parseString(ParticlePayloads.toJson(
                "minecraft:block",
                113.5, 70.0, 486.5,
                0.5, 0.5, 0.5,
                10, 0.1,
                data
        )).getAsJsonObject();

        assertEquals("particle", json.get("type").getAsString());
        assertEquals("minecraft:block", json.get("particle").getAsString());
        assertEquals(113.5, json.get("x").getAsDouble());
        assertEquals(10, json.get("count").getAsInt());
        assertEquals(1234, json.getAsJsonObject("data").get("stateId").getAsInt());
    }

    @Test
    void serializesExplosionParticlePayloads() {
        JsonObject json = JsonParser.parseString(ParticlePayloads.toJson(
                "minecraft:explosion",
                12.0, 65.0, -4.0,
                0.0, 0.0, 0.0,
                1, 0.0,
                new JsonObject()
        )).getAsJsonObject();

        assertEquals("minecraft:explosion", json.get("particle").getAsString());
        assertEquals(1, json.get("count").getAsInt());
        assertEquals(0, json.getAsJsonObject("data").size());
    }

    @Test
    void serializesAmbientParticlePayloads() {
        JsonObject json = JsonParser.parseString(ParticlePayloads.toJson(
                "minecraft:smoke",
                8.25, 66.0, 2.25,
                0.15, 0.25, 0.15,
                6, 0.02,
                new JsonObject()
        )).getAsJsonObject();

        assertEquals("minecraft:smoke", json.get("particle").getAsString());
        assertEquals(6, json.get("count").getAsInt());
        assertEquals(0.15, json.get("dx").getAsDouble());
        assertEquals(0, json.getAsJsonObject("data").size());
    }

    private static final class CapturingSink implements ParticleChannelHandler.ParticlePacketSink {
        private final List<Object> packets = new ArrayList<>();

        @Override
        public Logger logger() {
            return TEST_LOGGER;
        }

        @Override
        public void onParticlePacketIntercepted(Object packet) {
            packets.add(packet);
        }
    }

    private record FakeParticlePacket(String name) {
    }

    private record FakeBundlePacket(List<?> packets) {
        public Iterable<?> subPackets() {
            return packets;
        }
    }
}
