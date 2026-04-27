package me.mindaxis.view;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ContainerChannelHandlerTest {

    private static final Logger TEST_LOGGER = Logger.getLogger("ContainerChannelHandlerTest");

    @Test
    void forwardsDirectContainerPackets() throws Exception {
        CapturingSink sink = new CapturingSink();
        Method subPacketsMethod = FakeBundlePacket.class.getMethod("subPackets");
        ContainerChannelHandler handler = new ContainerChannelHandler(
                sink,
                FakeOpenPacket.class,
                FakeClosePacket.class,
                FakeContentPacket.class,
                FakeSlotPacket.class,
                FakeBundlePacket.class,
                subPacketsMethod
        );

        FakeOpenPacket openPacket = new FakeOpenPacket(1);
        FakeContentPacket contentPacket = new FakeContentPacket(1);
        FakeSlotPacket slotPacket = new FakeSlotPacket(1);
        FakeClosePacket closePacket = new FakeClosePacket(1);

        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.writeOutbound(openPacket);
        channel.writeOutbound(contentPacket);
        channel.writeOutbound(slotPacket);
        channel.writeOutbound(closePacket);

        assertEquals(List.of("open", "content", "slot", "close"), sink.kinds);
        assertSame(openPacket, sink.packets.get(0));
        assertSame(contentPacket, sink.packets.get(1));
        assertSame(slotPacket, sink.packets.get(2));
        assertSame(closePacket, sink.packets.get(3));
    }

    @Test
    void forwardsBundledContainerPackets() throws Exception {
        CapturingSink sink = new CapturingSink();
        Method subPacketsMethod = FakeBundlePacket.class.getMethod("subPackets");
        ContainerChannelHandler handler = new ContainerChannelHandler(
                sink,
                FakeOpenPacket.class,
                FakeClosePacket.class,
                FakeContentPacket.class,
                FakeSlotPacket.class,
                FakeBundlePacket.class,
                subPacketsMethod
        );

        FakeOpenPacket openPacket = new FakeOpenPacket(7);
        FakeContentPacket contentPacket = new FakeContentPacket(7);
        FakeSlotPacket slotPacket = new FakeSlotPacket(7);
        FakeClosePacket closePacket = new FakeClosePacket(7);
        FakeBundlePacket bundlePacket = new FakeBundlePacket(List.of(
                new Object(),
                openPacket,
                contentPacket,
                slotPacket,
                closePacket,
                new Object()
        ));

        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.writeOutbound(bundlePacket);

        assertEquals(List.of("open", "content", "slot", "close"), sink.kinds);
        assertSame(openPacket, sink.packets.get(0));
        assertSame(contentPacket, sink.packets.get(1));
        assertSame(slotPacket, sink.packets.get(2));
        assertSame(closePacket, sink.packets.get(3));
    }

    private static final class CapturingSink implements ContainerChannelHandler.ContainerPacketSink {
        private final List<String> kinds = new ArrayList<>();
        private final List<Object> packets = new ArrayList<>();

        @Override
        public Logger logger() {
            return TEST_LOGGER;
        }

        @Override
        public void onContainerOpenPacketIntercepted(Object packet) {
            kinds.add("open");
            packets.add(packet);
        }

        @Override
        public void onContainerClosePacketIntercepted(Object packet) {
            kinds.add("close");
            packets.add(packet);
        }

        @Override
        public void onContainerContentPacketIntercepted(Object packet) {
            kinds.add("content");
            packets.add(packet);
        }

        @Override
        public void onContainerSlotPacketIntercepted(Object packet) {
            kinds.add("slot");
            packets.add(packet);
        }
    }

    private record FakeOpenPacket(int windowId) {
    }

    private record FakeClosePacket(int windowId) {
    }

    private record FakeContentPacket(int windowId) {
    }

    private record FakeSlotPacket(int windowId) {
    }

    private record FakeBundlePacket(List<?> packets) {
        public Iterable<?> subPackets() {
            return packets;
        }
    }
}
