package me.mindaxis.view;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.lang.reflect.Method;
import java.util.logging.Logger;

/**
 * Netty channel handler that intercepts outgoing container packets on the
 * anchor player's network channel.
 *
 * Uses reflection-only packet checks so this compiles against paper-api
 * without requiring server internals on the compile classpath.
 */
public class ContainerChannelHandler extends ChannelDuplexHandler {

    interface ContainerPacketSink {
        Logger logger();
        void onContainerOpenPacketIntercepted(Object packet);
        void onContainerClosePacketIntercepted(Object packet);
        void onContainerContentPacketIntercepted(Object packet);
        void onContainerSlotPacketIntercepted(Object packet);
    }

    private final ContainerPacketSink sink;
    private final Class<?> openScreenPacketClass;
    private final Class<?> containerClosePacketClass;
    private final Class<?> containerContentPacketClass;
    private final Class<?> containerSlotPacketClass;

    private Class<?> bundlePacketClass;
    private Method subPacketsMethod;

    public ContainerChannelHandler(
            MindAxisViewPlugin plugin,
            Class<?> openScreenPacketClass,
            Class<?> containerClosePacketClass,
            Class<?> containerContentPacketClass,
            Class<?> containerSlotPacketClass
    ) {
        this(new ContainerPacketSink() {
            @Override
            public Logger logger() {
                return plugin.getLogger();
            }

            @Override
            public void onContainerOpenPacketIntercepted(Object packet) {
                plugin.onContainerOpenPacketIntercepted(packet);
            }

            @Override
            public void onContainerClosePacketIntercepted(Object packet) {
                plugin.onContainerClosePacketIntercepted(packet);
            }

            @Override
            public void onContainerContentPacketIntercepted(Object packet) {
                plugin.onContainerContentPacketIntercepted(packet);
            }

            @Override
            public void onContainerSlotPacketIntercepted(Object packet) {
                plugin.onContainerSlotPacketIntercepted(packet);
            }
        }, openScreenPacketClass, containerClosePacketClass, containerContentPacketClass, containerSlotPacketClass, null, null);
    }

    ContainerChannelHandler(
            ContainerPacketSink sink,
            Class<?> openScreenPacketClass,
            Class<?> containerClosePacketClass,
            Class<?> containerContentPacketClass,
            Class<?> containerSlotPacketClass,
            Class<?> bundlePacketClass,
            Method subPacketsMethod
    ) {
        this.sink = sink;
        this.openScreenPacketClass = openScreenPacketClass;
        this.containerClosePacketClass = containerClosePacketClass;
        this.containerContentPacketClass = containerContentPacketClass;
        this.containerSlotPacketClass = containerSlotPacketClass;
        this.bundlePacketClass = bundlePacketClass;
        this.subPacketsMethod = subPacketsMethod;
        if (this.bundlePacketClass == null || this.subPacketsMethod == null) {
            initBundleReflection();
        }
    }

    private void initBundleReflection() {
        try {
            bundlePacketClass = Class.forName("net.minecraft.network.protocol.game.ClientboundBundlePacket");
            subPacketsMethod = bundlePacketClass.getMethod("subPackets");
            sink.logger().info("[ContainerInterceptor] Bundle packet support enabled (ClientboundBundlePacket found)");
        } catch (ClassNotFoundException e) {
            try {
                bundlePacketClass = Class.forName("net.minecraft.network.protocol.BundlePacket");
                subPacketsMethod = bundlePacketClass.getMethod("subPackets");
                sink.logger().info("[ContainerInterceptor] Bundle packet support enabled (BundlePacket found)");
            } catch (Exception e2) {
                bundlePacketClass = null;
                subPacketsMethod = null;
                sink.logger().warning("[ContainerInterceptor] Bundle packet class not found — container packets inside bundles will be missed");
            }
        } catch (Exception e) {
            bundlePacketClass = null;
            subPacketsMethod = null;
            sink.logger().warning("[ContainerInterceptor] Bundle reflection init failed: " + e.getMessage());
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        try {
            if (bundlePacketClass != null && bundlePacketClass.isInstance(msg)) {
                Iterable<?> subPackets = (Iterable<?>) subPacketsMethod.invoke(msg);
                for (Object subPacket : subPackets) {
                    checkContainerPacket(subPacket);
                }
            } else {
                checkContainerPacket(msg);
            }
        } catch (Exception e) {
            sink.logger().warning("[ContainerInterceptor] " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        super.write(ctx, msg, promise);
    }

    private void checkContainerPacket(Object packet) {
        if (openScreenPacketClass != null && openScreenPacketClass.isInstance(packet)) {
            sink.onContainerOpenPacketIntercepted(packet);
        } else if (containerClosePacketClass != null && containerClosePacketClass.isInstance(packet)) {
            sink.onContainerClosePacketIntercepted(packet);
        } else if (containerContentPacketClass != null && containerContentPacketClass.isInstance(packet)) {
            sink.onContainerContentPacketIntercepted(packet);
        } else if (containerSlotPacketClass != null && containerSlotPacketClass.isInstance(packet)) {
            sink.onContainerSlotPacketIntercepted(packet);
        }
    }
}
