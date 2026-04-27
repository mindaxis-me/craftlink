package me.mindaxis.view;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.lang.reflect.Method;
import java.util.logging.Logger;

/**
 * Netty channel handler that intercepts outgoing particle packets on the
 * anchor player's network channel.
 *
 * Uses reflection-only packet checks so this compiles against paper-api
 * without requiring server internals on the compile classpath.
 */
public class ParticleChannelHandler extends ChannelDuplexHandler {

    interface ParticlePacketSink {
        Logger logger();
        void onParticlePacketIntercepted(Object packet);
    }

    private final ParticlePacketSink sink;
    private final Class<?> particlePacketClass;

    private Class<?> bundlePacketClass;
    private Method subPacketsMethod;

    public ParticleChannelHandler(MindAxisViewPlugin plugin, Class<?> particlePacketClass) {
        this(new ParticlePacketSink() {
            @Override
            public Logger logger() {
                return plugin.getLogger();
            }

            @Override
            public void onParticlePacketIntercepted(Object packet) {
                plugin.onParticlePacketIntercepted(packet);
            }
        }, particlePacketClass, null, null);
    }

    ParticleChannelHandler(ParticlePacketSink sink, Class<?> particlePacketClass,
                           Class<?> bundlePacketClass, Method subPacketsMethod) {
        this.sink = sink;
        this.particlePacketClass = particlePacketClass;
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
            sink.logger().info("[ParticleInterceptor] Bundle packet support enabled (ClientboundBundlePacket found)");
        } catch (ClassNotFoundException e) {
            try {
                bundlePacketClass = Class.forName("net.minecraft.network.protocol.BundlePacket");
                subPacketsMethod = bundlePacketClass.getMethod("subPackets");
                sink.logger().info("[ParticleInterceptor] Bundle packet support enabled (BundlePacket found)");
            } catch (Exception e2) {
                bundlePacketClass = null;
                subPacketsMethod = null;
                sink.logger().warning("[ParticleInterceptor] Bundle packet class not found — particles inside bundles will be missed");
            }
        } catch (Exception e) {
            bundlePacketClass = null;
            subPacketsMethod = null;
            sink.logger().warning("[ParticleInterceptor] Bundle reflection init failed: " + e.getMessage());
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        try {
            if (bundlePacketClass != null && bundlePacketClass.isInstance(msg)) {
                Iterable<?> subPackets = (Iterable<?>) subPacketsMethod.invoke(msg);
                for (Object subPacket : subPackets) {
                    checkParticlePacket(subPacket);
                }
            } else {
                checkParticlePacket(msg);
            }
        } catch (Exception e) {
            sink.logger().warning("[ParticleInterceptor] " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        super.write(ctx, msg, promise);
    }

    private void checkParticlePacket(Object packet) {
        if (particlePacketClass != null && particlePacketClass.isInstance(packet)) {
            sink.onParticlePacketIntercepted(packet);
        }
    }
}
