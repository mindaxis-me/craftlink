package me.mindaxis.view;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.lang.reflect.Method;

/**
 * Netty channel handler that intercepts outgoing block event packets on the
 * anchor player's network channel.
 *
 * Uses reflection-only NMS access so it compiles against paper-api + netty-all
 * without paperweight-userdev.
 *
 * Paper 1.19.4+ bundles multiple packets inside ClientboundBundlePacket, so
 * block events must be checked inside bundle sub-packets as well.
 */
public class BlockEventChannelHandler extends ChannelDuplexHandler {

    private final Class<?> blockEventPacketClass;
    private final MindAxisViewPlugin plugin;

    // Bundle packet support (Paper 1.19.4+)
    private Class<?> bundlePacketClass;
    private Method subPacketsMethod;

    public BlockEventChannelHandler(MindAxisViewPlugin plugin, Class<?> blockEventPacketClass) {
        this.plugin = plugin;
        this.blockEventPacketClass = blockEventPacketClass;
        initBundleReflection();
    }

    /**
     * Resolve ClientboundBundlePacket and its subPackets() method via reflection.
     * Falls back gracefully if not available (pre-1.19.4).
     */
    private void initBundleReflection() {
        try {
            bundlePacketClass = Class.forName("net.minecraft.network.protocol.game.ClientboundBundlePacket");
            subPacketsMethod = bundlePacketClass.getMethod("subPackets");
            plugin.getLogger().info("[BlockEventInterceptor] Bundle packet support enabled (ClientboundBundlePacket found)");
        } catch (ClassNotFoundException e) {
            try {
                bundlePacketClass = Class.forName("net.minecraft.network.protocol.BundlePacket");
                subPacketsMethod = bundlePacketClass.getMethod("subPackets");
                plugin.getLogger().info("[BlockEventInterceptor] Bundle packet support enabled (BundlePacket found)");
            } catch (Exception e2) {
                bundlePacketClass = null;
                subPacketsMethod = null;
                plugin.getLogger().warning("[BlockEventInterceptor] Bundle packet class not found — block events inside bundles will be missed");
            }
        } catch (Exception e) {
            bundlePacketClass = null;
            subPacketsMethod = null;
            plugin.getLogger().warning("[BlockEventInterceptor] Bundle reflection init failed: " + e.getMessage());
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        try {
            if (bundlePacketClass != null && bundlePacketClass.isInstance(msg)) {
                Iterable<?> subPackets = (Iterable<?>) subPacketsMethod.invoke(msg);
                for (Object subPacket : subPackets) {
                    checkBlockEventPacket(subPacket);
                }
            } else {
                checkBlockEventPacket(msg);
            }
        } catch (Exception e) {
            plugin.getLogger().warning("[BlockEventInterceptor] " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        super.write(ctx, msg, promise);
    }

    private void checkBlockEventPacket(Object packet) {
        if (blockEventPacketClass != null && blockEventPacketClass.isInstance(packet)) {
            plugin.onBlockEventPacketIntercepted(packet);
        }
    }
}
