package me.mindaxis.view;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.lang.reflect.Method;

/**
 * Netty channel handler that intercepts outgoing sound packets on the
 * anchor player's network channel.
 *
 * Uses Class.isInstance() checks (resolved by MindAxisViewPlugin via reflection)
 * instead of compile-time NMS imports, so this compiles against paper-api + netty-all
 * without needing paperweight-userdev.
 *
 * Paper 1.19.4+ bundles multiple packets inside ClientboundBundlePacket.
 * Sound packets are typically inside bundles, so we iterate sub-packets.
 */
public class SoundChannelHandler extends ChannelDuplexHandler {

    private final Class<?> soundPacketClass;
    private final Class<?> entitySoundPacketClass;
    private final MindAxisViewPlugin plugin;

    // Bundle packet support (Paper 1.19.4+)
    private Class<?> bundlePacketClass;
    private Method subPacketsMethod;

    public SoundChannelHandler(MindAxisViewPlugin plugin, Class<?> soundPacketClass, Class<?> entitySoundPacketClass) {
        this.plugin = plugin;
        this.soundPacketClass = soundPacketClass;
        this.entitySoundPacketClass = entitySoundPacketClass;
        initBundleReflection();
    }

    /**
     * Resolve ClientboundBundlePacket and its subPackets() method via reflection.
     * Falls back gracefully if not available (pre-1.19.4).
     */
    private void initBundleReflection() {
        try {
            bundlePacketClass = Class.forName("net.minecraft.network.protocol.game.ClientboundBundlePacket");
            // BundlePacket has subPackets() returning Iterable<Packet<?>>
            subPacketsMethod = bundlePacketClass.getMethod("subPackets");
            plugin.getLogger().info("[SoundInterceptor] Bundle packet support enabled (ClientboundBundlePacket found)");
        } catch (ClassNotFoundException e) {
            // Try the parent class directly
            try {
                bundlePacketClass = Class.forName("net.minecraft.network.protocol.BundlePacket");
                subPacketsMethod = bundlePacketClass.getMethod("subPackets");
                plugin.getLogger().info("[SoundInterceptor] Bundle packet support enabled (BundlePacket found)");
            } catch (Exception e2) {
                bundlePacketClass = null;
                subPacketsMethod = null;
                plugin.getLogger().warning("[SoundInterceptor] Bundle packet class not found — sounds inside bundles will be missed");
            }
        } catch (Exception e) {
            bundlePacketClass = null;
            subPacketsMethod = null;
            plugin.getLogger().warning("[SoundInterceptor] Bundle reflection init failed: " + e.getMessage());
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        try {
            // Check for bundle packets first (Paper 1.19.4+ wraps most packets in bundles)
            if (bundlePacketClass != null && bundlePacketClass.isInstance(msg)) {
                Iterable<?> subPackets = (Iterable<?>) subPacketsMethod.invoke(msg);
                for (Object subPacket : subPackets) {
                    checkSoundPacket(subPacket);
                }
            } else {
                checkSoundPacket(msg);
            }
        } catch (Exception e) {
            plugin.getLogger().warning("[SoundInterceptor] " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        super.write(ctx, msg, promise);
    }

    /**
     * Check if a packet is a sound packet and forward to the plugin if so.
     */
    private void checkSoundPacket(Object packet) {
        if (soundPacketClass != null && soundPacketClass.isInstance(packet)) {
            plugin.onSoundPacketIntercepted(packet, false);
        } else if (entitySoundPacketClass != null && entitySoundPacketClass.isInstance(packet)) {
            plugin.onSoundPacketIntercepted(packet, true);
        }
    }
}
