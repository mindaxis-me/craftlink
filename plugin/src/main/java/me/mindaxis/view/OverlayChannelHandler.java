package me.mindaxis.view;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.lang.reflect.Method;

/**
 * Netty channel handler that intercepts outgoing scoreboard and bossbar packets
 * for the anchor player's connection. Mirrors the sound/particle interception
 * pattern and unwraps bundle packets when present.
 */
public class OverlayChannelHandler extends ChannelDuplexHandler {

    private final MindAxisViewPlugin plugin;
    private final Class<?> objectivePacketClass;
    private final Class<?> scorePacketClass;
    private final Class<?> resetScorePacketClass;
    private final Class<?> displayPacketClass;
    private final Class<?> bossEventPacketClass;

    private Class<?> bundlePacketClass;
    private Method subPacketsMethod;

    public OverlayChannelHandler(
            MindAxisViewPlugin plugin,
            Class<?> objectivePacketClass,
            Class<?> scorePacketClass,
            Class<?> resetScorePacketClass,
            Class<?> displayPacketClass,
            Class<?> bossEventPacketClass
    ) {
        this.plugin = plugin;
        this.objectivePacketClass = objectivePacketClass;
        this.scorePacketClass = scorePacketClass;
        this.resetScorePacketClass = resetScorePacketClass;
        this.displayPacketClass = displayPacketClass;
        this.bossEventPacketClass = bossEventPacketClass;
        initBundleReflection();
    }

    private void initBundleReflection() {
        try {
            bundlePacketClass = Class.forName("net.minecraft.network.protocol.game.ClientboundBundlePacket");
            subPacketsMethod = bundlePacketClass.getMethod("subPackets");
            plugin.getLogger().info("[OverlayInterceptor] Bundle packet support enabled (ClientboundBundlePacket found)");
        } catch (ClassNotFoundException e) {
            try {
                bundlePacketClass = Class.forName("net.minecraft.network.protocol.BundlePacket");
                subPacketsMethod = bundlePacketClass.getMethod("subPackets");
                plugin.getLogger().info("[OverlayInterceptor] Bundle packet support enabled (BundlePacket found)");
            } catch (Exception e2) {
                bundlePacketClass = null;
                subPacketsMethod = null;
                plugin.getLogger().warning("[OverlayInterceptor] Bundle packet class not found — bundled overlay packets will be missed");
            }
        } catch (Exception e) {
            bundlePacketClass = null;
            subPacketsMethod = null;
            plugin.getLogger().warning("[OverlayInterceptor] Bundle reflection init failed: " + e.getMessage());
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        try {
            if (bundlePacketClass != null && bundlePacketClass.isInstance(msg)) {
                Iterable<?> subPackets = (Iterable<?>) subPacketsMethod.invoke(msg);
                for (Object subPacket : subPackets) {
                    checkOverlayPacket(subPacket);
                }
            } else {
                checkOverlayPacket(msg);
            }
        } catch (Exception e) {
            plugin.getLogger().warning("[OverlayInterceptor] " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        super.write(ctx, msg, promise);
    }

    private void checkOverlayPacket(Object packet) {
        if (objectivePacketClass != null && objectivePacketClass.isInstance(packet)) {
            plugin.onScoreboardObjectivePacketIntercepted(packet);
        } else if (scorePacketClass != null && scorePacketClass.isInstance(packet)) {
            plugin.onScoreboardScorePacketIntercepted(packet);
        } else if (resetScorePacketClass != null && resetScorePacketClass.isInstance(packet)) {
            plugin.onScoreboardResetScorePacketIntercepted(packet);
        } else if (displayPacketClass != null && displayPacketClass.isInstance(packet)) {
            plugin.onScoreboardDisplayPacketIntercepted(packet);
        } else if (bossEventPacketClass != null && bossEventPacketClass.isInstance(packet)) {
            plugin.onBossEventPacketIntercepted(packet);
        }
    }
}
