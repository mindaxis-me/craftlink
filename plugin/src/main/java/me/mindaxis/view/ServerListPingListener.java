package me.mindaxis.view;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.serializer.gson.GsonComponentSerializer;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.server.ServerListPingEvent;

final class ServerListPingListener implements Listener {

    private static final String PORTAL_ENABLED_CONFIG_PATH = "portal.enabled";
    private static final String PORTAL_MARKER_TEXT = "CL1";
    private static final GsonComponentSerializer GSON_COMPONENT = GsonComponentSerializer.gson();

    private final MindAxisViewPlugin plugin;

    ServerListPingListener(MindAxisViewPlugin plugin) {
        this.plugin = plugin;
    }

    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true)
    public void onServerListPing(ServerListPingEvent event) {
        if (!plugin.getConfig().getBoolean(PORTAL_ENABLED_CONFIG_PATH, true)) {
            return;
        }
        event.motd(appendPortalMarker(event.motd()));
    }

    private Component appendPortalMarker(Component currentMotd) {
        Component motd = currentMotd == null ? Component.empty() : currentMotd;
        JsonElement serializedMotd = JsonParser.parseString(GSON_COMPONENT.serialize(motd));
        JsonObject root = ensureObjectRoot(serializedMotd);
        JsonArray extra = root.has("extra") && root.get("extra").isJsonArray()
                ? root.getAsJsonArray("extra")
                : new JsonArray();
        if (!root.has("extra") || !root.get("extra").isJsonArray()) {
            root.add("extra", extra);
        }
        extra.add(createPortalMarker());
        return GSON_COMPONENT.deserialize(root.toString());
    }

    private JsonObject ensureObjectRoot(JsonElement serializedMotd) {
        if (serializedMotd.isJsonObject()) {
            return serializedMotd.getAsJsonObject();
        }
        JsonObject root = new JsonObject();
        root.addProperty("text", "");
        JsonArray extra = new JsonArray();
        extra.add(serializedMotd);
        root.add("extra", extra);
        return root;
    }

    private JsonObject createPortalMarker() {
        JsonObject marker = new JsonObject();
        marker.addProperty("text", PORTAL_MARKER_TEXT);
        marker.addProperty("color", "black");
        marker.addProperty("obfuscated", true);
        return marker;
    }
}
