package me.mindaxis.view;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

final class OverlayPayloads {

    private OverlayPayloads() {
    }

    static String scoreboardSetObjective(String name, String displayName, String renderType) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "scoreboard");
        msg.addProperty("action", "setObjective");
        msg.addProperty("name", name);
        msg.addProperty("displayName", displayName);
        msg.addProperty("renderType", renderType);
        return msg.toString();
    }

    static String scoreboardRemoveObjective(String name) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "scoreboard");
        msg.addProperty("action", "removeObjective");
        msg.addProperty("name", name);
        return msg.toString();
    }

    static String scoreboardSetScore(String objective, String entry, int value, String displayName) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "scoreboard");
        msg.addProperty("action", "setScore");
        msg.addProperty("objective", objective);
        msg.addProperty("entry", entry);
        msg.addProperty("value", value);
        if (displayName == null) {
            msg.add("displayName", JsonNull.INSTANCE);
        } else {
            msg.addProperty("displayName", displayName);
        }
        return msg.toString();
    }

    static String scoreboardRemoveScore(String objective, String entry) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "scoreboard");
        msg.addProperty("action", "removeScore");
        if (objective == null) {
            msg.add("objective", JsonNull.INSTANCE);
        } else {
            msg.addProperty("objective", objective);
        }
        msg.addProperty("entry", entry);
        return msg.toString();
    }

    static String scoreboardSetDisplay(String slot, String objective) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "scoreboard");
        msg.addProperty("action", "setDisplay");
        msg.addProperty("slot", slot);
        if (objective == null) {
            msg.add("objective", JsonNull.INSTANCE);
        } else {
            msg.addProperty("objective", objective);
        }
        return msg.toString();
    }

    static String bossbarAdd(
            String id,
            String name,
            float progress,
            String color,
            String division,
            boolean darkenSky,
            boolean playBossMusic,
            boolean createWorldFog
    ) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "bossbar");
        msg.addProperty("action", "add");
        msg.addProperty("id", id);
        msg.addProperty("name", name);
        msg.addProperty("progress", progress);
        msg.addProperty("color", color);
        msg.addProperty("division", division);
        msg.addProperty("darkenSky", darkenSky);
        msg.addProperty("playBossMusic", playBossMusic);
        msg.addProperty("createWorldFog", createWorldFog);
        return msg.toString();
    }

    static String bossbarRemove(String id) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "bossbar");
        msg.addProperty("action", "remove");
        msg.addProperty("id", id);
        return msg.toString();
    }

    static String bossbarUpdateProgress(String id, float progress) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "bossbar");
        msg.addProperty("action", "updateProgress");
        msg.addProperty("id", id);
        msg.addProperty("progress", progress);
        return msg.toString();
    }

    static String bossbarUpdateName(String id, String name) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "bossbar");
        msg.addProperty("action", "updateName");
        msg.addProperty("id", id);
        msg.addProperty("name", name);
        return msg.toString();
    }

    static String bossbarUpdateStyle(String id, String color, String division) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "bossbar");
        msg.addProperty("action", "updateStyle");
        msg.addProperty("id", id);
        msg.addProperty("color", color);
        msg.addProperty("division", division);
        return msg.toString();
    }

    static String bossbarUpdateProperties(String id, boolean darkenSky, boolean playBossMusic, boolean createWorldFog) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "bossbar");
        msg.addProperty("action", "updateProperties");
        msg.addProperty("id", id);
        msg.addProperty("darkenSky", darkenSky);
        msg.addProperty("playBossMusic", playBossMusic);
        msg.addProperty("createWorldFog", createWorldFog);
        return msg.toString();
    }
}
