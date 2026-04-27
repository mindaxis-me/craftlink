package me.mindaxis.view;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OverlayPayloadsTest {

    @Test
    void serializesScoreboardObjectiveAndDisplayMessages() {
        JsonObject objective = parse(OverlayPayloads.scoreboardSetObjective("kills", "Kills", "integer"));
        assertEquals("scoreboard", objective.get("type").getAsString());
        assertEquals("setObjective", objective.get("action").getAsString());
        assertEquals("kills", objective.get("name").getAsString());
        assertEquals("Kills", objective.get("displayName").getAsString());
        assertEquals("integer", objective.get("renderType").getAsString());

        JsonObject score = parse(OverlayPayloads.scoreboardSetScore("kills", "PlayerName", 5, null));
        assertEquals("setScore", score.get("action").getAsString());
        assertEquals("kills", score.get("objective").getAsString());
        assertEquals("PlayerName", score.get("entry").getAsString());
        assertEquals(5, score.get("value").getAsInt());
        assertTrue(score.get("displayName").isJsonNull());

        JsonObject display = parse(OverlayPayloads.scoreboardSetDisplay("sidebar", "kills"));
        assertEquals("setDisplay", display.get("action").getAsString());
        assertEquals("sidebar", display.get("slot").getAsString());
        assertEquals("kills", display.get("objective").getAsString());
    }

    @Test
    void serializesBossbarMessages() {
        JsonObject add = parse(OverlayPayloads.bossbarAdd(
                "123e4567-e89b-12d3-a456-426614174000",
                "Ender Dragon",
                0.75f,
                "pink",
                "progress",
                true,
                false,
                true
        ));
        assertEquals("bossbar", add.get("type").getAsString());
        assertEquals("add", add.get("action").getAsString());
        assertEquals("123e4567-e89b-12d3-a456-426614174000", add.get("id").getAsString());
        assertEquals("Ender Dragon", add.get("name").getAsString());
        assertEquals(0.75f, add.get("progress").getAsFloat());
        assertEquals("pink", add.get("color").getAsString());
        assertEquals("progress", add.get("division").getAsString());
        assertTrue(add.get("darkenSky").getAsBoolean());
        assertTrue(add.get("createWorldFog").getAsBoolean());

        JsonObject updateProgress = parse(OverlayPayloads.bossbarUpdateProgress("boss-1", 0.5f));
        assertEquals("updateProgress", updateProgress.get("action").getAsString());
        assertEquals(0.5f, updateProgress.get("progress").getAsFloat());

        JsonObject updateStyle = parse(OverlayPayloads.bossbarUpdateStyle("boss-1", "red", "notched_10"));
        assertEquals("updateStyle", updateStyle.get("action").getAsString());
        assertEquals("red", updateStyle.get("color").getAsString());
        assertEquals("notched_10", updateStyle.get("division").getAsString());
    }

    private static JsonObject parse(String json) {
        return JsonParser.parseString(json).getAsJsonObject();
    }
}
