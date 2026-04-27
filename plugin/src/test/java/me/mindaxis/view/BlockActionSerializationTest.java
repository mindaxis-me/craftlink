package me.mindaxis.view;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BlockActionSerializationTest {

    @Test
    void serializesChestOpenBlockAction() {
        JsonObject msg = MindAxisViewPlugin.buildBlockActionMessage(113, 70, 486, 1, 1, 54);

        assertEquals(7, msg.entrySet().size());
        assertEquals("blockAction", msg.get("type").getAsString());
        assertEquals(113, msg.get("x").getAsInt());
        assertEquals(70, msg.get("y").getAsInt());
        assertEquals(486, msg.get("z").getAsInt());
        assertEquals(1, msg.get("actionId").getAsInt());
        assertEquals(1, msg.get("actionParam").getAsInt());
        assertEquals(54, msg.get("blockId").getAsInt());
    }

    @Test
    void serializesChestCloseBlockAction() {
        JsonObject msg = MindAxisViewPlugin.buildBlockActionMessage(113, 70, 486, 1, 0, 54);

        assertEquals(7, msg.entrySet().size());
        assertEquals("blockAction", msg.get("type").getAsString());
        assertEquals(113, msg.get("x").getAsInt());
        assertEquals(70, msg.get("y").getAsInt());
        assertEquals(486, msg.get("z").getAsInt());
        assertEquals(1, msg.get("actionId").getAsInt());
        assertEquals(0, msg.get("actionParam").getAsInt());
        assertEquals(54, msg.get("blockId").getAsInt());
    }
}
