package me.mindaxis.view;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContainerPayloadsTest {

    @Test
    void serializesContainerOpenAndContentMessages() {
        JsonObject open = parse(ContainerPayloads.containerOpen(1, "minecraft:generic_9x3", "Chest", 27));
        assertEquals("containerOpen", open.get("type").getAsString());
        assertEquals(1, open.get("windowId").getAsInt());
        assertEquals("minecraft:generic_9x3", open.get("containerType").getAsString());
        assertEquals("Chest", open.get("title").getAsString());
        assertEquals(27, open.get("slots").getAsInt());

        JsonArray items = new JsonArray();
        items.add(JsonNull.INSTANCE);
        JsonObject item = new JsonObject();
        item.addProperty("slot", 1);
        item.addProperty("id", "minecraft:diamond");
        item.addProperty("count", 64);
        item.add("nbt", new JsonObject());
        items.add(item);

        JsonObject content = parse(ContainerPayloads.containerContent(1, items));
        assertEquals("containerContent", content.get("type").getAsString());
        assertEquals(1, content.get("windowId").getAsInt());
        assertEquals(2, content.getAsJsonArray("items").size());
        assertTrue(content.getAsJsonArray("items").get(0).isJsonNull());
        assertEquals("minecraft:diamond", content.getAsJsonArray("items").get(1).getAsJsonObject().get("id").getAsString());
    }

    @Test
    void serializesContainerCloseAndSlotMessages() {
        JsonObject close = parse(ContainerPayloads.containerClose(4));
        assertEquals("containerClose", close.get("type").getAsString());
        assertEquals(4, close.get("windowId").getAsInt());

        JsonObject slot = parse(ContainerPayloads.containerSlot(4, 3, JsonNull.INSTANCE));
        assertEquals("containerSlot", slot.get("type").getAsString());
        assertEquals(4, slot.get("windowId").getAsInt());
        assertEquals(3, slot.get("slot").getAsInt());
        assertTrue(slot.get("item").isJsonNull());
    }

    private static JsonObject parse(String json) {
        return JsonParser.parseString(json).getAsJsonObject();
    }
}
