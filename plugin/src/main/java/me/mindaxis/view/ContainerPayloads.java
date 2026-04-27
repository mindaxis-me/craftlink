package me.mindaxis.view;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

final class ContainerPayloads {

    private ContainerPayloads() {
    }

    static String containerOpen(int windowId, String containerType, String title, int slots) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "containerOpen");
        msg.addProperty("windowId", windowId);
        msg.addProperty("containerType", containerType);
        msg.addProperty("title", title);
        msg.addProperty("slots", slots);
        return msg.toString();
    }

    static String containerClose(int windowId) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "containerClose");
        msg.addProperty("windowId", windowId);
        return msg.toString();
    }

    static String containerContent(int windowId, JsonArray items) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "containerContent");
        msg.addProperty("windowId", windowId);
        msg.add("items", items == null ? new JsonArray() : items);
        return msg.toString();
    }

    static String containerSlot(int windowId, int slot, JsonElement item) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "containerSlot");
        msg.addProperty("windowId", windowId);
        msg.addProperty("slot", slot);
        msg.add("item", item == null ? JsonNull.INSTANCE : item);
        return msg.toString();
    }
}
