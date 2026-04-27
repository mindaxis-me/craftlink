package me.mindaxis.view;

import com.google.gson.JsonObject;

final class ParticlePayloads {

    private ParticlePayloads() {
    }

    static String toJson(String particleName, double x, double y, double z,
                         double dx, double dy, double dz,
                         int count, double speed, JsonObject data) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "particle");
        msg.addProperty("particle", particleName);
        msg.addProperty("x", x);
        msg.addProperty("y", y);
        msg.addProperty("z", z);
        msg.addProperty("dx", dx);
        msg.addProperty("dy", dy);
        msg.addProperty("dz", dz);
        msg.addProperty("count", count);
        msg.addProperty("speed", speed);
        msg.add("data", data != null ? data : new JsonObject());
        return msg.toString();
    }
}
