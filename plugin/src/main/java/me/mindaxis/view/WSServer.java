package me.mindaxis.view;

import com.google.gson.JsonElement;
import org.java_websocket.WebSocket;
import org.java_websocket.drafts.Draft;
import org.java_websocket.exceptions.InvalidDataException;
import org.java_websocket.framing.CloseFrame;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.handshake.ServerHandshakeBuilder;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Embedded WebSocket server that broadcasts viewer transport messages to all connected clients.
 * New clients perform an explicit plugin-driven initial sync instead of history replay, so
 * chunk/block frames are queued per-connection until the plugin flushes them after the snapshot send.
 */
public class WSServer extends WebSocketServer {

    private final Logger logger;
    private final int maxHistory;
    private final String authToken;
    private final List<String> history = new CopyOnWriteArrayList<>();
    private final List<byte[]> binaryHistory = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<WebSocket, CopyOnWriteArrayList<PendingFrame>> pendingInitialFrames =
            new ConcurrentHashMap<>();
    private Consumer<WebSocket> onClientConnect;
    private BiConsumer<WebSocket, String> onClientMessage;

    public WSServer(int port, int maxHistory, Logger logger, String authToken) {
        super(new InetSocketAddress(port));
        this.logger = logger;
        this.maxHistory = maxHistory;
        this.authToken = authToken == null ? "" : authToken.trim();
        setReuseAddr(true);
    }

    @Override
    public ServerHandshakeBuilder onWebsocketHandshakeReceivedAsServer(
            WebSocket conn, Draft draft, ClientHandshake handshake) throws InvalidDataException {
        if (!isAuthorized(handshake)) {
            logger.warning("[MindAxisView] Rejecting unauthorized WS client: " + conn.getRemoteSocketAddress());
            throw new InvalidDataException(CloseFrame.POLICY_VALIDATION, "Missing or invalid token");
        }
        return super.onWebsocketHandshakeReceivedAsServer(conn, draft, handshake);
    }

    /**
     * Set a callback that fires when a new WS client connects.
     * Used by the plugin to trigger a full chunk resend.
     */
    public void setOnClientConnect(Consumer<WebSocket> callback) {
        this.onClientConnect = callback;
    }

    public void setOnClientMessage(BiConsumer<WebSocket, String> callback) {
        this.onClientMessage = callback;
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        logger.info("[MindAxisView] WS client connected: " + conn.getRemoteSocketAddress()
                + " (clients=" + getConnections().size() + ")");
        if (onClientConnect != null) {
            pendingInitialFrames.put(conn, new CopyOnWriteArrayList<>());
            onClientConnect.accept(conn);
        }
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        pendingInitialFrames.remove(conn);
        logger.info("[MindAxisView] WS client disconnected: " + conn.getRemoteSocketAddress()
                + " (clients=" + getConnections().size() + ")");
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        if (onClientMessage == null) return;
        try {
            onClientMessage.accept(conn, message);
        } catch (Exception ex) {
            logger.warning("[MindAxisView] WS message handler failed: " + ex.getMessage());
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        logger.warning("[MindAxisView] WS error: " + ex.getMessage());
    }

    @Override
    public void onStart() {
        logger.info("[MindAxisView] WS server started on port " + getPort());
    }

    /**
     * Broadcast a JSON string to all connected clients and add to history.
     */
    public void broadcastMessage(String json) {
        // Add to history buffer
        history.add(json);
        while (history.size() > maxHistory) {
            history.remove(0);
        }
        sendTextToClients(json, true);
    }

    /**
     * Broadcast a JSON string to all connected clients WITHOUT adding to history.
     * Use for high-frequency ephemeral data (position updates) that should not
     * evict chunk data from the history buffer.
     */
    public void broadcastLive(String json) {
        broadcast(json);
    }

    /**
     * Broadcast a Gson JsonElement.
     */
    public void broadcastJson(JsonElement element) {
        broadcastMessage(element.toString());
    }

    /**
     * Broadcast a binary message (byte[]) to all connected clients and add to binary history.
     * Used for binary subchunk data (raw stateId arrays).
     */
    public void broadcastBinary(byte[] data) {
        binaryHistory.add(data);
        while (binaryHistory.size() > maxHistory) {
            binaryHistory.remove(0);
        }
        sendBinaryToClients(data, true);
    }

    /**
     * Clear the history buffer (e.g. on stop).
     */
    public void clearHistory() {
        history.clear();
        binaryHistory.clear();
        pendingInitialFrames.clear();
    }

    /**
     * Flush queued chunk/block frames for a client after its initial snapshot sync finishes.
     */
    public void flushQueuedFrames(WebSocket conn) {
        if (conn == null) return;
        List<PendingFrame> frames = pendingInitialFrames.remove(conn);
        if (frames == null || frames.isEmpty()) return;
        for (PendingFrame frame : new ArrayList<>(frames)) {
            if (!conn.isOpen()) return;
            if (frame.binary != null) {
                conn.send(frame.binary);
            } else if (frame.text != null) {
                conn.send(frame.text);
            }
        }
    }

    public int getClientCount() {
        return getConnections().size();
    }

    static boolean isAuthorized(ClientHandshake handshake, String authToken) {
        String expected = authToken == null ? "" : authToken.trim();
        if (expected.isEmpty()) return true;
        if (handshake == null) return false;

        String header = handshake.getFieldValue("Authorization");
        if (header != null) {
            String prefix = "Bearer ";
            if (header.regionMatches(true, 0, prefix, 0, prefix.length())) {
                String token = header.substring(prefix.length()).trim();
                if (expected.equals(token)) return true;
            }
        }

        String queryToken = extractTokenFromResource(handshake.getResourceDescriptor());
        return expected.equals(queryToken);
    }

    boolean isAuthorized(ClientHandshake handshake) {
        return isAuthorized(handshake, authToken);
    }

    private void sendTextToClients(String json, boolean queuePending) {
        PendingFrame frame = queuePending ? PendingFrame.text(json) : null;
        for (WebSocket conn : getConnections()) {
            if (conn == null || !conn.isOpen()) continue;
            CopyOnWriteArrayList<PendingFrame> pending = pendingInitialFrames.get(conn);
            if (queuePending && pending != null) {
                pending.add(frame);
                continue;
            }
            conn.send(json);
        }
    }

    private void sendBinaryToClients(byte[] data, boolean queuePending) {
        PendingFrame frame = queuePending ? PendingFrame.binary(data) : null;
        for (WebSocket conn : getConnections()) {
            if (conn == null || !conn.isOpen()) continue;
            CopyOnWriteArrayList<PendingFrame> pending = pendingInitialFrames.get(conn);
            if (queuePending && pending != null) {
                pending.add(frame);
                continue;
            }
            conn.send(data);
        }
    }

    private static String extractTokenFromResource(String resourceDescriptor) {
        if (resourceDescriptor == null || resourceDescriptor.isEmpty()) return "";
        int queryIndex = resourceDescriptor.indexOf('?');
        if (queryIndex < 0 || queryIndex == resourceDescriptor.length() - 1) return "";
        String query = resourceDescriptor.substring(queryIndex + 1);
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) continue;
            String key = pair.substring(0, eq);
            if (!"token".equals(key) && !"auth-token".equals(key)) continue;
            return pair.substring(eq + 1);
        }
        return "";
    }

    private static final class PendingFrame {
        private final String text;
        private final byte[] binary;

        private PendingFrame(String text, byte[] binary) {
            this.text = text;
            this.binary = binary;
        }

        private static PendingFrame text(String text) {
            return new PendingFrame(text, null);
        }

        private static PendingFrame binary(byte[] binary) {
            return new PendingFrame(null, binary);
        }
    }
}
