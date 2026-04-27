package me.mindaxis.view;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WSServerTest {

    private WSServer server;

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.stop(1000);
            server = null;
        }
    }

    @Test
    void rejectsConnectionsWithoutToken() throws Exception {
        server = new WSServer(freePort(), 8, Logger.getLogger("WSServerTest"), "secret-token");
        server.start();
        Thread.sleep(150L);

        HttpClient client = HttpClient.newHttpClient();
        CompletionException error = assertThrows(CompletionException.class, () ->
                client.newWebSocketBuilder()
                        .buildAsync(URI.create("ws://127.0.0.1:" + server.getPort()), new NoopListener())
                        .join());

        assertTrue(error.getCause() != null, "handshake rejection should surface a cause");
        Thread.sleep(100L);
        assertEquals(0, server.getClientCount());
    }

    @Test
    void acceptsConnectionsWithBearerToken() throws Exception {
        server = new WSServer(freePort(), 8, Logger.getLogger("WSServerTest"), "secret-token");
        server.start();
        Thread.sleep(150L);

        HttpClient client = HttpClient.newHttpClient();
        WebSocket ws = client.newWebSocketBuilder()
                .header("Authorization", "Bearer secret-token")
                .buildAsync(URI.create("ws://127.0.0.1:" + server.getPort()), new NoopListener())
                .join();

        Thread.sleep(100L);
        assertEquals(1, server.getClientCount());
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(2, TimeUnit.SECONDS);
    }

    @Test
    void acceptsConnectionsWithAuthTokenQueryParameter() throws Exception {
        server = new WSServer(freePort(), 8, Logger.getLogger("WSServerTest"), "secret-token");
        server.start();
        Thread.sleep(150L);

        HttpClient client = HttpClient.newHttpClient();
        WebSocket ws = client.newWebSocketBuilder()
                .buildAsync(URI.create("ws://127.0.0.1:" + server.getPort() + "?auth-token=secret-token"),
                        new NoopListener())
                .join();

        Thread.sleep(100L);
        assertEquals(1, server.getClientCount());
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(2, TimeUnit.SECONDS);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static final class NoopListener implements WebSocket.Listener {
    }
}
