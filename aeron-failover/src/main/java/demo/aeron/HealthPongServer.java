package demo.aeron;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

final class HealthPongServer implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final Thread acceptThread;
    private final int readTimeoutMs;
    private volatile boolean closed;

    HealthPongServer(int memberId, DemoConfig config) throws IOException {
        int port = ClusterNode.healthPort(config, memberId);
        readTimeoutMs = config.integer("cluster.health-read-timeout-ms");
        if (readTimeoutMs <= 0 || port < 1 || port > 65535) {
            throw new IllegalArgumentException("Health endpoint port and read timeout must be valid and positive");
        }
        serverSocket = new ServerSocket(port);
        acceptThread = new Thread(this::acceptPings, "health-pong-server-" + memberId);
        acceptThread.setDaemon(true);
        acceptThread.start();
        System.out.printf("[SERVER %s] Health PING/PONG endpoint listening on TCP %d%n",
            ClusterNode.memberName(memberId), port);
    }

    private void acceptPings() {
        while (!closed) {
            try {
                Socket socket = serverSocket.accept();
                socket.setSoTimeout(readTimeoutMs);
                try (socket;
                     BufferedReader input = new BufferedReader(
                         new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
                     BufferedWriter output = new BufferedWriter(
                         new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.US_ASCII))) {
                    if ("PING".equals(input.readLine())) {
                        output.write("PONG");
                        output.newLine();
                        output.flush();
                    }
                }
            } catch (IOException e) {
                if (!closed) {
                    System.err.println("[HEALTH] PING/PONG server failed: " + e);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        serverSocket.close();
    }
}
