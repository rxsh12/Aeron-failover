package demo.aeron;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public final class TcpSubscriber {
    private TcpSubscriber() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 0) {
            throw new IllegalArgumentException("Usage: TcpSubscriber (settings are read from config.properties)");
        }
        DemoConfig config = DemoConfig.load();
        String proxyHost = config.string("proxy.host");
        int proxyPort = config.integer("proxy.tcp-port");
        int reconnectDelayMs = config.integer("subscriber.reconnect-delay-ms");
        int connectTimeoutMs = config.integer("subscriber.connect-timeout-ms");
        boolean verbose = config.bool("subscriber.verbose", false);
        Path cursorFile = config.path("subscriber.cursor-file");
        if (proxyPort < 1 || proxyPort > 65535 || reconnectDelayMs < 0 || connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("Subscriber proxy port must be valid and reconnect delay non-negative");
        }
        boolean hasPreviousCursor = Files.exists(cursorFile);
        long lastSeen = readLastSeen(cursorFile);
        long missingSequences = 0;
        long receivedThisRun = 0;
        System.out.printf("Starting subscriber at cursor %d (%s).%n",
            lastSeen, hasPreviousCursor ? "persisted cursor" : "no prior cursor; joining live stream");

        while (!Thread.currentThread().isInterrupted()) {
            try (Socket socket = new Socket()) {
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);
                socket.connect(new InetSocketAddress(proxyHost, proxyPort), connectTimeoutMs);
                System.out.println("Connected to stable TCP proxy " + proxyHost + ":" + proxyPort);
                DataInputStream input = new DataInputStream(socket.getInputStream());
                while (!Thread.currentThread().isInterrupted()) {
                    long sequence = input.readLong();
                    if (sequence <= lastSeen) {
                        continue;
                    }

                    if (hasPreviousCursor && sequence > lastSeen + 1) {
                        long gap = sequence - lastSeen - 1;
                        missingSequences += gap;
                        System.err.printf("[GAP] Missing %d sequence(s): last received=%d, next received=%d; " +
                                "detected gaps this run=%d%n",
                            gap, lastSeen, sequence, missingSequences);
                    }

                    lastSeen = sequence;
                    hasPreviousCursor = true;
                    receivedThisRun++;
                    persistLastSeen(cursorFile, sequence);
                    if (verbose) {
                        System.out.printf("Received stream sequence %d (received this run=%d, detected gaps=%d)%n",
                            sequence, receivedThisRun, missingSequences);
                    }
                }
            } catch (IOException e) {
                System.err.printf("TCP proxy connection lost at cursor %d; reconnecting: %s%n",
                    lastSeen, e.getMessage());
                try {
                    Thread.sleep(reconnectDelayMs);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static long readLastSeen(Path path) throws IOException {
        return Files.exists(path) ? Long.parseLong(Files.readString(path).trim()) : 0L;
    }

    private static void persistLastSeen(Path path, long sequence) throws IOException {
        Path parent = path.toAbsolutePath().getParent();
        Files.createDirectories(parent);
        Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
        Files.writeString(temporary, Long.toString(sequence));
        Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
