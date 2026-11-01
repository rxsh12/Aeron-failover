package demo.aeron;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class TcpProxy implements EgressListener {
    private static final String[] MEMBER_NAMES = {"A", "B", "C"};
    private static final int LEADER_NOT_REPORTED = -3;
    private final DemoConfig config;
    private final int tcpPort;
    private final int producerPort;
    private final int leaderStatusPort;
    private final int clientQueueCapacity;
    private final int producerBatchSize;
    private final long requestRetryTimeoutNs;
    private final long offerIntervalNs;
    private final long reconnectDelayMs;
    private final long producerQueuePollTimeoutMs;
    private final long metricsReportIntervalNs;
    private final boolean verbose;
    private final Map<Socket, ClientConnection> clients = new ConcurrentHashMap<>();
    private final BlockingQueue<byte[]> producerQueue;
    private final MutableDirectBuffer command = new ExpandableArrayBuffer(Long.BYTES);
    private final DurableOutbox outbox;
    private final AtomicLong udpReceived = new AtomicLong();
    private final AtomicLong udpDropped = new AtomicLong();
    private final AtomicLong journaled = new AtomicLong();
    private final AtomicReference<IOException> ingressFailure = new AtomicReference<>();
    private final GapTracker gapTracker = new GapTracker();
    private long lastForwardedSequence;
    private byte[] pendingPayload;
    private boolean requestOffered;
    private long lastOfferTimeNs;
    private long commitReportStartedNs = System.nanoTime();
    private long lastCommitReportTimeNs = commitReportStartedNs;
    private long commitsSinceLastReport;
    private volatile int lastLeaderMemberId = LEADER_NOT_REPORTED;
    private volatile boolean clusterSessionClosed;

    private TcpProxy(DurableOutbox outbox, DemoConfig config) {
        this.outbox = outbox;
        this.config = config;
        tcpPort = config.integer("proxy.tcp-port");
        producerPort = config.integer("proxy.producer-udp-port");
        leaderStatusPort = config.integer("proxy.leader-status-port");
        clientQueueCapacity = config.integer("proxy.client-queue-capacity");
        producerQueue = new ArrayBlockingQueue<>(config.integer("proxy.producer-queue-capacity"));
        producerBatchSize = config.integer("proxy.producer-batch-size");
        requestRetryTimeoutNs = config.longValue("proxy.request-retry-timeout-ms") * 1_000_000L;
        offerIntervalNs = config.longValue("proxy.offer-interval-ns");
        reconnectDelayMs = config.longValue("proxy.reconnect-delay-ms");
        producerQueuePollTimeoutMs = config.longValue("proxy.queue-poll-timeout-ms");
        metricsReportIntervalNs = config.longValue("proxy.metrics-report-interval-ms") * 1_000_000L;
        verbose = config.bool("proxy.verbose", false);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 0) {
            throw new IllegalArgumentException("Usage: TcpProxy (settings are read from config.properties)");
        }
        DemoConfig config = DemoConfig.load();
        List<String> hosts = config.clusterHosts();
        if (hosts.size() != MEMBER_NAMES.length) {
            throw new IllegalArgumentException("Provide exactly three cluster member hosts.");
        }
        validateConfig(config);

        try (DurableOutbox outbox = new DurableOutbox(config.path("proxy.outbox-file"));
             MediaDriver mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                 .aeronDirectoryName(config.aeronDirectoryPrefix() + "-proxy")
                 .threadingMode(ThreadingMode.SHARED)
                 .dirDeleteOnStart(true)
                 .dirDeleteOnShutdown(true))) {
            TcpProxy proxy = new TcpProxy(outbox, config);
            proxy.run(hosts, mediaDriver.aeronDirectoryName());
        }
    }

    private static void validateConfig(DemoConfig config) {
        int tcpPort = config.integer("proxy.tcp-port");
        int producerPort = config.integer("proxy.producer-udp-port");
        int statusPort = config.integer("proxy.leader-status-port");
        if (!validPort(tcpPort) || !validPort(producerPort) || !validPort(statusPort) ||
            tcpPort == producerPort || tcpPort == statusPort || producerPort == statusPort) {
            throw new IllegalArgumentException("Proxy ports must be valid and distinct");
        }
        if (config.integer("proxy.client-queue-capacity") <= 0 ||
            config.integer("proxy.producer-queue-capacity") <= 0 ||
            config.integer("proxy.producer-batch-size") <= 0 ||
            config.longValue("proxy.request-retry-timeout-ms") <= 0 ||
            config.longValue("proxy.request-retry-timeout-ms") > Long.MAX_VALUE / 1_000_000L ||
            config.longValue("proxy.offer-interval-ns") < 0 ||
            config.longValue("proxy.reconnect-delay-ms") < 0 ||
            config.longValue("proxy.queue-poll-timeout-ms") < 0 ||
            config.longValue("proxy.metrics-report-interval-ms") <= 0 ||
            config.longValue("proxy.metrics-report-interval-ms") > Long.MAX_VALUE / 1_000_000L) {
            throw new IllegalArgumentException("Proxy queue sizes, batch size, and retry timeout must be positive; " +
                "offer interval must be zero or positive");
        }
    }

    private static boolean validPort(int port) {
        return port >= 1 && port <= 65535;
    }

    private void run(List<String> hosts, String aeronDirectory) throws IOException {
        String ingressEndpoints = "0=" + hosts.get(0) + ":" +
            ClusterNode.port(config, 0, config.integer("cluster.client-facing-offset")) +
            ",1=" + hosts.get(1) + ":" +
            ClusterNode.port(config, 1, config.integer("cluster.client-facing-offset")) +
            ",2=" + hosts.get(2) + ":" +
            ClusterNode.port(config, 2, config.integer("cluster.client-facing-offset"));
        IdleStrategy idle = BusySpinIdleStrategy.INSTANCE;

        try (ServerSocket serverSocket = new ServerSocket(tcpPort);
             DatagramSocket producerSocket = new DatagramSocket(producerPort);
             ServerSocket leaderStatusSocket =
                 new ServerSocket(leaderStatusPort, 50,
                     InetAddress.getByName(config.string("proxy.leader-status-bind-address")))) {
            Thread acceptor = new Thread(() -> acceptClients(serverSocket), "tcp-proxy-acceptor");
            acceptor.setDaemon(true);
            acceptor.start();
            Thread producerReceiver = new Thread(() -> receiveUdpProducers(producerSocket),
                "udp-proxy-producer-receiver");
            producerReceiver.setDaemon(true);
            producerReceiver.start();
            Thread outboxWriter = new Thread(this::persistProducerQueue, "udp-proxy-outbox-writer");
            outboxWriter.setDaemon(true);
            outboxWriter.start();
            Thread statusAcceptor = new Thread(
                () -> serveLeaderStatus(leaderStatusSocket), "tcp-proxy-leader-status");
            statusAcceptor.setDaemon(true);
            statusAcceptor.start();
            System.out.printf("TCP proxy ready on 0.0.0.0:%d; cluster endpoints %s%n",
                tcpPort, ingressEndpoints);
            System.out.printf("Fire-and-forget UDP producer ingress ready on 0.0.0.0:%d; 100-byte payloads.%n",
                producerPort);
            System.out.printf("Leader status available on loopback TCP %d for the monitor.%n",
                leaderStatusPort);

            while (!Thread.currentThread().isInterrupted()) {
                clusterSessionClosed = false;
                lastLeaderMemberId = LEADER_NOT_REPORTED;
                try (AeronCluster cluster = AeronCluster.connect(new AeronCluster.Context()
                    .aeronDirectoryName(aeronDirectory)
                    .ingressChannel("aeron:udp")
                    .ingressEndpoints(ingressEndpoints)
                    .egressChannel("aeron:udp?endpoint=0.0.0.0:0")
                    .egressListener(this))) {
                    if (pendingPayload != null) {
                        requestOffered = false;
                    }
                    while (!Thread.currentThread().isInterrupted() && !clusterSessionClosed) {
                        IOException writerFailure = ingressFailure.get();
                        if (writerFailure != null) {
                            throw new UncheckedIOException("UDP producer outbox writer failed", writerFailure);
                        }
                        int fragments = cluster.pollEgress();
                        int currentLeaderMemberId = cluster.leaderMemberId();
                        if (currentLeaderMemberId >= 0 && currentLeaderMemberId < MEMBER_NAMES.length) {
                            lastLeaderMemberId = currentLeaderMemberId;
                        }
                        long now = System.nanoTime();
                        if (pendingPayload == null) {
                            pendingPayload = outbox.nextPending();
                            requestOffered = false;
                        }
                        if (pendingPayload == null) {
                            cluster.sendKeepAlive();
                        }
                        if (pendingPayload != null && requestOffered &&
                            now - lastOfferTimeNs >= requestRetryTimeoutNs) {
                            requestOffered = false;
                            System.err.printf("[C PROXY] No cluster commit response for message %d; retrying.%n",
                                ByteBuffer.wrap(pendingPayload).getLong());
                        }
                        if (pendingPayload != null && !requestOffered &&
                            now - lastOfferTimeNs >= offerIntervalNs) {
                            command.putBytes(0, pendingPayload);
                            long result = cluster.offer(command, 0, DurableOutbox.PAYLOAD_LENGTH);
                            if (result > 0) {
                                requestOffered = true;
                                lastOfferTimeNs = now;
                            }
                        }

                        idle.idle(fragments);
                    }
                } catch (TimeoutException e) {
                    System.err.println("[C PROXY] Timed out connecting to Aeron Cluster; retrying in " +
                        reconnectDelayMs + " ms: " +
                        e.getMessage());
                }
                if (!Thread.currentThread().isInterrupted()) {
                    System.err.printf("[C PROXY] Cluster client session ended; reconnecting in %d ms.%n",
                        reconnectDelayMs);
                    lastLeaderMemberId = -1;
                    requestOffered = false;
                    try {
                        Thread.sleep(reconnectDelayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private void receiveUdpProducers(DatagramSocket socket) {
        byte[] receiveBuffer = new byte[2048];
        DatagramPacket packet = new DatagramPacket(receiveBuffer, receiveBuffer.length);
        while (!socket.isClosed()) {
            try {
                packet.setLength(receiveBuffer.length);
                socket.receive(packet);
                if (packet.getLength() != DurableOutbox.PAYLOAD_LENGTH) {
                    udpDropped.incrementAndGet();
                    continue;
                }
                byte[] payload = java.util.Arrays.copyOfRange(
                    packet.getData(), packet.getOffset(), packet.getOffset() + packet.getLength());
                udpReceived.incrementAndGet();
                if (producerQueue.offer(payload)) {
                    gapTracker.record(ByteBuffer.wrap(payload).getLong());
                } else {
                    udpDropped.incrementAndGet();
                }
            } catch (IOException e) {
                if (!socket.isClosed()) {
                    ingressFailure.compareAndSet(null, e);
                    System.err.println("UDP producer receiver failed: " + e);
                }
                return;
            }
        }
    }

    private void persistProducerQueue() {
        List<byte[]> batch = new ArrayList<>(producerBatchSize);
        long reportStartedNs = System.nanoTime();
        long previousReceived = 0;
        long previousDropped = 0;
        long previousJournaled = 0;
        long previousRecovered = 0;
        long previousDuplicates = 0;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                byte[] first = producerQueue.poll(producerQueuePollTimeoutMs, TimeUnit.MILLISECONDS);
                if (first != null) {
                    batch.add(first);
                    producerQueue.drainTo(batch, producerBatchSize - 1);
                    journaled.addAndGet(outbox.appendBatch(batch));
                    batch.clear();
                }
                long now = System.nanoTime();
                long elapsed = now - reportStartedNs;
                if (elapsed >= metricsReportIntervalNs) {
                    long received = udpReceived.get();
                    long dropped = udpDropped.get();
                    long persisted = journaled.get();
                    long missing = gapTracker.missingIds();
                    long recovered = gapTracker.recoveredIds();
                    long duplicates = gapTracker.duplicates();
                    System.out.printf("[UDP INGRESS] received=%.0f/s journaled=%.0f/s dropped=%.0f/s " +
                            "missingIds=%d recoveredGaps=%.0f/s duplicateIds=%.0f/s queued=%d%n",
                        (received - previousReceived) * 1_000_000_000.0 / elapsed,
                        (persisted - previousJournaled) * 1_000_000_000.0 / elapsed,
                        (dropped - previousDropped) * 1_000_000_000.0 / elapsed,
                        missing,
                        (recovered - previousRecovered) * 1_000_000_000.0 / elapsed,
                        (duplicates - previousDuplicates) * 1_000_000_000.0 / elapsed,
                        producerQueue.size());
                    previousReceived = received;
                    previousDropped = dropped;
                    previousJournaled = persisted;
                    previousRecovered = recovered;
                    previousDuplicates = duplicates;
                    reportStartedNs = now;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                ingressFailure.compareAndSet(null, e);
                System.err.println("Durable UDP outbox append failed: " + e);
                return;
            }
        }
    }

    private void serveLeaderStatus(ServerSocket serverSocket) {
        while (!serverSocket.isClosed()) {
            try (Socket socket = serverSocket.accept()) {
                socket.getOutputStream().write(
                    (lastLeaderMemberId + "\n").getBytes(StandardCharsets.US_ASCII));
                socket.getOutputStream().flush();
            } catch (IOException e) {
                if (!serverSocket.isClosed()) {
                    System.err.println("Leader status request failed: " + e);
                }
                return;
            }
        }
    }

    private void acceptClients(ServerSocket serverSocket) {
        while (!serverSocket.isClosed()) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);
                ClientConnection connection = new ClientConnection(socket, clientQueueCapacity, verbose);
                clients.put(socket, connection);
                Thread writer = new Thread(() -> writeToClient(connection),
                    "tcp-proxy-client-" + socket.getRemoteSocketAddress());
                writer.setDaemon(true);
                writer.start();
                if (verbose) {
                    System.out.println("TCP subscriber connected: " + socket.getRemoteSocketAddress());
                }
            } catch (IOException e) {
                if (!serverSocket.isClosed()) {
                    System.err.println("TCP client accept failed: " + e);
                }
                return;
            }
        }
    }

    private void writeToClient(ClientConnection connection) {
        try (Socket socket = connection.socket;
             DataOutputStream output = new DataOutputStream(socket.getOutputStream())) {
            while (!socket.isClosed()) {
                Long sequence = connection.sequences.take();
                output.writeLong(sequence);
                output.flush();
            }
        } catch (IOException e) {
            if (verbose) {
                System.err.println("TCP subscriber disconnected: " + e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            clients.remove(connection.socket);
            connection.close();
        }
    }

    @Override
    public void onMessage(
        long clusterSessionId,
        long timestamp,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header) {
        if (length != StreamService.RESPONSE_LENGTH) {
            System.err.println("Ignoring cluster response with invalid length " + length);
            return;
        }

        long responseRequestId = buffer.getLong(offset);
        long sequence = buffer.getLong(offset + Long.BYTES);
        if (pendingPayload != null && responseRequestId == ByteBuffer.wrap(pendingPayload).getLong()) {
            try {
                outbox.markCommitted(responseRequestId);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to persist cluster commit cursor", e);
            }
            pendingPayload = null;
            requestOffered = false;
            reportCommitRate();
        }

        if (sequence <= lastForwardedSequence) {
            return;
        }
        lastForwardedSequence = sequence;

        for (ClientConnection client : clients.values()) {
            if (!client.sequences.offer(sequence)) {
                System.err.println("Disconnecting slow TCP subscriber with a full delivery queue: " +
                    client.socket.getRemoteSocketAddress());
                clients.remove(client.socket);
                client.close();
            }
        }
        if (verbose) {
            System.out.println("Queued stream sequence " + sequence + " for " + clients.size() + " TCP subscriber(s)");
        }
    }

    private void reportCommitRate() {
        long now = System.nanoTime();
        commitsSinceLastReport++;
        long elapsed = now - lastCommitReportTimeNs;
        if (now - commitReportStartedNs >= metricsReportIntervalNs) {
            System.out.printf("[C PROXY] Cluster commit responses: %.0f messages/s%n",
                commitsSinceLastReport * 1_000_000_000.0 / elapsed);
            commitsSinceLastReport = 0;
            lastCommitReportTimeNs = now;
            commitReportStartedNs = now;
        }
    }

    @Override
    public void onSessionEvent(
        long correlationId,
        long clusterSessionId,
        long leadershipTermId,
        int leaderMemberId,
        EventCode code,
        String detail) {
        System.out.printf("Cluster event: leader=%d term=%d code=%s detail=%s%n",
            leaderMemberId, leadershipTermId, code, detail);
        if (code == EventCode.CLOSED) {
            clusterSessionClosed = true;
            lastLeaderMemberId = -1;
            requestOffered = false;
            return;
        }
        if (leaderMemberId >= 0 && leaderMemberId < MEMBER_NAMES.length) {
            lastLeaderMemberId = leaderMemberId;
        }
    }

    @Override
    public void onNewLeader(
        long clusterSessionId,
        long leadershipTermId,
        int leaderMemberId,
        String ingressEndpoints) {
        String newLeaderName = memberName(leaderMemberId);
        if (lastLeaderMemberId < 0) {
            System.out.printf("[C PROXY] Cluster connected. Current leader: Server %s (member %d, term %d).%n",
                newLeaderName, leaderMemberId, leadershipTermId);
        } else if (lastLeaderMemberId != leaderMemberId) {
            System.out.printf("%n[C PROXY] FAILOVER OBSERVED: Server %s -> Server %s (term %d).%n" +
                    "[C PROXY] TCP client endpoint :%d is unchanged; forwarding continues through the new leader.%n%n",
                memberName(lastLeaderMemberId), newLeaderName, leadershipTermId, tcpPort);
            if (pendingPayload != null && requestOffered) {
                requestOffered = false;
                System.out.printf("[C PROXY] Re-offering in-flight message %d after leader change.%n",
                    ByteBuffer.wrap(pendingPayload).getLong());
            }
        }
        lastLeaderMemberId = leaderMemberId;
    }

    private static String memberName(int id) {
        return id >= 0 && id < MEMBER_NAMES.length ? MEMBER_NAMES[id] : "member-" + id;
    }

    private static final class GapTracker {
        private final TreeMap<Long, Long> missingRanges = new TreeMap<>();
        private long highestId;
        private long recoveredIds;
        private long duplicateIds;

        synchronized void record(long id) {
            if (id <= 0) {
                return;
            }
            if (highestId == 0) {
                highestId = id;
            } else if (id > highestId) {
                if (id > highestId + 1) {
                    missingRanges.put(highestId + 1, id - 1);
                }
                highestId = id;
            } else {
                Map.Entry<Long, Long> range = missingRanges.floorEntry(id);
                if (range == null || range.getValue() < id) {
                    duplicateIds++;
                    return;
                }
                long start = range.getKey();
                long end = range.getValue();
                missingRanges.remove(start);
                if (start < id) {
                    missingRanges.put(start, id - 1);
                }
                if (id < end) {
                    missingRanges.put(id + 1, end);
                }
                recoveredIds++;
            }
        }

        synchronized long missingIds() {
            long total = 0;
            for (Map.Entry<Long, Long> range : missingRanges.entrySet()) {
                total += range.getValue() - range.getKey() + 1;
            }
            return total;
        }

        synchronized long recoveredIds() {
            return recoveredIds;
        }

        synchronized long duplicates() {
            return duplicateIds;
        }
    }

    private static final class ClientConnection {
        private final Socket socket;
        private final BlockingQueue<Long> sequences;
        private final boolean verbose;

        private ClientConnection(Socket socket, int queueCapacity, boolean verbose) {
            this.socket = socket;
            this.sequences = new ArrayBlockingQueue<>(queueCapacity);
            this.verbose = verbose;
        }

        private void close() {
            try {
                socket.close();
            } catch (IOException e) {
                if (verbose) {
                    System.err.println("Failed to close TCP subscriber socket: " + e);
                }
            }
        }
    }
}
