package demo.aeron;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class ClusterMonitor implements AutoCloseable {
    private static final int NO_LEADER = -1;
    private static final int STATUS_UNAVAILABLE = -2;
    private static final int LEADER_NOT_REPORTED = -3;
    private final long pingIntervalMs;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int missesBeforeDown;
    private final String leaderStatusHost;
    private final int leaderStatusPort;
    private final DemoConfig config;
    private final List<String> hosts;
    private final Thread monitorThread;
    private volatile boolean closed;

    public ClusterMonitor(List<String> hosts, DemoConfig config) {
        if (hosts.size() != 3) {
            throw new IllegalArgumentException("Provide exactly three cluster member hosts.");
        }
        this.hosts = hosts;
        this.config = config;
        pingIntervalMs = config.longValue("monitor.ping-interval-ms");
        connectTimeoutMs = config.integer("monitor.connect-timeout-ms");
        readTimeoutMs = config.integer("monitor.read-timeout-ms");
        missesBeforeDown = config.integer("monitor.misses-before-down");
        leaderStatusHost = config.string("monitor.proxy-status-host");
        leaderStatusPort = config.integer("proxy.leader-status-port");
        if (pingIntervalMs <= 0 || connectTimeoutMs <= 0 || readTimeoutMs <= 0 || missesBeforeDown <= 0 ||
            leaderStatusPort < 1 || leaderStatusPort > 65535) {
            throw new IllegalArgumentException("Monitor timing settings and leader status port must be positive");
        }
        monitorThread = new Thread(this::monitorMembers, "cluster-monitor-c");
        monitorThread.setDaemon(true);
        monitorThread.start();
        System.out.printf("[MONITOR] Reading the current leader from proxy status at %s:%d.%n",
            leaderStatusHost, leaderStatusPort);
        System.out.println("[MONITOR] Pinging only the current leader; Aeron Cluster elects the leader.");
    }

    private void monitorMembers() {
        int previousLeader = -2;
        int misses = 0;
        boolean down = false;
        boolean statusUnavailable = false;
        while (!closed) {
            int leaderMemberId = readLeaderMemberId();
            if (leaderMemberId == STATUS_UNAVAILABLE) {
                if (!statusUnavailable) {
                    System.out.printf("[MONITOR] Cannot read leader status from proxy at %s:%d; " +
                            "start the proxy or check the status port.%n",
                        leaderStatusHost, leaderStatusPort);
                    statusUnavailable = true;
                }
                previousLeader = -1;
                misses = 0;
                down = false;
                sleepUntilNextPing();
                continue;
            }

            statusUnavailable = false;
            if (leaderMemberId == NO_LEADER) {
                if (previousLeader != NO_LEADER) {
                    System.out.println("[MONITOR] Proxy reports no active cluster leader; no health ping sent.");
                }
                previousLeader = NO_LEADER;
                misses = 0;
                down = false;
                sleepUntilNextPing();
                continue;
            }
            if (leaderMemberId == LEADER_NOT_REPORTED) {
                if (previousLeader != LEADER_NOT_REPORTED) {
                    System.out.println("[MONITOR] Proxy has not reported a cluster leader yet; waiting.");
                }
                previousLeader = LEADER_NOT_REPORTED;
                misses = 0;
                down = false;
                sleepUntilNextPing();
                continue;
            }
            if (leaderMemberId >= hosts.size()) {
                System.err.printf("[MONITOR] Proxy reported invalid leader member ID %d.%n", leaderMemberId);
                sleepUntilNextPing();
                continue;
            }

            if (leaderMemberId != previousLeader) {
                previousLeader = leaderMemberId;
                misses = 0;
                down = false;
                System.out.printf("[MONITOR] Current leader: Server %s (member %d).%n",
                    ClusterNode.memberName(leaderMemberId), leaderMemberId);
            }

            String name = ClusterNode.memberName(leaderMemberId);
            boolean healthy = ping(hosts.get(leaderMemberId),
                ClusterNode.healthPort(config, leaderMemberId));
            if (healthy) {
                if (down) {
                    System.out.printf("[MONITOR] PING current leader Server %s -> PONG (reachable again).%n", name);
                } else {
                    System.out.printf("[MONITOR] PING current leader Server %s -> PONG%n", name);
                }
                misses = 0;
                down = false;
            } else {
                misses++;
                if (!down) {
                    System.out.printf("[MONITOR] PING current leader Server %s -> no PONG (%d/%d)%n",
                        name, misses, missesBeforeDown);
                }
                if (!down && misses >= missesBeforeDown) {
                    down = true;
                    System.out.printf("%n[MONITOR] Current leader Server %s is unreachable after %d PING attempts.%n" +
                            "[MONITOR] This is a health report; Aeron Cluster handles election.%n%n",
                        name, misses);
                }
            }
            sleepUntilNextPing();
        }
    }

    private int readLeaderMemberId() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(leaderStatusHost, leaderStatusPort), connectTimeoutMs);
            socket.setSoTimeout(readTimeoutMs);
            try (BufferedReader input = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII))) {
                String line = input.readLine();
                return line == null ? -1 : Integer.parseInt(line);
            }
        } catch (IOException | NumberFormatException e) {
            return STATUS_UNAVAILABLE;
        }
    }

    private void sleepUntilNextPing() {
        try {
            Thread.sleep(pingIntervalMs);
        } catch (InterruptedException e) {
            if (!closed) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean ping(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
            socket.setSoTimeout(readTimeoutMs);
            try (BufferedWriter output = new BufferedWriter(
                     new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.US_ASCII));
                 BufferedReader input = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII))) {
                output.write("PING");
                output.newLine();
                output.flush();
                String response = input.readLine();
                if (!"PONG".equals(response)) {
                    return false;
                }
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void close() {
        closed = true;
        monitorThread.interrupt();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 0) {
            throw new IllegalArgumentException("Usage: ClusterMonitor (settings are read from config.properties)");
        }

        DemoConfig config = DemoConfig.load();
        List<String> hosts = config.clusterHosts();
        try (ClusterMonitor ignored = new ClusterMonitor(hosts, config)) {
            Thread.currentThread().join();
        }
    }
}
