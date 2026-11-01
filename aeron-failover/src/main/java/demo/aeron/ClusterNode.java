package demo.aeron;

import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class ClusterNode {
    private ClusterNode() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: ClusterNode <member-id: 0|1|2>");
        }

        DemoConfig config = DemoConfig.load();
        int memberId = Integer.parseInt(args[0]);
        var hosts = config.clusterHosts();
        if (hosts.size() != 3 || memberId < 0 || memberId >= hosts.size()) {
            throw new IllegalArgumentException("Configuration must contain three hosts and member ID must be 0, 1, or 2.");
        }

        String host = hosts.get(memberId);
        String aeronDirectory = config.aeronDirectoryPrefix() + "-member-" + memberId;
        Path dataDirectory = config.path("cluster.data-dir");
        File memberDirectory = dataDirectory.resolve("member-" + memberId).toFile();
        String clusterMembers = clusterMembers(hosts, config);
        String termLength = config.string("cluster.term-length");
        String localControlChannel = "aeron:ipc?term-length=" + termLength;

        MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectory)
            .dirDeleteOnStart(false)
            .dirDeleteOnShutdown(false);

        AeronArchive.Context replicationArchiveContext = new AeronArchive.Context()
            .controlResponseChannel("aeron:udp?endpoint=" + host + ":0");

        Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(aeronDirectory)
            .archiveDir(new File(memberDirectory, "archive"))
            .controlChannel(udpChannel(config, memberId, host, config.integer("cluster.archive-control-offset")))
            .archiveClientContext(replicationArchiveContext)
            .localControlChannel(localControlChannel)
            .recordingEventsEnabled(false)
            .replicationChannel("aeron:udp?endpoint=" + host + ":0");

        AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .lock(NoOpLock.INSTANCE)
            .controlRequestChannel(localControlChannel)
            .controlResponseChannel(localControlChannel)
            .aeronDirectoryName(aeronDirectory);

        File clusterDirectory = new File(memberDirectory, "cluster");
        ConsensusModule.Context consensusContext = new ConsensusModule.Context()
            .clusterMemberId(memberId)
            .clusterMembers(clusterMembers)
            .clusterDir(clusterDirectory)
            .ingressChannel("aeron:udp?term-length=" + termLength)
            .replicationChannel("aeron:udp?endpoint=" + host + ":0")
            .archiveContext(aeronArchiveContext.clone());

        ClusteredServiceContainer.Context serviceContext = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDirectory)
            .archiveContext(aeronArchiveContext.clone())
            .clusterDir(clusterDirectory)
            .clusteredService(new StreamService(memberId));

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier()) {
            Thread shutdownHook = new Thread(barrier::signalAll, "cluster-node-shutdown-signal");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            try (HealthPongServer healthServer = new HealthPongServer(memberId, config);
                 ClusteredMediaDriver ignored = ClusteredMediaDriver.launch(
                    mediaDriverContext,
                    archiveContext,
                    consensusContext);
                 ClusteredServiceContainer ignoredService = ClusteredServiceContainer.launch(serviceContext)) {
                System.out.printf("Member %d started on %s; cluster members: %s%n", memberId, host, clusterMembers);
                barrier.await();
            } finally {
                barrier.remove();
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // The JVM is already shutting down; the hook has served its purpose.
                }
            }
        }
    }

    private static String clusterMembers(List<String> hosts, DemoConfig config) {
        List<String> members = new ArrayList<>();
        for (int id = 0; id < hosts.size(); id++) {
            String host = hosts.get(id);
            members.add(id + "," +
                host + ":" + port(config, id, config.integer("cluster.client-facing-offset")) + "," +
                host + ":" + port(config, id, config.integer("cluster.member-facing-offset")) + "," +
                host + ":" + port(config, id, config.integer("cluster.log-offset")) + "," +
                host + ":" + port(config, id, config.integer("cluster.transfer-offset")) + "," +
                host + ":" + port(config, id, config.integer("cluster.archive-control-offset")));
        }
        return String.join("|", members);
    }

    static int port(DemoConfig config, int memberId, int offset) {
        return config.integer("cluster.port-base") +
            memberId * config.integer("cluster.ports-per-member") + offset;
    }

    static int healthPort(DemoConfig config, int memberId) {
        return config.integer("cluster.health-port-base") + memberId;
    }

    static String memberName(int memberId) {
        return switch (memberId) {
            case 0 -> "A";
            case 1 -> "B";
            case 2 -> "C";
            default -> "member-" + memberId;
        };
    }

    private static String udpChannel(DemoConfig config, int memberId, String host, int offset) {
        return "aeron:udp?endpoint=" + host + ":" + port(config, memberId, offset) +
            "|term-length=" + config.string("cluster.term-length");
    }
}
