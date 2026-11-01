package demo.aeron;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.IdleStrategy;

import java.nio.ByteOrder;

public final class StreamService implements ClusteredService {
    static final int COMMAND_LENGTH = DurableOutbox.PAYLOAD_LENGTH;
    static final int RESPONSE_LENGTH = Long.BYTES * 2;

    private final MutableDirectBuffer response = new ExpandableArrayBuffer(RESPONSE_LENGTH);
    private final MutableDirectBuffer snapshot = new ExpandableArrayBuffer(RESPONSE_LENGTH);
    private long lastRequestId;
    private long streamSequence;
    private IdleStrategy idleStrategy;
    private final int memberId;

    StreamService(int memberId) {
        this.memberId = memberId;
    }

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        idleStrategy = cluster.idleStrategy();
        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
        System.out.printf("[SERVER %s] Started as %s; committed stream sequence=%d%n",
            ClusterNode.memberName(memberId), cluster.role(), streamSequence);
    }

    @Override
    public void onSessionMessage(
        ClientSession session,
        long timestamp,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header) {
        if (length != COMMAND_LENGTH && length != Long.BYTES) {
            throw new IllegalArgumentException("Expected a 100-byte message or legacy 8-byte request, got " + length);
        }

        long requestId = buffer.getLong(offset, ByteOrder.BIG_ENDIAN);
        if (requestId != lastRequestId) {
            lastRequestId = requestId;
            streamSequence++;
        }

        response.putLong(0, requestId);
        response.putLong(Long.BYTES, streamSequence);
        while (session.offer(response, 0, RESPONSE_LENGTH) < 0) {
            idleStrategy.idle();
        }
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication publication) {
        snapshot.putLong(0, lastRequestId);
        snapshot.putLong(Long.BYTES, streamSequence);
        idleStrategy.reset();
        while (publication.offer(snapshot, 0, RESPONSE_LENGTH) < 0) {
            idleStrategy.idle();
        }
    }

    private void loadSnapshot(Image image) {
        MutableBoolean loaded = new MutableBoolean(false);
        FragmentHandler handler = (buffer, offset, length, header) -> {
            if (length != RESPONSE_LENGTH) {
                throw new IllegalStateException("Invalid stream snapshot length: " + length);
            }
            lastRequestId = buffer.getLong(offset);
            streamSequence = buffer.getLong(offset + Long.BYTES);
            loaded.set(true);
        };

        while (!image.isEndOfStream() && !loaded.value) {
            int fragments = image.poll(handler, 1);
            idleStrategy.idle(fragments);
        }
        if (!loaded.value) {
            throw new IllegalStateException("Cluster snapshot did not contain stream state");
        }
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        if (newRole == Cluster.Role.LEADER) {
            System.out.printf("%n========== SERVER %s IS LEADER; TAKING OVER CLUSTER WORK ==========%n%n",
                ClusterNode.memberName(memberId));
        } else {
            System.out.printf("[SERVER %s] Role is now %s; work is handled by the elected leader.%n",
                ClusterNode.memberName(memberId), newRole);
        }
    }

    @Override
    public void onTerminate(Cluster cluster) {
        System.out.printf("[SERVER %s] Cluster service terminating.%n", ClusterNode.memberName(memberId));
    }
}
