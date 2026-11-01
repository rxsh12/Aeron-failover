package demo.aeron;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

final class DurableOutbox implements AutoCloseable {
    static final int PAYLOAD_LENGTH = 100;
    private static final int MAGIC = 0x4f425831;
    private static final int RECORD_LENGTH = Integer.BYTES + PAYLOAD_LENGTH + Integer.BYTES;

    private final Path journalPath;
    private final Path commitPath;
    private final FileChannel journal;
    private final Map<Long, Entry> entries = new HashMap<>();
    private final List<Entry> orderedEntries = new ArrayList<>();
    private long committedCount;

    DurableOutbox(Path journalPath) throws IOException {
        this.journalPath = journalPath.toAbsolutePath();
        this.commitPath = this.journalPath.resolveSibling(this.journalPath.getFileName() + ".commit");
        Files.createDirectories(this.journalPath.getParent());
        boolean journalExisted = Files.exists(this.journalPath);
        journal = FileChannel.open(this.journalPath,
            StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        if (!journalExisted) {
            forceDirectory(this.journalPath.getParent());
        }
        recoverJournal();
        committedCount = readCommitCount();
        if (committedCount > entries.size()) {
            throw new IOException("Outbox commit cursor exceeds the recovered journal length");
        }
    }

    synchronized void append(byte[] payload) throws IOException {
        appendBatch(List.of(payload));
    }

    synchronized int appendBatch(List<byte[]> payloads) throws IOException {
        Map<Long, byte[]> newEntries = new LinkedHashMap<>();
        for (byte[] payload : payloads) {
            if (payload.length != PAYLOAD_LENGTH) {
                throw new IOException("Producer payload must be exactly 100 bytes");
            }
            long id = ByteBuffer.wrap(payload).getLong();
            if (id <= 0) {
                throw new IOException("Producer message ID must be positive");
            }
            Entry existing = entries.get(id);
            byte[] pending = newEntries.get(id);
            byte[] previous = existing == null ? pending : existing.payload;
            if (previous != null) {
                if (!Arrays.equals(previous, payload)) {
                    throw new IOException("Message ID " + id + " was already used with different bytes");
                }
                continue;
            }
            newEntries.put(id, payload.clone());
        }

        if (newEntries.isEmpty()) {
            return 0;
        }
        ByteBuffer records = ByteBuffer.allocate(Math.multiplyExact(newEntries.size(), RECORD_LENGTH));
        for (byte[] payload : newEntries.values()) {
            records.put(encodeRecord(payload));
        }
        records.flip();
        journal.position(journal.size());
        writeFully(journal, records);
        journal.force(true);

        for (Map.Entry<Long, byte[]> entry : newEntries.entrySet()) {
            Entry stored = new Entry(orderedEntries.size(), entry.getValue());
            entries.put(entry.getKey(), stored);
            orderedEntries.add(stored);
        }
        return newEntries.size();
    }

    synchronized byte[] nextPending() {
        return committedCount < orderedEntries.size() ?
            orderedEntries.get((int)committedCount).payload.clone() : null;
    }

    synchronized void markCommitted(long id) throws IOException {
        Entry entry = entries.get(id);
        if (entry == null || entry.index != committedCount) {
            throw new IOException("Cluster acknowledged unexpected outbox message ID " + id);
        }
        persistCommitCount(committedCount + 1);
        committedCount++;
    }

    private void recoverJournal() throws IOException {
        long size = journal.size();
        long completeSize = size - size % RECORD_LENGTH;
        if (completeSize != size) {
            journal.truncate(completeSize);
            journal.force(true);
        }
        ByteBuffer record = ByteBuffer.allocate(RECORD_LENGTH);
        for (long offset = 0; offset < completeSize; offset += RECORD_LENGTH) {
            record.clear();
            readFully(journal, record, offset);
            record.flip();
            int magic = record.getInt();
            byte[] payload = new byte[PAYLOAD_LENGTH];
            record.get(payload);
            int storedCrc = record.getInt();
            if (magic != MAGIC || storedCrc != checksum(magic, payload)) {
                throw new IOException("Invalid durable outbox record at byte offset " + offset);
            }
            long id = ByteBuffer.wrap(payload).getLong();
            Entry entry = new Entry(orderedEntries.size(), payload);
            if (id <= 0 || entries.putIfAbsent(id, entry) != null) {
                throw new IOException("Invalid or duplicate message ID in durable outbox at byte offset " + offset);
            }
            orderedEntries.add(entry);
        }
    }

    private long readCommitCount() throws IOException {
        if (!Files.exists(commitPath)) {
            persistCommitCount(0);
            return 0;
        }
        try {
            long count = Long.parseLong(Files.readString(commitPath, StandardCharsets.US_ASCII).trim());
            if (count < 0) {
                throw new IOException("Negative outbox commit cursor: " + commitPath);
            }
            return count;
        } catch (NumberFormatException e) {
            throw new IOException("Invalid outbox commit cursor: " + commitPath, e);
        }
    }

    private void persistCommitCount(long count) throws IOException {
        Path temporary = commitPath.resolveSibling(commitPath.getFileName() + ".tmp");
        byte[] bytes = (Long.toString(count) + "\n").getBytes(StandardCharsets.US_ASCII);
        try (FileChannel output = FileChannel.open(temporary,
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            writeFully(output, ByteBuffer.wrap(bytes));
            output.force(true);
        }
        Files.move(temporary, commitPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        forceDirectory(commitPath.getParent());
    }

    private static void forceDirectory(Path directoryPath) throws IOException {
        try (FileChannel directory = FileChannel.open(directoryPath, StandardOpenOption.READ)) {
            directory.force(true);
        }
    }

    private static ByteBuffer encodeRecord(byte[] payload) {
        ByteBuffer record = ByteBuffer.allocate(RECORD_LENGTH);
        record.putInt(MAGIC).put(payload).putInt(checksum(MAGIC, payload)).flip();
        return record;
    }

    private static int checksum(int magic, byte[] payload) {
        CRC32 crc = new CRC32();
        ByteBuffer prefix = ByteBuffer.allocate(Integer.BYTES).putInt(magic);
        crc.update(prefix.array());
        crc.update(payload);
        return (int)crc.getValue();
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new IOException("Unexpected end of durable outbox");
            }
            position += read;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        journal.close();
    }

    private record Entry(long index, byte[] payload) {
    }
}
