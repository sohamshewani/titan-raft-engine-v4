package com.titan.raft.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.zip.CRC32C;

public final class RaftNode {

    public enum NodeState {
        FOLLOWER(0),
        CANDIDATE(1),
        LEADER(2);

        private final int value;
        NodeState(int value) {
            this.value = value;
        }
        public int value() {
            return value;
        }
    }

    private static final int ELECTION_TIMEOUT_MIN_MS = 150;
    private static final int ELECTION_TIMEOUT_MAX_MS = 300;

    private final int nodeId;
    private final int clusterSize;
    private final Path walDirectory;

    private volatile int currentState = NodeState.FOLLOWER.value();
    private static final AtomicIntegerFieldUpdater<RaftNode> STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(RaftNode.class, "currentState");

    private volatile long currentTerm = 0L;
    private static final AtomicLongFieldUpdater<RaftNode> TERM_UPDATER =
            AtomicLongFieldUpdater.newUpdater(RaftNode.class, "currentTerm");

    private volatile int votedFor = -1;
    private static final AtomicIntegerFieldUpdater<RaftNode> VOTED_FOR_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(RaftNode.class, "votedFor");

    private volatile long commitIndex = 0L;
    private static final AtomicLongFieldUpdater<RaftNode> COMMIT_INDEX_UPDATER =
            AtomicLongFieldUpdater.newUpdater(RaftNode.class, "commitIndex");

    private volatile long lastApplied = 0L;
    private static final AtomicLongFieldUpdater<RaftNode> LAST_APPLIED_UPDATER =
            AtomicLongFieldUpdater.newUpdater(RaftNode.class, "lastApplied");

    private long lastHeartbeatTimestamp = System.currentTimeMillis();
    private long currentElectionTimeout = computeRandomElectionTimeout();

    private WALSegment activeWalSegment;
    private final ThreadLocal<ByteBuffer> scratchBufferTL = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(65536)
    );

    public RaftNode(int nodeId, int clusterSize, Path walDirectory) {
        this.nodeId = nodeId;
        this.clusterSize = clusterSize;
        this.walDirectory = walDirectory;
    }

    public void initialize() throws IOException {
        recoverWAL();
    }

    private void recoverWAL() throws IOException {
        Path segmentPath = walDirectory.resolve(String.format("wal-%016d.dat", 0));
        this.activeWalSegment = new WALSegment(segmentPath, 65536);
        this.activeWalSegment.recover();
        long maxIndex = activeWalSegment.getLastLogIndex();
        if (maxIndex > 0) {
            LogEntryHeader header = new LogEntryHeader();
            if (activeWalSegment.readEntryHeader(maxIndex, header)) {
                TERM_UPDATER.set(this, header.term);
            }
        }
    }

    private long computeRandomElectionTimeout() {
        return ThreadLocalRandom.current().nextInt(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS + 1);
    }

    public void tick(long currentTimeMs) {
        int state = STATE_UPDATER.get(this);
        if (state == NodeState.LEADER.value()) {
            return;
        }

        if (currentTimeMs - lastHeartbeatTimestamp > currentElectionTimeout) {
            startElection(currentTimeMs);
        }
    }

    private void startElection(long currentTimeMs) {
        long newTerm = TERM_UPDATER.incrementAndGet(this);
        STATE_UPDATER.set(this, NodeState.CANDIDATE.value());
        VOTED_FOR_UPDATER.set(this, nodeId);
        lastHeartbeatTimestamp = currentTimeMs;
        currentElectionTimeout = computeRandomElectionTimeout();

        int votesReceived = 1;
        int requiredQuorum = (clusterSize / 2) + 1;

        if (votesReceived >= requiredQuorum) {
            transitionToLeader();
        }
    }

    private void transitionToLeader() {
        if (STATE_UPDATER.compareAndSet(this, NodeState.CANDIDATE.value(), NodeState.LEADER.value())) {
            lastHeartbeatTimestamp = System.currentTimeMillis();
        }
    }

    public boolean handleAppendEntries(long term, int leaderId, long prevLogIndex, long prevLogTerm, ByteBuffer entriesPayload, long leaderCommit) {
        if (term < TERM_UPDATER.get(this)) {
            return false;
        }

        if (term > TERM_UPDATER.get(this)) {
            TERM_UPDATER.set(this, term);
            STATE_UPDATER.set(this, NodeState.FOLLOWER.value());
            VOTED_FOR_UPDATER.set(this, -1);
        }

        lastHeartbeatTimestamp = System.currentTimeMillis();

        if (prevLogIndex > 0) {
            LogEntryHeader header = new LogEntryHeader();
            try {
                if (!activeWalSegment.readEntryHeader(prevLogIndex, header) || header.term != prevLogTerm) {
                    return false;
                }
            } catch (IOException e) {
                return false;
            }
        }

        if (entriesPayload != null && entriesPayload.remaining() > 0) {
            try {
                long nextIndex = activeWalSegment.getLastLogIndex() + 1;
                appendEntriesInternal(nextIndex, term, entriesPayload);
            } catch (IOException e) {
                return false;
            }
        }

        if (leaderCommit > COMMIT_INDEX_UPDATER.get(this)) {
            long lastNewIndex = activeWalSegment.getLastLogIndex();
            COMMIT_INDEX_UPDATER.set(this, Math.min(leaderCommit, lastNewIndex));
            applyEntries();
        }

        return true;
    }

    public boolean handleRequestVote(long term, int candidateId, long lastLogIndex, long lastLogTerm) {
        long currentT = TERM_UPDATER.get(this);
        if (term < currentT) {
            return false;
        }

        if (term > currentT) {
            TERM_UPDATER.set(this, term);
            STATE_UPDATER.set(this, NodeState.FOLLOWER.value());
            VOTED_FOR_UPDATER.set(this, -1);
        }

        int currentVotedFor = VOTED_FOR_UPDATER.get(this);
        long localLastIndex = activeWalSegment.getLastLogIndex();
        long localLastTerm = 0L;
        if (localLastIndex > 0) {
            LogEntryHeader h = new LogEntryHeader();
            try {
                activeWalSegment.readEntryHeader(localLastIndex, h);
                localLastTerm = h.term;
            } catch (IOException ignored) {}
        }

        boolean logIsUpToDate = (lastLogTerm > localLastTerm) ||
                (lastLogTerm == localLastTerm && lastLogIndex >= localLastIndex);

        if ((currentVotedFor == -1 || currentVotedFor == candidateId) && logIsUpToDate) {
            if (VOTED_FOR_UPDATER.compareAndSet(this, currentVotedFor, candidateId)) {
                lastHeartbeatTimestamp = System.currentTimeMillis();
                return true;
            }
        }

        return false;
    }

    private void appendEntriesInternal(long index, long term, ByteBuffer payload) throws IOException {
        int payloadSize = payload.remaining();
        ByteBuffer scratch = scratchBufferTL.get();
        scratch.clear();

        scratch.putLong(index);
        scratch.putLong(term);
        scratch.putInt(payloadSize);
        scratch.put(payload);

        scratch.flip();
        CRC32C crc = new CRC32C();
        crc.update(scratch);
        long checksum = crc.getValue();

        scratch.putLong(checksum);
        scratch.flip();

        activeWalSegment.append(scratch);
    }

    private void applyEntries() {
        long commitIdx = COMMIT_INDEX_UPDATER.get(this);
        long appliedIdx = LAST_APPLIED_UPDATER.get(this);

        while (appliedIdx < commitIdx) {
            appliedIdx++;
            LAST_APPLIED_UPDATER.set(this, appliedIdx);
        }
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getCurrentState() {
        return STATE_UPDATER.get(this);
    }

    public long getCurrentTerm() {
        return TERM_UPDATER.get(this);
    }

    public static final class LogEntryHeader {
        public long index;
        public long term;
        public int payloadSize;
    }

    public static final class WALSegment {
        private final Path path;
        private final int blockSize;
        private FileChannel channel;
        private ByteBuffer mappedBuffer;
        private long lastLogIndex = 0L;

        public WALSegment(Path path, int blockSize) throws IOException {
            this.path = path;
            this.blockSize = blockSize;
            this.channel = FileChannel.open(path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            this.mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024);
        }

        public void recover() throws IOException {
            mappedBuffer.position(0);
            while (mappedBuffer.remaining() >= 24) {
                int pos = mappedBuffer.position();
                long index = mappedBuffer.getLong();
                long term = mappedBuffer.getLong();
                int size = mappedBuffer.getInt();

                if (index == 0 && term == 0 && size == 0) {
                    mappedBuffer.position(pos);
                    break;
                }

                if (size < 0 || mappedBuffer.remaining() < size + 8) {
                    mappedBuffer.position(pos);
                    break;
                }

                mappedBuffer.position(pos + 20 + size);
                long storedChecksum = mappedBuffer.getLong();

                mappedBuffer.position(pos);
                ByteBuffer checkSlice = mappedBuffer.slice();
                checkSlice.limit(20 + size);
                CRC32C crc = new CRC32C();
                crc.update(checkSlice);

                if (crc.getValue() != storedChecksum) {
                    mappedBuffer.position(pos);
                    break;
                }

                lastLogIndex = index;
                mappedBuffer.position(pos + 28 + size);
            }
        }

        public boolean readEntryHeader(long index, LogEntryHeader outHeader) throws IOException {
            mappedBuffer.position(0);
            while (mappedBuffer.remaining() >= 24) {
                int pos = mappedBuffer.position();
                long idx = mappedBuffer.getLong();
                long term = mappedBuffer.getLong();
                int size = mappedBuffer.getInt();

                if (idx == 0 || size < 0 || mappedBuffer.remaining() < size + 8) {
                    break;
                }

                if (idx == index) {
                    outHeader.index = idx;
                    outHeader.term = term;
                    outHeader.payloadSize = size;
                    return true;
                }

                mappedBuffer.position(pos + 28 + size);
            }
            return false;
        }

        public void append(ByteBuffer entryBuffer) throws IOException {
            int needed = entryBuffer.remaining();
            if (mappedBuffer.remaining() < needed) {
                long newSize = mappedBuffer.capacity() * 2;
                long currentPos = mappedBuffer.position();
                mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, newSize);
                mappedBuffer.position((int) currentPos);
            }
            long index = entryBuffer.getLong(0);
            mappedBuffer.put(entryBuffer);
            lastLogIndex = index;
            mappedBuffer.force();
        }

        public long getLastLogIndex() {
            return lastLogIndex;
        }

        public void close() throws IOException {
            if (channel != null) {
                channel.close();
            }
        }
    }
}