package com.titan.raft.consensus;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Production-grade implementation of Raft node state management for 'titan-raft-engine'.
 * Tracks volatile and persistent Raft state variables using atomic primitives and volatile
 * semantics for high-throughput single-writer Disruptor execution loops.
 */
public final class RaftState {

    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private static final VarHandle ROLE_HANDLE;
    private static final VarHandle CURRENT_TERM_HANDLE;
    private static final VarHandle VOTED_FOR_HANDLE;
    private static final VarHandle COMMIT_INDEX_HANDLE;
    private static final VarHandle LAST_APPLIED_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            ROLE_HANDLE = l.findVarHandle(RaftState.class, "role", Role.class);
            CURRENT_TERM_HANDLE = l.findVarHandle(RaftState.class, "currentTerm", long.class);
            VOTED_FOR_HANDLE = l.findVarHandle(RaftState.class, "votedFor", int.class);
            COMMIT_INDEX_HANDLE = l.findVarHandle(RaftState.class, "commitIndex", long.class);
            LAST_APPLIED_HANDLE = l.findVarHandle(RaftState.class, "lastApplied", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final int nodeId;
    private final long clusterEpoch;

    // Persistent state on all servers
    @SuppressWarnings("unused")
    private volatile long currentTerm;
    @SuppressWarnings("unused")
    private volatile int votedFor;

    // Volatile state on all servers
    @SuppressWarnings("unused")
    private volatile long commitIndex;
    @SuppressWarnings("unused")
    private volatile long lastApplied;

    // Volatile state on leaders
    private final long[] nextIndex;
    private final long[] matchIndex;

    // Current Server Role
    @SuppressWarnings("unused")
    private volatile Role role;

    // Leader lease and timing state
    private volatile long lastHeartbeatNanos;
    private volatile long electionTimeoutNanos;

    public RaftState(int nodeId, int clusterSize, long clusterEpoch) {
        if (nodeId < 0) {
            throw new IllegalArgumentException("Node ID must be non-negative");
        }
        if (clusterSize <= 0) {
            throw new IllegalArgumentException("Cluster size must be positive");
        }
        this.nodeId = nodeId;
        this.clusterEpoch = clusterEpoch;
        
        this.currentTerm = 0L;
        this.votedFor = -1;
        this.commitIndex = 0L;
        this.lastApplied = 0L;
        
        this.role = Role.FOLLOWER;
        
        this.nextIndex = new long[clusterSize];
        this.matchIndex = new long[clusterSize];
        
        this.lastHeartbeatNanos = System.nanoTime();
        this.electionTimeoutNanos = computeRandomizedElectionTimeout();
    }

    public int getNodeId() {
        return nodeId;
    }

    public long getClusterEpoch() {
        return clusterEpoch;
    }

    public Role getRole() {
        return (Role) ROLE_HANDLE.getVolatile(this);
    }

    public void setRole(Role newRole) {
        if (newRole == null) {
            throw new IllegalArgumentException("Role cannot be null");
        }
        ROLE_HANDLE.setVolatile(this, newRole);
    }

    public boolean compareAndSetRole(Role expected, Role update) {
        return ROLE_HANDLE.compareAndSet(this, expected, update);
    }

    public long getCurrentTerm() {
        return (long) CURRENT_TERM_HANDLE.getVolatile(this);
    }

    public void setCurrentTerm(long term) {
        if (term < 0) {
            throw new IllegalArgumentException("Term cannot be negative");
        }
        CURRENT_TERM_HANDLE.setVolatile(this, term);
    }

    public boolean updateTermIfGreater(long incomingTerm) {
        while (true) {
            long current = getCurrentTerm();
            if (incomingTerm <= current) {
                return false;
            }
            if (CURRENT_TERM_HANDLE.compareAndSet(this, current, incomingTerm)) {
                setVotedFor(-1);
                setRole(Role.FOLLOWER);
                return true;
            }
        }
    }

    public int getVotedFor() {
        return (int) VOTED_FOR_HANDLE.getVolatile(this);
    }

    public void setVotedFor(int candidateId) {
        VOTED_FOR_HANDLE.setVolatile(this, candidateId);
    }

    public boolean grantVote(long term, int candidateId) {
        while (true) {
            long current = getCurrentTerm();
            if (term < current) {
                return false;
            }
            if (term > current) {
                if (CURRENT_TERM_HANDLE.compareAndSet(this, current, term)) {
                    setVotedFor(-1);
                    setRole(Role.FOLLOWER);
                } else {
                    continue;
                }
            }
            int currentVotedFor = getVotedFor();
            if (currentVotedFor == -1 || currentVotedFor == candidateId) {
                if (VOTED_FOR_HANDLE.compareAndSet(this, currentVotedFor, candidateId)) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    public long getCommitIndex() {
        return (long) COMMIT_INDEX_HANDLE.getVolatile(this);
    }

    public void setCommitIndex(long index) {
        if (index < 0) {
            throw new IllegalArgumentException("Commit index cannot be negative");
        }
        while (true) {
            long current = getCommitIndex();
            if (index <= current) {
                return;
            }
            if (COMMIT_INDEX_HANDLE.compareAndSet(this, current, index)) {
                return;
            }
        }
    }

    public long getLastApplied() {
        return (long) LAST_APPLIED_HANDLE.getVolatile(this);
    }

    public void setLastApplied(long index) {
        if (index < 0) {
            throw new IllegalArgumentException("Last applied index cannot be negative");
        }
        while (true) {
            long current = getLastApplied();
            if (index <= current) {
                return;
            }
            if (LAST_APPLIED_HANDLE.compareAndSet(this, current, index)) {
                return;
            }
        }
    }

    public long getNextIndex(int peerId) {
        validatePeerId(peerId);
        synchronized (nextIndex) {
            return nextIndex[peerId];
        }
    }

    public void setNextIndex(int peerId, long index) {
        validatePeerId(peerId);
        if (index < 0) {
            throw new IllegalArgumentException("Next index cannot be negative");
        }
        synchronized (nextIndex) {
            nextIndex[peerId] = index;
        }
    }

    public long getMatchIndex(int peerId) {
        validatePeerId(peerId);
        synchronized (matchIndex) {
            return matchIndex[peerId];
        }
    }

    public void setMatchIndex(int peerId, long index) {
        validatePeerId(peerId);
        if (index < 0) {
            throw new IllegalArgumentException("Match index cannot be negative");
        }
        synchronized (matchIndex) {
            matchIndex[peerId] = index;
        }
    }

    public void initializeLeaderState(long lastLogIndex, int clusterSize) {
        synchronized (nextIndex) {
            for (int i = 0; i < clusterSize; i++) {
                if (i != nodeId) {
                    nextIndex[i] = lastLogIndex + 1;
                    matchIndex[i] = 0L;
                }
            }
        }
    }

    public long getLastHeartbeatNanos() {
        return lastHeartbeatNanos;
    }

    public void refreshHeartbeat() {
        this.lastHeartbeatNanos = System.nanoTime();
        this.electionTimeoutNanos = computeRandomizedElectionTimeout();
    }

    public boolean isElectionTimeoutExceeded() {
        return (System.nanoTime() - lastHeartbeatNanos) > electionTimeoutNanos;
    }

    private long computeRandomizedElectionTimeout() {
        // Randomized election timeout between 150ms and 300ms in nanoseconds
        long minMs = 150L;
        long maxMs = 300L;
        long randomMs = minMs + (long) (Math.random() * (maxMs - minMs + 1));
        return randomMs * 1_000_000L;
    }

    private void validatePeerId(int peerId) {
        if (peerId < 0 || peerId >= nextIndex.length) {
            throw new IndexOutOfBoundsException("Peer ID " + peerId + " is out of bounds for cluster size " + nextIndex.length);
        }
    }

    public void serializeTo(ByteBuffer buffer) {
        buffer.putLong(getCurrentTerm());
        buffer.putInt(getVotedFor());
        buffer.putLong(getCommitIndex());
        buffer.putLong(getLastApplied());
        buffer.putLong(clusterEpoch);
    }

    public static RaftState deserializeFrom(ByteBuffer buffer, int nodeId, int clusterSize) {
        long term = buffer.getLong();
        int votedFor = buffer.getInt();
        long commitIdx = buffer.getLong();
        long lastApp = buffer.getLong();
        long epoch = buffer.getLong();

        RaftState state = new RaftState(nodeId, clusterSize, epoch);
        state.setCurrentTerm(term);
        state.setVotedFor(votedFor);
        state.setCommitIndex(commitIdx);
        state.setLastApplied(lastApp);
        return state;
    }
}