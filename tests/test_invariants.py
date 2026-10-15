package com.titan.raft.engine;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive Unit, Invariant, and Concurrent Stress-Test Suite for 'titan-raft-engine'.
 * 
 * This test suite validates consensus invariants (Election Safety, Leader Append-Only, 
 * Log Matching, Leader Completeness, State Machine Safety), high-concurrency multi-threaded 
 * client contention, failure recovery scenarios, and comprehensive boundary/edge conditions.
 */
public class TitanRaftEngineTestSuite {

    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("titan-raft-test-");
    }

    @AfterEach
    void tearDown() throws IOException {
        deleteRecursively(tempDir.toFile());
    }

    private void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    deleteRecursively(f);
                }
            }
        }
        file.delete();
    }

    // =========================================================================
    // 1. UNIT & BOUNDARY TESTS
    // =========================================================================
    @Nested
    @DisplayName("Unit and Boundary Tests")
    class UnitAndBoundaryTests {

        @Test
        @DisplayName("Node Initialization with Default Configurations")
        void testDefaultInitialization() throws Exception {
            RaftConfig config = new RaftConfig.Builder()
                    .nodeId(1)
                    .clusterPeers(Set.of(2, 3))
                    .storagePath(tempDir.resolve("node-1"))
                    .build();

            try (RaftEngine engine = new RaftEngine(config)) {
                assertEquals(1, engine.getNodeId());
                assertEquals(RaftState.FOLLOWER, engine.getCurrentState());
                assertEquals(0, engine.getCurrentTerm());
                assertEquals(0, engine.getLastLogIndex());
            }
        }

        @ParameterizedTest
        @ValueSource(ints = {-1, 0, Integer.MIN_VALUE})
        @DisplayName("Edge Case: Invalid Node ID Rejection")
        void testInvalidNodeId(int invalidId) {
            RaftConfig.Builder builder = new RaftConfig.Builder()
                    .nodeId(invalidId)
                    .clusterPeers(Set.of(2, 3))
                    .storagePath(tempDir.resolve("invalid-node"));

            assertThrows(IllegalArgumentException.class, builder::build);
        }

        @Test
        @DisplayName("Edge Case: Empty Cluster Peers Configuration")
        void testEmptyClusterPeers() {
            RaftConfig.Builder builder = new RaftConfig.Builder()
                    .nodeId(1)
                    .clusterPeers(Collections.emptySet())
                    .storagePath(tempDir.resolve("empty-peers"));

            assertThrows(IllegalArgumentException.class, builder::build);
        }

        @Test
        @DisplayName("Null Command Rejection during Client Write")
        void testNullCommandRejection() throws Exception {
            RaftConfig config = new RaftConfig.Builder()
                    .nodeId(1)
                    .clusterPeers(Set.of(2))
                    .storagePath(tempDir.resolve("node-1"))
                    .build();

            try (RaftEngine engine = new RaftEngine(config)) {
                assertThrows(NullPointerException.class, () -> engine.propose(null));
            }
        }

        @Test
        @DisplayName("Empty Command Proposal Handling")
        void testEmptyCommandProposal() throws Exception {
            RaftConfig config = new RaftConfig.Builder()
                    .nodeId(1)
                    .clusterPeers(Set.of(2))
                    .storagePath(tempDir.resolve("node-1"))
                    .build();

            try (RaftEngine engine = new RaftEngine(config)) {
                // Force engine to leader state for testing direct proposal execution paths
                engine.forceStateForTesting(RaftState.LEADER, 1);
                byte[] emptyCommand = new byte[0];
                CompletableFutureResult<LogEntry> result = engine.propose(emptyCommand);
                assertNotNull(result);
            }
        }
    }

    // =========================================================================
    // 2. INVARIANT VERIFICATION TESTS
    // =========================================================================
    @Nested
    @DisplayName("Raft Consensus Invariant Tests")
    class InvariantVerificationTests {

        @Test
        @DisplayName("Election Safety: At most one leader can be elected per term")
        @Timeout(10)
        void testElectionSafety() throws Exception {
            int clusterSize = 3;
            List<RaftEngine> cluster = new ArrayList<>();
            Set<Integer> peers = Set.of(1, 2, 3);

            try {
                for (int i = 1; i <= clusterSize; i++) {
                    RaftConfig config = new RaftConfig.Builder()
                            .nodeId(i)
                            .clusterPeers(peers)
                            .storagePath(tempDir.resolve("node-" + i))
                            .electionTimeoutMin(150)
                            .electionTimeoutMax(300)
                            .heartbeatInterval(50)
                            .build();
                    cluster.add(new RaftEngine(config));
                }

                // Wire up network transport layer simulation
                MockNetwork network = new MockNetwork(cluster);
                for (RaftEngine engine : cluster) {
                    engine.setTransport(network);
                    engine.start();
                }

                // Allow time for elections
                Thread.sleep(1000);

                // Verify invariant: Count leaders across all terms/nodes
                Map<Long, Integer> leadersPerTerm = new ConcurrentHashMap<>();
                for (RaftEngine engine : cluster) {
                    if (engine.getCurrentState() == RaftState.LEADER) {
                        long term = engine.getCurrentTerm();
                        leadersPerTerm.merge(term, 1, Integer::sum);
                    }
                }

                for (Map.Entry<Long, Integer> entry : leadersPerTerm.entrySet()) {
                    assertTrue(entry.getValue() <= 1, 
                        "Election Safety Violated: Term " + entry.getKey() + " has " + entry.getValue() + " leaders!");
                }

            } finally {
                for (RaftEngine engine : cluster) {
                    engine.close();
                }
            }
        }

        @Test
        @DisplayName("Leader Append-Only: A leader never overwrites or truncates its own entries")
        @Timeout(10)
        void testLeaderAppendOnly() throws Exception {
            RaftConfig config = new RaftConfig.Builder()
                    .nodeId(1)
                    .clusterPeers(Set.of(2, 3))
                    .storagePath(tempDir.resolve("node-1"))
                    .build();

            try (RaftEngine engine = new RaftEngine(config)) {
                engine.forceStateForTesting(RaftState.LEADER, 1);

                List<LogEntry> initialLog = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    CompletableFutureResult<LogEntry> res = engine.propose(("cmd-" + i).getBytes());
                    initialLog.add(res.get(1, TimeUnit.SECONDS));
                }

                List<LogEntry> snapshotBefore = engine.getLogEntries();

                // Trigger operations that might stress log integrity
                engine.handleClientHeartbeat();

                List<LogEntry> snapshotAfter = engine.getLogEntries();

                assertEquals(snapshotBefore.size(), snapshotAfter.size());
                for (int i = 0; i < snapshotBefore.size(); i++) {
                    assertEquals(snapshotBefore.get(i).getIndex(), snapshotAfter.get(i).getIndex());
                    assertEquals(snapshotBefore.get(i).getTerm(), snapshotAfter.get(i).getTerm());
                    assertArrayEquals(snapshotBefore.get(i).getCommand(), snapshotAfter.get(i).getCommand());
                }
            }
        }

        @Test
        @DisplayName("Log Matching Invariant: If two logs contain an entry with the same index and term, they are identical up to that index")
        void testLogMatchingInvariant() throws Exception {
            int clusterSize = 3;
            List<RaftEngine> cluster = new ArrayList<>();
            Set<Integer> peers = Set.of(1, 2, 3);

            try {
                for (int i = 1; i <= clusterSize; i++) {
                    RaftConfig config = new RaftConfig.Builder()
                            .nodeId(i)
                            .clusterPeers(peers)
                            .storagePath(tempDir.resolve("node-" + i))
                            .build();
                    cluster.add(new RaftEngine(config));
                }

                MockNetwork network = new MockNetwork(cluster);
                for (RaftEngine engine : cluster) {
                    engine.setTransport(network);
                    engine.start();
                }

                cluster.get(0).forceStateForTesting(RaftState.LEADER, 1);
                cluster.get(0).propose("test-command".getBytes());

                Thread.sleep(500);

                // Verify log matching across nodes
                List<LogEntry> referenceLog = cluster.get(0).getLogEntries();
                for (RaftEngine engine : cluster) {
                    List<LogEntry> peerLog = engine.getLogEntries();
                    for (int i = 0; i < Math.min(referenceLog.size(), peerLog.size()); i++) {
                        LogEntry refEntry = referenceLog.get(i);
                        LogEntry peerEntry = peerLog.get(i);
                        if (refEntry.getIndex() == peerEntry.getIndex()) {
                            assertEquals(refEntry.getTerm(), peerEntry.getTerm(), 
                                "Log Matching Invariant Violated at index " + refEntry.getIndex());
                        }
                    }
                }
            } finally {
                for (RaftEngine engine : cluster) {
                    engine.close();
                }
            }
        }
    }

    // =========================================================================
    // 3. CONCURRENT STRESS & RACE CONDITION TESTS
    // =========================================================================
    @Nested
    @DisplayName("Concurrent Stress and Race Condition Tests")
    class ConcurrentStressTests {

        @RepeatedTest(5)
        @DisplayName("Multi-threaded Concurrent Proposals Under High Contention")
        @Timeout(15)
        void testHighContentionConcurrentProposals() throws Exception {
            RaftConfig config = new RaftConfig.Builder()
                    .nodeId(1)
                    .clusterPeers(Set.of(2, 3))
                    .storagePath(tempDir.resolve("node-1"))
                    .build();

            int threadCount = 16;
            int proposalsPerThread = 100;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failureCount = new AtomicInteger(0);

            try (RaftEngine engine = new RaftEngine(config)) {
                engine.forceStateForTesting(RaftState.LEADER, 1);

                List<Callable<Void>> tasks = new ArrayList<>();
                for (int t = 0; t < threadCount; t