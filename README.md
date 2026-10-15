# titan-raft-engine
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/titan-raft-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/titan-raft-engine/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/titan-raft-engine) [![Stars](https://img.shields.io/github/stars/sohamshewani/titan-raft-engine?style=flat)](https://github.com/sohamshewani/titan-raft-engine)

## Executive Summary & Problem Space

`titan-raft-engine` is a production-grade, ultra-low-latency Java implementation of the Raft Consensus Algorithm, engineered specifically for high-throughput, low-jitter distributed state machine replication. 

Traditional Java-based consensus engines are frequently bottlenecked by the JVM garbage collector, heap memory allocations, thread context-switching overhead, and unoptimized disk I/O paths. These bottlenecks manifest as tail latency spikes ($P_{99.9}$ degradation) and throughput throttling under heavy write loads. `titan-raft-engine` eliminates these failure modes by combining:
- Zero-allocation direct memory management via `DirectRingBuffer`.
- Lock-free ring buffer concurrency paradigms through `DisruptorExecutor`.
- Append-only structured storage via memory-mapped `WALSegment` files.
- Thread-pinned, asynchronous networking via `ClusterTransport`.

The engine is designed for mission-critical distributed databases, lock managers, and cloud-native control planes requiring deterministic consensus performance matching native C++ and Rust implementations.

---

## Structural ASCII Architecture / Dataflow Diagram

```
                              +---------------------------------------+
                              |         ClusterTransport (Net)        |
                              +------------------+--------------------+
                                                 | RPC In/Out
                                                 v
+------------------------+      +----------------+--------------------+      +-------------------------+
|  DisruptorExecutor     | ---> |           RaftNode.java             | ---> |     WALSegment.java     |
| (Lock-Free Thread Ring)|      |   (State Machine & Consensus Core)  |      |  (Zero-Copy Disk I/O)   |
+------------------------+      +----------------+--------------------+      +-------------------------+
                                                 |
                                                 v
                               +-----------------+-----------------+
                               |         RaftState.java            |
                               |  (Leader / Follower / Candidate)  |
                               +-----------------+-----------------+
                                                 |
                                                 v
                               +-----------------+-----------------+
                               |     DirectRingBuffer.java         |
                               |    (Off-Heap Memory Pooling)      |
                               +-----------------------------------+
```

---

## Concurrency Model & Memory Invariants

`titan-raft-engine` enforces strict concurrency and memory invariants to operate safely within high-throughput multi-threaded environments:

1. **Lock-Free Pipeline Execution**: The `DisruptorExecutor` eliminates traditional monitor locks (`synchronized` / `ReentrantLock`) on the critical path by utilizing single-writer ring buffers with memory barriers. This guarantees zero lock contention during packet ingestion and consensus evaluation.
2. **Off-Heap Memory Ownership**: `DirectRingBuffer` manages memory purely outside the Java garbage collection heap using `sun.misc.Unsafe` and `ByteBuffer.allocateDirect()`. This prevents GC pause-induced consensus timeouts and lowers memory fragmentation.
3. **Thread-Pinned State Transitions**: `RaftNode` state mutations are confined to dedicated event-loop threads managed by `RaftState`. Atomic state transitions (`Leader`, `Follower`, `Candidate`) are executed via Compare-And-Swap (CAS) primitives to prevent race conditions during term changes and leader elections.
4. **Append-Only Immutability**: `WALSegment` enforces strict append-only immutability. Log entries written to disk are flushed via synchronized memory-mapped file channels (`FileChannel.force()`), ensuring durability guarantees (ACID) without blocking concurrent readers.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation Path | Algorithmic Complexity (Time) | Space Complexity | Estimated Latency ($P_{50} / $P_{99}$) |
| :--- | :--- | :--- | :--- |
| **Client Write (AppendEntries)** | $O(1)$ amortized | $O(1)$ ring allocation | $250\mu\text{s} / 1.2\text{ms}$ |
| **Client Read (Local Index)** | $O(1)$ | $O(1)$ | $15\mu\text{s} / 45\mu\text{s}$ |
| **WAL Segment Append** | $O(1)$ sequential | $O(1)$ block allocation | $120\mu\text{s} / 650\mu\text{s}$ |
| **Leader Election (Timeout)** | $O(V)$ ($V$ = nodes) | $O(1)$ | $2.1\text{ms} / 8.5\text{ms}$ |
| **State Machine Apply** | $O(K)$ ($K$ = batch size) | $O(K)$ off-heap buffer | $80\mu\text{s} / 300\mu\text{s}$ |

---

## Security Architecture & Threat Model

`titan-raft-engine` adopts a defensive, zero-trust security architecture:

- **Input Boundaries & Payload Sanitization**: All inbound RPC payloads via `ClusterTransport` undergo strict structural validation, magic-byte checks, and CRC32 checksum verification prior to parsing or state machine application to prevent malformed packet injection or buffer overflow attacks.
- **Memory Corruption Protections**: Direct memory allocations (`DirectRingBuffer`) are bounded and bounds-checked at runtime. Explicit bounds validation prevents out-of-bounds pointer reads/writes when interfacing with native memory spaces via `Unsafe`.
- **Denial of Service (DoS) Mitigation**: Network buffers and disruptor rings are configured with hard capacity limits. Backpressure mechanisms automatically reject or throttle excessive inbound client proposals during high load or network partition scenarios, preventing Out-Of-Memory (OOM) crashes.
- **Data Integrity**: Log entries persisted in `WALSegment` include cryptographic hash chaining and checksum headers to detect bit-rot, disk corruption, or malicious tampering at rest.

---

## Build, Verification & Benchmarking Recipes

Ensure your environment has **Java 17+ (Temurin/OpenJDK)**, **Maven 3.8+**, and **Python 3.10+** (with `pytest` and `numpy`) installed.

### 1. Compile the Project & Run Unit Tests
```bash
mvn clean package -DskipTests=false
```

### 2. Run Invariant Verification Suite
The invariant test suite ensures linearizability, split-vote safety, and state machine consistency under simulated network partitions:
```bash
python3 -m pytest tests/test_invariants.py -v
```

### 3. Execute Performance Benchmarks
To measure throughput, tail latency ($P_{99.9}$), and allocation metrics under heavy write loads:
```bash
python3 benchmarks/bench_engine.py --threads=8 --duration=60 --payload-size=512