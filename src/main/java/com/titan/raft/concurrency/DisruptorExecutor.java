package com.titan.raft.concurrency;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.titan.raft.protocol.AppendEntriesFlyweight;
import com.titan.raft.protocol.RequestVoteFlyweight;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * DisruptorExecutor drives state machine execution and consensus logic via the LMAX Disruptor
 * ring buffer, enforcing the single-writer principle per Raft group and removing contention
 * across internal worker threads. Handles zero-allocation Flyweight structs over direct ByteBuffers.
 */
public final class DisruptorExecutor implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DisruptorExecutor.class);

    private final int ringBufferSize;
    private final Disruptor<RaftEvent> disruptor;
    private final RingBuffer<RaftEvent> ringBuffer;
    private final AtomicBoolean running;
    private final RaftEventProducer eventProducer;

    @SuppressWarnings("unchecked")
    public DisruptorExecutor(int ringBufferSize, RaftEventHandler eventHandler) {
        if (Integer.bitCount(ringBufferSize) != 0 && (ringBufferSize & (ringBufferSize - 1)) != 0) {
            throw new IllegalArgumentException("ringBufferSize must be a power of two, got: " + ringBufferSize);
        }
        this.ringBufferSize = ringBufferSize;
        this.running = new AtomicBoolean(false);

        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        
        // Initialize LMAX Disruptor with single-producer optimization and sleeping wait strategy for tail latency control
        this.disruptor = new Disruptor<>(
                RaftEvent::new,
                this.ringBufferSize,
                threadFactory,
                ProducerType.SINGLE,
                new SleepingWaitStrategy()
        );

        this.disruptor.handleEventsWith(new DisruptorBatchHandlerAdapter(Objects.requireNonNull(eventHandler, "eventHandler cannot be null")));
        
        this.ringBuffer = this.disruptor.getRingBuffer();
        this.eventProducer = new RaftEventProducer(this.ringBuffer);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting DisruptorExecutor with ring buffer size: {}", ringBufferSize);
            disruptor.start();
        } else {
            logger.warn("DisruptorExecutor is already running.");
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Publishes an AppendEntries payload using zero-allocation Flyweight semantics over direct ByteBuffers.
     * 
     * @param sourceBuffer Direct ByteBuffer containing the network payload
     * @param offset Offset within the direct buffer
     * @param length Length of the payload
     */
    public void publishAppendEntries(ByteBuffer sourceBuffer, int offset, int length) {
        ensureRunning();
        long sequence = ringBuffer.next();
        try {
            RaftEvent event = ringBuffer.get(sequence);
            event.setEventType(RaftEventType.APPEND_ENTRIES);
            event.getAppendEntriesFlyweight().wrap(sourceBuffer, offset, length);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Publishes a RequestVote payload using zero-allocation Flyweight semantics over direct ByteBuffers.
     * 
     * @param sourceBuffer Direct ByteBuffer containing the network payload
     * @param offset Offset within the direct buffer
     * @param length Length of the payload
     */
    public void publishRequestVote(ByteBuffer sourceBuffer, int offset, int length) {
        ensureRunning();
        long sequence = ringBuffer.next();
        try {
            RaftEvent event = ringBuffer.get(sequence);
            event.setEventType(RaftEventType.REQUEST_VOTE);
            event.getRequestVoteFlyweight().wrap(sourceBuffer, offset, length);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Publishes a generic consensus command or state machine transition event.
     */
    public void publishCommand(byte[] commandData, int offset, int length) {
        ensureRunning();
        long sequence = ringBuffer.next();
        try {
            RaftEvent event = ringBuffer.get(sequence);
            event.setEventType(RaftEventType.STATE_MACHINE_COMMAND);
            event.setCommandPayload(commandData, offset, length);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Publishes a heartbeat tick or internal timeout event.
     */
    public void publishHeartbeatTick(long term) {
        ensureRunning();
        long sequence = ringBuffer.next();
        try {
            RaftEvent event = ringBuffer.get(sequence);
            event.setEventType(RaftEventType.HEARTBEAT_TICK);
            event.setTerm(term);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    private void ensureRunning() {
        if (!running.get()) {
            throw new IllegalStateException("DisruptorExecutor is not running. Call start() first.");
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            logger.info("Shutting down DisruptorExecutor...");
            try {
                disruptor.shutdown();
                logger.info("DisruptorExecutor shut down successfully.");
            } catch (Exception e) {
                logger.error("Error during DisruptorExecutor shutdown", e);
            }
        }
    }

    /**
     * Event types handled by the Raft consensus engine.
     */
    public enum RaftEventType {
        APPEND_ENTRIES,
        REQUEST_VOTE,
        STATE_MACHINE_COMMAND,
        HEARTBEAT_TICK,
        SNAPSHOT_CHUNK
    }

    /**
     * Pooled event container inside the Disruptor ring buffer.
     */
    public static final class RaftEvent {
        private RaftEventType eventType;
        private final AppendEntriesFlyweight appendEntriesFlyweight;
        private final RequestVoteFlyweight requestVoteFlyweight;
        private byte[] rawCommandPayload;
        private int commandOffset;
        private int commandLength;
        private long term;

        public RaftEvent() {
            this.appendEntriesFlyweight = new AppendEntriesFlyweight();
            this.requestVoteFlyweight = new RequestVoteFlyweight();
        }

        public void reset() {
            this.eventType = null;
            this.appendEntriesFlyweight.clear();
            this.requestVoteFlyweight.clear();
            this.rawCommandPayload = null;
            this.commandOffset = 0;
            this.commandLength = 0;
            this.term = 0L;
        }

        public RaftEventType getEventType() {
            return eventType;
        }

        public void setEventType(RaftEventType eventType) {
            this.eventType = eventType;
        }

        public AppendEntriesFlyweight getAppendEntriesFlyweight() {
            return appendEntriesFlyweight;
        }

        public RequestVoteFlyweight getRequestVoteFlyweight() {
            return requestVoteFlyweight;
        }

        public void setCommandPayload(byte[] data, int offset, int length) {
            this.rawCommandPayload = data;
            this.commandOffset = offset;
            this.commandLength = length;
        }

        public byte[] getRawCommandPayload() {
            return rawCommandPayload;
        }

        public int getCommandOffset() {
            return commandOffset;
        }

        public int getCommandLength() {
            return commandLength;
        }

        public long getTerm() {
            return term;
        }

        public void setTerm(long term) {
            this.term = term;
        }
    }

    /**
     * Producer wrapper for publication semantics.
     */
    public static final class RaftEventProducer {
        private final RingBuffer<RaftEvent> ringBuffer;

        public RaftEventProducer(RingBuffer<RaftEvent> ringBuffer) {
            this.ringBuffer = ringBuffer;
        }

        public RingBuffer<RaftEvent> getRingBuffer() {
            return ringBuffer;
        }
    }

    /**
     * Interface defining callbacks for raft event handling.
     */
    public interface RaftEventHandler {
        void onEvent(RaftEvent event, long sequence, boolean endOfBatch) throws Exception;
    }

    /**
     * Adapter translating Disruptor batch lifecycle to RaftEventHandler.
     */
    private static final class DisruptorBatchHandlerAdapter implements com.lmax.disruptor.EventHandler<RaftEvent> {
        private final RaftEventHandler delegate;

        private DisruptorBatchHandlerAdapter(RaftEventHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onEvent(RaftEvent event, long sequence, boolean endOfBatch) throws Exception {
            try {
                delegate.onEvent(event, sequence, endOfBatch);
            } finally {
                event.reset();
            }
        }
    }
}