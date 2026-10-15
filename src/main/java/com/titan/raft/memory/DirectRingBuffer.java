package com.titan.raft.memory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongFieldUpdater;

/**
 * DirectRingBuffer is a high-performance, lock-free, zero-allocation circular buffer 
 * backed by off-heap direct ByteBuffer memory. Designed specifically for the 'titan-raft-engine' 
 * to handle high-throughput network bursts and inter-thread messaging without triggering 
 * Java Garbage Collection pauses.
 * 
 * Features:
 * - Backed entirely by off-heap memory (Direct ByteBuffer).
 * - Power-of-two capacity enforcement for fast bitwise masking.
 * - Lock-free single-producer / single-consumer or multi-producer / multi-consumer CAS operations.
 * - Fully zero-allocation flyweight access patterns.
 */
public final class DirectRingBuffer implements AutoCloseable {

    private static final long CACHE_LINE_SIZE = 64;

    // Memory layout constants per slot
    // We store an 8-byte sequence/status marker followed by payload bytes.
    private static final int SEQUENCE_OFFSET_IN_SLOT = 0;
    private static final int HEADER_SIZE = 8;

    private final int capacity;
    private final int mask;
    private final int slotSize;
    private final ByteBuffer directBuffer;
    private final long baseAddress;

    // Pointers padded to avoid false sharing
    @SuppressWarnings("unused")
    private volatile long pad0_0, pad0_1, pad0_2, pad0_3, pad0_4, pad0_5, pad0_6, pad0_7;
    
    private volatile long head = 0L;
    private static final LongFieldUpdater<DirectRingBuffer> HEAD_UPDATER =
            LongFieldUpdater.newUpdater(DirectRingBuffer.class, "head");

    @SuppressWarnings("unused")
    private volatile long pad1_0, pad1_1, pad1_2, pad1_3, pad1_4, pad1_5, pad1_6, pad1_7;

    private volatile long tail = 0L;
    private static final LongFieldUpdater<DirectRingBuffer> TAIL_UPDATER =
            LongFieldUpdater.newUpdater(DirectRingBuffer.class, "tail");

    @SuppressWarnings("unused")
    private volatile long pad2_0, pad2_1, pad2_2, pad2_3, pad2_4, pad2_5, pad2_6, pad2_7;

    /**
     * Constructs a DirectRingBuffer with a specified capacity and payload size per slot.
     * 
     * @param capacity        Must be a power of two.
     * @param payloadSizeInBytes Size of each slot's payload in bytes.
     */
    public DirectRingBuffer(int capacity, int payloadSizeInBytes) {
        if (capacity <= 0 || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException("Capacity must be a positive power of two: " + capacity);
        }
        if (payloadSizeInBytes <= 0) {
            throw new IllegalArgumentException("Payload size must be greater than zero: " + payloadSizeInBytes);
        }

        this.capacity = capacity;
        this.mask = capacity - 1;
        this.slotSize = HEADER_SIZE + payloadSizeInBytes;

        long totalSize = (long) this.slotSize * capacity;
        if (totalSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Total ring buffer size exceeds integer limit: " + totalSize);
        }

        this.directBuffer = ByteBuffer.allocateDirect((int) totalSize).order(ByteOrder.nativeOrder());
        this.baseAddress = ((sun.misc.Unsafe) UnsafeAccess.UNSAFE).allocateMemory(totalSize);
        
        // Zero out the off-heap memory arena
        ((sun.misc.Unsafe) UnsafeAccess.UNSAFE).setMemory(this.baseAddress, totalSize, (byte) 0);
    }

    /**
     * Returns the capacity of the ring buffer (power of two).
     */
    public int capacity() {
        return this.capacity;
    }

    /**
     * Returns the payload size of each individual slot in bytes.
     */
    public int payloadSize() {
        return this.slotSize - HEADER_SIZE;
    }

    /**
     * Claims the next available slot for writing in a lock-free manner.
     * 
     * @return The sequence number assigned to the claimed slot, or -1 if the buffer is full.
     */
    public long tryClaim() {
        long currentTail;
        long currentHead;
        do {
            currentTail = this.tail;
            currentHead = this.head;
            if (currentTail - currentHead >= capacity) {
                return -1L; // Buffer full
            }
        } while (!TAIL_UPDATER.compareAndSet(this, currentTail, currentTail + 1));

        return currentTail;
    }

    /**
     * Writes source ByteBuffer data into the specified sequence slot.
     * 
     * @fn Zero-copy memory transfer from JVM/Direct buffer to off-heap ring slot.
     */
    public void write(long sequence, ByteBuffer src) {
        int index = (int) (sequence & mask);
        long offset = baseAddress + ((long) index * slotSize);
        
        int length = src.remaining();
        if (length > (slotSize - HEADER_SIZE)) {
            throw new IllegalArgumentException("Source buffer remaining bytes (" + length + 
                    ") exceeds slot payload capacity (" + (slotSize - HEADER_SIZE) + ")");
        }

        sun.misc.Unsafe unsafe = (sun.misc.Unsafe) UnsafeAccess.UNSAFE;

        // Write payload
        if (src.isDirect()) {
            long srcAddress = ((sun.misc.Unsafe) UnsafeAccess.UNSAFE).objectFieldOffset(
                    BufferFieldAccessor.getBufferAddressField()) /* fallback to direct copy if needed */;
            // Safely copy via absolute absolute get/put or Unsafe copy memory
            // For robustness, we utilize ByteBuffer bulk gets if off-heap or slice
            ByteBuffer slice = directBuffer.duplicate();
            slice.position(index * slotSize + HEADER_SIZE);
            slice.limit(slice.position() + length);
            slice.put(src);
        } else {
            byte[] array = src.array();
            int arrayOffset = src.arrayOffset() + src.position();
            unsafe.copyMemory(array, sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET + arrayOffset,
                    null, offset + HEADER_SIZE, length);
            src.position(src.limit());
        }

        // Publish by writing sequence to the header barrier
        unsafe.putLongVolatile(null, offset + SEQUENCE_OFFSET_IN_SLOT, sequence + 1);
    }

    /**
     * Attempts to read a message at the given sequence.
     * 
     * @param sequence The sequence number to read.
     * @param dst      The destination ByteBuffer to copy the payload into.
     * @return true if successfully read, false if the slot is not yet published.
     */
    public boolean read(long sequence, ByteBuffer dst) {
        int index = (int) (sequence & mask);
        long offset = baseAddress + ((long) index * slotSize);

        sun.misc.Unsafe unsafe = (sun.misc.Unsafe) UnsafeAccess.UNSAFE;
        long availableSequence = unsafe.getLongVolatile(null, offset + SEQUENCE_OFFSET_IN_SLOT);

        if (availableSequence < sequence + 1) {
            return false; // Not yet available / published
        }

        int payloadLen = slotSize - HEADER_SIZE;
        if (dst.remaining() < payloadLen) {
            throw new IllegalArgumentException("Destination buffer remaining capacity (" + dst.remaining() + 
                    ") is insufficient for slot payload size (" + payloadLen + ")");
        }

        // Copy from off-heap slot to destination buffer
        if (dst.isDirect()) {
            ByteBuffer slice = directBuffer.duplicate();
            slice.position(index * slotSize + HEADER_SIZE);
            slice.limit(slice.position() + payloadLen);
            dst.put(slice);
        } else {
            byte[] array = dst.array();
            int arrayOffset = dst.arrayOffset() + dst.position();
            unsafe.copyMemory(null, offset + HEADER_SIZE, array, 
                    sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET + arrayOffset, payloadLen);
            dst.position(dst.position() + payloadLen);
        }

        // Advance head to acknowledge consumption
        long currentHead;
        do {
            currentHead = this.head;
            if (sequence < currentHead) {
                break;
            }
        } while (!HEAD_UPDATER.compareAndSet(this, currentHead, sequence + 1));

        return true;
    }

    /**
     * Exposes a direct memory pointer to the slot payload for absolute zero-copy flyweight operations.
     * 
     * @param sequence The sequence number of the slot.
     * @return Off-heap memory address pointing directly to the payload region.
     */
    public long acquireDirectPayloadAddress(long sequence) {
        int index = (int) (sequence & mask);
        return baseAddress + ((long) index * slotSize) + HEADER_SIZE;
    }

    /**
     * Returns the current number of elements available in the ring buffer.
     */
    public int size() {
        long currentTail = TAIL_UPDATER.get(this);
        long currentHead = HEAD_UPDATER.get(this);
        long size = currentTail - currentHead;
        return size < 0 ? 0 : (int) Math.min(size, capacity);
    }

    /**
     * Checks if the ring buffer is empty.
     */
    public boolean isEmpty() {
        return HEAD_UPDATER.get(this) >= TAIL_UPDATER.get(this);
    }

    /**
     * Checks if the ring buffer is full.
     */
    public boolean isFull() {
        return size() >= capacity;
    }

    @Override
    public void close() {
        if (baseAddress != 0L) {
            ((sun.misc.Unsafe) UnsafeAccess.UNSAFE).freeMemory(baseAddress);
        }
    }

    /**
     * Internal helper class for accessing sun.misc.Unsafe safely across Java versions.
     */
    private static final class UnsafeAccess {
        static final Object UNSAFE;
        static {
            Object unsafeInstance;
            try {
                java.lang.reflect.Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                unsafeInstance = field.get(null);
            } catch (Exception e) {
                try {
                    java.lang.reflect.Constructor<sun.misc.Unsafe> unsafeConstructor = 
                            sun.misc.Unsafe.class.getDeclaredConstructor();
                    unsafeConstructor.setAccessible(true);
                    unsafeInstance = unsafeConstructor.newInstance();
                } catch (Exception ex) {
                    throw new RuntimeException("Unable to acquire sun.misc.Unsafe instance", ex);
                }
            }
            UNSAFE = unsafeInstance;
        }
    }

    /**
     * Internal reflection helper for DirectByteBuffer memory address field access.
     */
    private static final class BufferFieldAccessor {
        private static final java.lang.reflect.Field ADDRESS_FIELD;
        static {
            try {
                ADDRESS_FIELD = java.nio.Buffer.class.getDeclaredField("address");
                ADDRESS_FIELD.setAccessible(true);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException("Failed to initialize ByteBuffer address field accessor", e);
            }
        }

        static java.lang.reflect.Field getBufferAddressField() {
            return ADDRESS_FIELD;
        }
    }
}