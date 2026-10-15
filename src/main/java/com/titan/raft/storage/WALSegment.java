package com.titan.raft.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/**
 * WALSegment manages a memory-mapped Write-Ahead Log segment file structured as
 * contiguous 64KB log entry blocks with CRC32C checksum trailers, monotonic indices,
 * and variable-length payloads, strictly aligned to 4KB OS page boundaries.
 */
public final class WALSegment implements AutoCloseable {

    public static final int BLOCK_SIZE = 65536; // 64KB block size
    public static final int PAGE_ALIGNMENT = 4096; // 4KB page boundary alignment
    public static final int HEADER_SIZE = 24; // term(8) + index(8) + payloadLength(4) + magic(4)
    public static final int FOOTER_SIZE = 4; // CRC32C(4)
    public static final int MAGIC_NUMBER = 0x54495441; // "TITA"

    private final long baseIndex;
    private final File file;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private ByteBuffer mappedBuffer;
    private long currentWriteOffset;
    private boolean closed;

    public WALSegment(File file, long baseIndex) throws IOException {
        this.file = file;
        this.baseIndex = baseIndex;
        this.closed = false;

        boolean exists = file.exists();
        if (!exists) {
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                if (!parent.mkdirs()) {
                    throw new IOException("Failed to create parent directories for WAL segment: " + file);
                }
            }
        }

        this.randomAccessFile = new RandomAccessFile(file, "rw");
        
        // Ensure file size is at least BLOCK_SIZE and aligned, or expand appropriately.
        long initialSize = exists ? randomAccessFile.length() : BLOCK_SIZE;
        if (initialSize < BLOCK_SIZE) {
            initialSize = BLOCK_SIZE;
        }
        // Round up to multiple of 4KB page alignment
        long remainder = initialSize % PAGE_ALIGNMENT;
        if (remainder != 0) {
            initialSize += (PAGE_ALIGNMENT - remainder);
        }
        randomAccessFile.setLength(initialSize);

        this.fileChannel = randomAccessFile.getChannel();
        this.mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, initialSize);
        this.mappedBuffer.load(); // Hint OS to page-in

        if (exists) {
            this.currentWriteOffset = recoverAndScanValidOffset();
        } else {
            this.currentWriteOffset = 0;
        }
    }

    public synchronized long append(long term, long index, ByteBuffer payload) throws IOException {
        ensureOpen();

        int payloadLength = payload.remaining();
        int totalEntrySize = HEADER_SIZE + payloadLength + FOOTER_SIZE;

        if (currentWriteOffset + totalEntrySize > mappedBuffer.capacity()) {
            expandSegment(currentWriteOffset + totalEntrySize);
        }

        int startPosition = (int) currentWriteOffset;
        mappedBuffer.position(startPosition);

        // Write Header
        mappedBuffer.putLong(term);
        mappedBuffer.putLong(index);
        mappedBuffer.putInt(payloadLength);
        mappedBuffer.putInt(MAGIC_NUMBER);

        // Write Payload
        int payloadBeginOffset = mappedBuffer.position();
        mappedBuffer.put(payload);

        // Compute CRC32C over header and payload
        CRC32C crc32c = new CRC32C();
        // Slice the buffer from startPosition to current position for CRC calculation
        ByteBuffer crcSlice = mappedBuffer.duplicate();
        crcSlice.position(startPosition);
        crcSlice.limit(payloadBeginOffset + payloadLength);
        crc32c.update(crcSlice);
        int checksum = (int) crc32c.getValue();

        // Write Footer (CRC32C checksum)
        mappedBuffer.putInt(checksum);

        currentWriteOffset += totalEntrySize;
        return currentWriteOffset;
    }

    public synchronized void flush() throws IOException {
        ensureOpen();
        mappedBuffer.force();
    }

    public synchronized LogEntry read(long offset) throws IOException {
        ensureOpen();
        if (offset < 0 || offset + HEADER_SIZE > currentWriteOffset) {
            throw new IllegalArgumentException("Invalid read offset: " + offset);
        }

        int pos = (int) offset;
        ByteBuffer buf = mappedBuffer.duplicate();
        buf.position(pos);

        long term = buf.getLong();
        long index = buf.getLong();
        int payloadLength = buf.getInt();
        int magic = buf.getInt();

        if (magic != MAGIC_NUMBER) {
            throw new IOException("Corrupted WAL entry: invalid magic number at offset " + offset);
        }

        int totalSize = HEADER_SIZE + payloadLength + FOOTER_SIZE;
        if (pos + totalSize > currentWriteOffset) {
            throw new IOException("Corrupted WAL entry: entry size exceeds current write offset");
        }

        int payloadBegin = buf.position();
        ByteBuffer payload = buf.duplicate();
        payload.position(payloadBegin);
        payload.limit(payloadBegin + payloadLength);
        ByteBuffer slicedPayload = payload.slice();

        buf.position(payloadBegin + payloadLength);
        int storedChecksum = buf.getInt();

        // Validate CRC32C
        CRC32C crc32c = new CRC32C();
        ByteBuffer crcSlice = mappedBuffer.duplicate();
        crcSlice.position(pos);
        crcSlice.limit(payloadBegin + payloadLength);
        crc32c.update(crcSlice);
        int computedChecksum = (int) crc32c.getValue();

        if (storedChecksum != computedChecksum) {
            throw new IOException("CRC32C checksum mismatch at offset " + offset 
                + ". Stored: " + Integer.toHexString(storedChecksum) 
                + ", Computed: " + Integer.toHexString(computedChecksum));
        }

        return new LogEntry(term, index, slicedPayload);
    }

    private long recoverAndScanValidOffset() throws IOException {
        long scanOffset = 0;
        long lastValidOffset = 0;
        long capacity = mappedBuffer.capacity();

        while (scanOffset + HEADER_SIZE <= capacity) {
            ByteBuffer buf = mappedBuffer.duplicate();
            buf.position((int) scanOffset);

            // Check if remaining space is zeroed out (end of valid writes)
            boolean allZero = true;
            for (int i = 0; i < HEADER_SIZE; i++) {
                if (buf.get() != 0) {
                    allZero = false;
                    break;
                }
            }
            if (allZero) {
                break;
            }

            buf.position((int) scanOffset);
            long term = buf.getLong();
            long index = buf.getLong();
            int payloadLength = buf.getInt();
            int magic = buf.getInt();

            if (magic != MAGIC_NUMBER || payloadLength < 0 || scanOffset + HEADER_SIZE + payloadLength + FOOTER_SIZE > capacity) {
                // Encountered corruption or trailing uncommitted partial write; truncate here.
                break;
            }

            int totalSize = HEADER_SIZE + payloadLength + FOOTER_SIZE;
            int payloadBegin = (int) scanOffset + HEADER_SIZE;

            // Verify checksum
            CRC32C crc32c = new CRC32C();
            ByteBuffer crcSlice = mappedBuffer.duplicate();
            crcSlice.position((int) scanOffset);
            crcSlice.limit(payloadBegin + payloadLength);
            crc32c.update(crcSlice);
            int computedChecksum = (int) crc32c.getValue();

            buf.position(payloadBegin + payloadLength);
            int storedChecksum = buf.getInt();

            if (storedChecksum != computedChecksum) {
                // Checksum failure indicates corrupted or partial write; stop scanning.
                break;
            }

            scanOffset += totalSize;
            lastValidOffset = scanOffset;
        }

        // Zero out any trailing uncommitted partial writes beyond lastValidOffset
        if (lastValidOffset < capacity) {
            ByteBuffer zeroBuf = mappedBuffer.duplicate();
            zeroBuf.position((int) lastValidOffset);
            while (zeroBuf.position() < capacity) {
                zeroBuf.put((byte) 0);
            }
        }

        return lastValidOffset;
    }

    private void expandSegment(long requiredCapacity) throws IOException {
        long newCapacity = mappedBuffer.capacity();
        while (newCapacity < requiredCapacity) {
            newCapacity *= 2; // Double capacity geometrically
        }
        // Ensure alignment to PAGE_ALIGNMENT (4KB)
        long remainder = newCapacity % PAGE_ALIGNMENT;
        if (remainder != 0) {
            newCapacity += (PAGE_ALIGNMENT - remainder);
        }

        long currentPosition = mappedBuffer.position();
        randomAccessFile.setLength(newCapacity);
        this.mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, newCapacity);
        this.mappedBuffer.position((int) currentPosition);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("WALSegment is closed");
        }
    }

    public long getBaseIndex() {
        return baseIndex;
    }

    public synchronized long getCurrentWriteOffset() {
        return currentWriteOffset;
    }

    public File getFile() {
        return file;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            force();
        } finally {
            fileChannel.close();
            randomAccessFile.close();
            // Unmapping direct ByteBuffer in Java requires cleaner API or GC collection.
            // We clear reference to assist GC.
            mappedBuffer = null;
        }
    }

    public static final class LogEntry {
        private final long term;
        private final long index;
        private final ByteBuffer payload;

        public LogEntry(long term, long index, ByteBuffer payload) {
            this.term = term;
            this.index = index;
            this.payload = payload;
        }

        public long getTerm() {
            return term;
        }

        public long getIndex() {
            return index;
        }

        public ByteBuffer getPayload() {
            return payload;
        }
    }
}