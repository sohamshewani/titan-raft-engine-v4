package com.titan.raft.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public final class ClusterTransport implements AutoCloseable {

    private static final int BUFFER_CAPACITY = 65536;
    private static final int MAGIC_HEADER = 0x5449544E; // "TITN"

    private final int nodeId;
    private final InetSocketAddress bindAddress;
    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final Thread selectorThread;
    private final AtomicBoolean running;

    private final ConcurrentHashMap<Integer, SocketChannel> outboundChannels;
    private final ConcurrentHashMap<Integer, InetSocketAddress> peerAddresses;
    
    private final ThreadLocal<ByteBuffer> sendBufferPool;
    private final ThreadLocal<ByteBuffer> receiveBufferPool;
    
    private volatile MessageHandler messageHandler;

    public interface MessageHandler {
        void onMessage(int sourceNodeId, ByteBuffer payload);
    }

    public ClusterTransport(int nodeId, InetSocketAddress bindAddress) throws IOException {
        this.nodeId = nodeId;
        this.bindAddress = bindAddress;
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        this.serverChannel.bind(bindAddress);
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        this.running = new AtomicBoolean(true);
        this.outboundChannels = new ConcurrentHashMap<>();
        this.peerAddresses = new ConcurrentHashMap<>();

        this.sendBufferPool = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(BUFFER_CAPACITY));
        this.receiveBufferPool = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(BUFFER_CAPACITY));

        this.selectorThread = new Thread(this::eventLoop, "titan-transport-selector-" + nodeId);
        this.selectorThread.setDaemon(true);
        this.selectorThread.start();
    }

    public void setMessageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    public void registerPeer(int peerId, InetSocketAddress address) {
        peerAddresses.put(peerId, address);
    }

    public void send(int targetNodeId, ByteBuffer payload) throws IOException {
        SocketChannel channel = outboundChannels.get(targetNodeId);
        if (channel == null || !channel.isOpen()) {
            InetSocketAddress address = peerAddresses.get(targetNodeId);
            if (address == null) {
                throw new IOException("Target node " + targetNodeId + " address not registered.");
            }
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(address);
            
            // Register with selector for connection completion
            synchronized (selector) {
                selector.wakeup();
                channel.register(selector, SelectionKey.OP_CONNECT, targetNodeId);
            }
            outboundChannels.put(targetNodeId, channel);
        }

        int remaining = payload.remaining();
        ByteBuffer sendBuf = sendBufferPool.get();
        sendBuf.clear();
        
        if (sendBuf.capacity() < remaining + 8) {
            throw new IOException("Payload exceeds direct buffer pool capacity: " + remaining);
        }

        sendBuf.putInt(MAGIC_HEADER);
        sendBuf.putInt(remaining);
        sendBuf.put(payload);
        sendBuf.flip();

        while (sendBuf.hasRemaining()) {
            long written = channel.write(sendBuf);
            if (written < 0) {
                throw new IOException("Connection closed by peer during write: " + targetNodeId);
            }
            if (written == 0) {
                LockSupport.parkNanos(1000); // Backoff on full socket buffer
            }
        }
    }

    private void eventLoop() {
        while (running.get()) {
            try {
                selector.select(10);
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isAcceptable()) {
                        handleAccept();
                    } else if (key.isConnectable()) {
                        handleConnect(key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    }
                }
            } catch (IOException e) {
                if (!running.get()) {
                    break;
                }
            }
        }
    }

    private void handleAccept() throws IOException {
        SocketChannel clientChannel = serverChannel.accept();
        if (clientChannel != null) {
            clientChannel.configureBlocking(false);
            clientChannel.register(selector, SelectionKey.OP_READ);
        }
    }

    private void handleConnect(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            if (channel.isConnectionPending()) {
                channel.finishConnect();
            }
            key.interestOps(SelectionKey.OP_READ);
        } catch (IOException e) {
            key.cancel();
            try {
                channel.close();
            } catch (IOException ignored) {}
        }
    }

    private void handleRead(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer recvBuf = receiveBufferPool.get();
        
        try {
            int readBytes = channel.read(recvBuf);
            if (readBytes == -1) {
                key.cancel();
                channel.close();
                return;
            }

            recvBuf.flip();
            while (recvBuf.remaining() >= 8) {
                int pos = recvBuf.position();
                int magic = recvBuf.getInt();
                if (magic != MAGIC_HEADER) {
                    throw new IOException("Invalid protocol magic header: " + Integer.toHexString(magic));
                }
                int length = recvBuf.getInt();
                if (recvBuf.remaining() < length) {
                    recvBuf.position(pos);
                    break;
                }

                int limit = recvBuf.limit();
                recvBuf.limit(recvBuf.position() + length);
                ByteBuffer slice = recvBuf.slice();
                
                MessageHandler handler = messageHandler;
                if (handler != null) {
                    handler.onMessage(-1, slice);
                }

                recvBuf.position(recvBuf.limit());
                recvBuf.limit(limit);
            }
            recvBuf.compact();
        } catch (IOException e) {
            key.cancel();
            try {
                channel.close();
            } catch (IOException ignored) {}
        }
    }

    @Override
    public void close() {
        running.set(false);
        selector.wakeup();
        try {
            selectorThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        for (SocketChannel channel : outboundChannels.values()) {
            try {
                channel.close();
            } catch (IOException ignored) {}
        }
        outboundChannels.clear();

        try {
            serverChannel.close();
        } catch (IOException ignored) {}

        try {
            selector.close();
        } catch (IOException ignored) {}
    }
}