package StreamLayer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

class ConnectionSession {
    public static final int HEADER_SIZE = 11;
    public static final byte MAGIC_BYTE = (byte) 0xCA;

    private final ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
    private ByteBuffer payloadBuffer = null;
    private boolean headerParsed = false;

    private int frameLength = -1;
    private byte apiKey = 0;
    private int correlationId = 0;
    private byte priorityTier = 0;

    public boolean read(SocketChannel channel) throws IOException {
        // Step 1: Read the 11-byte header if not already parsed
        if (!headerParsed) {
            int bytesRead = channel.read(headerBuffer);
            if (bytesRead == -1) {
                throw new IOException("Peer disconnected during header read");
            }
            if (headerBuffer.hasRemaining()) {
                return false; // Wait for remaining header bytes
            }

            // Header fully assembled
            headerBuffer.flip();
            this.frameLength = headerBuffer.getInt();
            byte magic = headerBuffer.get();
            if (magic != MAGIC_BYTE) {
                throw new IOException(String.format("Malformed frame: invalid magic byte 0x%02X", magic));
            }
            this.apiKey = headerBuffer.get();
            this.correlationId = headerBuffer.getInt();
            this.priorityTier = headerBuffer.get();

            int payloadSize = frameLength - (HEADER_SIZE - 4);
            if (payloadSize < 0 || payloadSize > 32 * 1024 * 1024) {
                throw new IOException("Invalid payload size: " + payloadSize);
            }

            this.payloadBuffer = ByteBuffer.allocate(payloadSize);
            this.headerParsed = true; // State transition!
        }

        // Step 2: Read the payload bytes
        if (headerParsed) {
            if (payloadBuffer.hasRemaining()) {
                int bytesRead = channel.read(payloadBuffer);
                if (bytesRead == -1) {
                    throw new IOException("Peer disconnected during payload read");
                }
            }

            // If payload has arrived completely (or payloadSize was 0)
            if (!payloadBuffer.hasRemaining()) {
                payloadBuffer.flip();
                return true;
            }
        }

        return false;
    }

    public byte getApiKey() { return apiKey; }
    public int getCorrelationId() { return correlationId; }
    public byte getPriorityTier() { return priorityTier; }
    public ByteBuffer getPayload() { return payloadBuffer; }

    public void reset() {
        headerBuffer.clear();
        payloadBuffer = null;
        frameLength = -1;
        headerParsed = false; // Ready for next frame!
    }
}

    
public class MeshNode implements Runnable {
    private final String ip;
    private final int port;
    private Selector selector;
    private ServerSocketChannel serverChannel;
    private volatile boolean running = true;
    private final FrameConsumer frameConsumer;

    public MeshNode(int port, FrameConsumer frameConsumer) {
    this("127.0.0.1", port, frameConsumer);
    }
    public MeshNode(String ip, int port, FrameConsumer frameConsumer) {
        this.ip = ip;
        this.port = port;
        this.frameConsumer = frameConsumer;
    }

    public void start() throws IOException {
        this.selector = Selector.open();

        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        this.serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.serverChannel.bind(new InetSocketAddress(ip, port));

        // Listen for new inbound connections
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        System.out.printf("[MeshNode] Initialized on %s:%d\n", ip, port);
        new Thread(this, "MeshNode-EventLoop").start();
    }

    @Override
    public void run() {
        while (running) {
            try {
                int readyChannels = selector.select();
                if (readyChannels == 0) continue;

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iter = selectedKeys.iterator();

                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove(); // Essential: prevent reprocessing stale key

                    if (!key.isValid()) continue;

                    if (key.isAcceptable()) {
                        handleAccept(key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    }
                }
            } catch (IOException e) {
                if (running) {
                    System.err.println("[MeshNode] Selector error: " + e.getMessage());
                }
            }
        }
    }

    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();

        if (client != null) {
            client.configureBlocking(false);
            client.setOption(StandardSocketOptions.TCP_NODELAY, true);

            // Register client for read and attach its OWN independent session
            SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);
            clientKey.attach(new ConnectionSession());

            System.out.println("[MeshNode] Peer connected from " + client.getRemoteAddress());
        }
    }

    private void handleRead(SelectionKey key) {
        SocketChannel client = (SocketChannel) key.channel();
        ConnectionSession session = (ConnectionSession) key.attachment();

        try {
            boolean complete = session.read(client);
            if (complete) {
                // Whole frame is ready: dispatch to storage/handler
                dispatch(client, session);
                session.reset();
            }
        } catch (IOException e) {
            System.out.println("[MeshNode] Connection dropped: " + e.getMessage());
            try {
                key.cancel();
                client.close();
            } catch (IOException ignored) {}
        }
    }

    private void dispatch(SocketChannel client, ConnectionSession session) {
        System.out.printf("Received Frame -> ApiKey: %d, CorrId: %d, Tier: %d, PayloadSize: %d bytes\n",
                session.getApiKey(), session.getCorrelationId(), session.getPriorityTier(), session.getPayload().remaining());

        if (frameConsumer != null) {
            frameConsumer.onFrameReceived(session.getApiKey(), session.getCorrelationId(), session.getPriorityTier(), session.getPayload(), client);
        }
    }
    /**
     * Main method to start the MeshNode.
     *
     * @param args command-line arguments
     * @throws IOException if an I/O error occurs
     */
    public static void main(String[] args) throws IOException {
        MeshNode node = new MeshNode("127.0.0.1", 9001, (apiKey, correlationId, priorityTier, payload, channel) -> {
            System.out.printf("[FrameConsumer] ApiKey: %d, CorrId: %d, Tier: %d, PayloadSize: %d bytes\n",
                    apiKey, correlationId, priorityTier, payload.remaining());
            // Here you can implement further processing of the received frame
        });
        node.start();
    }
}
