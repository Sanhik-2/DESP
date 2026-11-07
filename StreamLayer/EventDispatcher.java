package StreamLayer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventDispatcher implements Runnable {
    public static final byte CMD_PING = 0x00;
    public static final byte CMD_PRODUCE = 0x01;
    public static final byte CMD_FETCH = 0x02;
    public static final byte CMD_REPLICATE = 0x05;
    public static final byte CMD_HEARTBEAT_PROBE =  0x06;
    public static final byte CMD_METADATA_SYNC = 0x07;
    

    public static final short STATUS_SUCCESS = 0x0000;
    public static final short STATUS_UNKNOWN_CMD = 0x0001;
    public static final short STATUS_QUEUE_FULL = 0x0002;

    private final BlockingQueue<MessageEvent> eventQueue;
    private final Thread workerThread;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public EventDispatcher(int queueCapacity) {
        this.eventQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.workerThread = new Thread(this, "DESP-Dispatcher-Worker");
    }

    public void start() {
        workerThread.start();
    }

    /**
     * Called by MeshNode on the Reactor Thread. Non-blocking enqueue.
     */
    public boolean submit(MessageEvent event) {
        boolean accepted = eventQueue.offer(event);
        if (!accepted) {
            // Queue full: Drop or fast-fail back to the client immediately
            sendAck(event.getClientChannel(), event.getCorrelationId(), STATUS_QUEUE_FULL);
        }
        return accepted;
    }

    @Override
    public void run() {
        while (running.get()) {
            try {
                MessageEvent event = eventQueue.take();
                processEvent(event);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void processEvent(MessageEvent event) {
        switch (event.getApiKey()) {
            case CMD_PRODUCE:
                handleProduce(event);
                break;

            case CMD_FETCH:
                handleFetch(event);
                break;

            case CMD_PING:
                sendAck(event.getClientChannel(), event.getCorrelationId(), STATUS_SUCCESS);
                break;

            default:
                sendAck(event.getClientChannel(), event.getCorrelationId(), STATUS_UNKNOWN_CMD);
                break;
        }
    }

    private void handleProduce(MessageEvent event) {
        // Log basic arrival stats
        ByteBuffer payload = event.getPayload();
        
        // TODO: Pass event to StorageTierManager:
        // Tier 0 -> RingBuffer
        // Tier 1 -> TTL Segment Log
        // Tier 2 -> Append-Only Commit Log (.log file)

        // For now, commit ACK back to client
        sendAck(event.getClientChannel(), event.getCorrelationId(), STATUS_SUCCESS);
    }

    private void handleFetch(MessageEvent event) {
        // TODO: Read from StorageEngine and stream chunks back
    }

    /**
     * Transmits the standard 10-byte binary acknowledgment:
     * [4B Length = 6] [4B CorrelationId] [2B StatusCode]
     */
    public static void sendAck(SocketChannel channel, int correlationId, short status) {
        if (channel == null || !channel.isOpen()) return;

        try {
            ByteBuffer ack = ByteBuffer.allocate(10);
            ack.putInt(6); // Payload length (4 + 2)
            ack.putInt(correlationId);
            ack.putShort(status);
            ack.flip();

            while (ack.hasRemaining()) {
                channel.write(ack);
            }
        } catch (IOException e) {
            System.err.println("[EventDispatcher] Failed to send ACK: " + e.getMessage());
        }
    }

    public void stop() {
        running.set(false);
        workerThread.interrupt();
    }
}