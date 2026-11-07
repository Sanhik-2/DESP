package StreamLayer;


import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public final class MessageEvent {
    private final byte apiKey;
    private final int correlationId;
    private final byte priorityTier;
    private final ByteBuffer payload;
    private final SocketChannel clientChannel;
    private final long arrivedTimestamp;

    public MessageEvent(byte apiKey, int correlationId, byte priorityTier, ByteBuffer payload, SocketChannel clientChannel) {
        this.apiKey = apiKey;
        this.correlationId = correlationId;
        this.priorityTier = priorityTier;
        this.payload = payload;
        this.clientChannel = clientChannel;
        this.arrivedTimestamp = System.currentTimeMillis();
    }

    public byte getApiKey() { return apiKey; }
    public int getCorrelationId() { return correlationId; }
    public byte getPriorityTier() { return priorityTier; }
    public ByteBuffer getPayload() { return payload; }
    public SocketChannel getClientChannel() { return clientChannel; }
    public long getArrivedTimestamp() { return arrivedTimestamp; }
}