package StreamLayer;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class MeshClient implements AutoCloseable {
    private static final byte MAGIC_BYTE = (byte) 0xCA;
    
    // API Keys
    public static final byte API_PING = 0x00;
    public static final byte API_PRODUCE = 0x01;
    public static final byte API_FETCH = 0x02;

    // Tiers
    public static final byte TIER_WORKING = 0x00;
    public static final byte TIER_MED = 0x01;
    public static final byte TIER_IMPORTANT = 0x02;

    private final String host;
    private final int port;
    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    private final AtomicInteger correlationSeq = new AtomicInteger(1);

    public MeshClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        this.socket = new Socket(host, port);
        // Disable Nagle's algorithm on client side as well
        this.socket.setTcpNoDelay(true);
        this.out = new DataOutputStream(socket.getOutputStream());
        this.in = new DataInputStream(socket.getInputStream());
        System.out.printf("[MeshClient] Connected to broker at %s:%d\n", host, port);
    }

    /**
     * Sends a PRODUCE request with raw byte payload and waits for acknowledgment.
     * @return Server status code (0 = SUCCESS)
     */
    public short produce(byte tier, byte[] payload) throws IOException {
        int correlationId = correlationSeq.getAndIncrement();

        // Frame Length = Magic(1) + ApiKey(1) + CorrId(4) + Tier(1) + payload.length
        int frameLength = 1 + 1 + 4 + 1 + payload.length;

        // Allocate buffer for 4-byte prefix + entire frame
        ByteBuffer buffer = ByteBuffer.allocate(4 + frameLength);
        buffer.putInt(frameLength);
        buffer.put(MAGIC_BYTE);
        buffer.put(API_PRODUCE);
        buffer.putInt(correlationId);
        buffer.put(tier);
        buffer.put(payload);

        // Send over the wire
        out.write(buffer.array());
        out.flush();

        // Read response: [4B Length] [4B CorrelationId] [2B StatusCode]
        int respLen = in.readInt();
        if (respLen != 6) {
            throw new IOException("Unexpected response length: " + respLen);
        }

        int echoedCorrId = in.readInt();
        short statusCode = in.readShort();

        if (echoedCorrId != correlationId) {
            throw new IOException(String.format("Correlation ID mismatch! Expected %d, got %d", 
                correlationId, echoedCorrId));
        }

        return statusCode;
    }

    /**
     * Sends an empty PING / Heartbeat packet.
     */
    public short ping() throws IOException {
        return produce(TIER_WORKING, new byte[0]);
    }

    @Override
    public void close() throws IOException {
        if (socket != null && !socket.isClosed()) {
            socket.close();
            System.out.println("[MeshClient] Connection closed.");
        }
    }

    // Quick standalone test runner
    public static void main(String[] args) {
        try (MeshClient client = new MeshClient("127.0.0.1", 9001)) {
            client.connect();

            // 1. Send High-Priority Event
            String alert = "ALERT: Drone-04 battery critically low (12%)";
            short status = client.produce(TIER_IMPORTANT, alert.getBytes());
            System.out.println("Produce Status: " + status);

            // 2. Send 5 fast telemetry packets
            for (int i = 0; i < 5; i++) {
                String telemetry = String.format("DRONE_01: tick=%d, lat=17.385%d, lon=78.486%d", i, i, i);
                status = client.produce(TIER_WORKING, telemetry.getBytes());
                System.out.printf("Sent packet %d -> Ack Status: %d\n", i, status);
                Thread.sleep(100);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}