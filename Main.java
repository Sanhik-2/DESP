import StreamLayer.MeshNode;
import StorageLayer.DatabaseManager;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Main {
    public static void main(String[] args) throws Exception {
        // 1. Initialize Storage
        DatabaseManager db = new DatabaseManager();
        db.init();

        // 2. Start MeshNode, wiring inbound frames directly to the storage sink
        MeshNode broker = new MeshNode("127.0.0.1", 9001, (apiKey, corrId, tier, payload, channel) -> {
            byte[] raw = new byte[payload.remaining()];
            payload.get(raw);

            // Persist to SQLite
            db.insertMessage(corrId, tier, raw);

            // Send back the binary ACK (Status 0x0000 = SUCCESS)
            sendAck(channel, corrId, (short) 0x0000);
        });

        System.out.println("[DESP] Broker engine initialized on port 9001.");
        broker.start();
    }

    private static void sendAck(SocketChannel channel, int corrId, short status) {
        if (channel == null || !channel.isOpen()) return;
        try {
            ByteBuffer ack = ByteBuffer.allocate(10);
            ack.putInt(6);
            ack.putInt(corrId);
            ack.putShort(status);
            ack.flip();
            while (ack.hasRemaining()) {
                channel.write(ack);
            }
        } catch (Exception ignored) {}
    }
}