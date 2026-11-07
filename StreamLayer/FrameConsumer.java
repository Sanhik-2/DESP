package StreamLayer;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

@FunctionalInterface
public interface FrameConsumer {
    void onFrameReceived(byte apiKey, int correlationId, byte priorityTier, ByteBuffer payload, SocketChannel channel);
}
