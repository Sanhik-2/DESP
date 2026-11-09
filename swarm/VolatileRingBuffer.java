package swarm;
import java.util.*;

public class VolatileRingBuffer<T> {
    private final Object[] buffer;
    private final int capacity;
    private int head = 0;
    private int size = 0;

    public VolatileRingBuffer(int capacity) {
        this.capacity = capacity;
        this.buffer = new Object[capacity];
    }

    public synchronized void append(T item) {
        buffer[head] = item;
        head = (head + 1) % capacity;
        if (size < capacity) {
            size++;
        }
    }

    @SuppressWarnings("unchecked")
    public synchronized List<T> getRecent(int count) {
        List<T> list = new ArrayList<>();
        int fetchCount = Math.min(count, size);
        for (int i = 0; i < fetchCount; i++) {
            int index = (head - 1 - i + capacity) % capacity;
            list.add((T) buffer[index]);
        }
        return list;
    }

    public synchronized int size() {
        return size;
    }
}