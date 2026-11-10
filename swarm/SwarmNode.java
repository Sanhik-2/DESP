package swarm;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import StorageLayer.DatabaseManager;

public class SwarmNode {

    public enum NodeStatus {
        IDLE,
        BUSY,
        MALFUNCTIONING
    }

    public record Message(
        String senderId,
        String swarmId,
        byte tier,              // 0 = Working/Telemetry, 1 = Operational, 2 = Critical
        double[] position,
        double batteryLevel,
        NodeStatus status,
        long timestamp
    ) {}

    public static class Task {
        private final int taskId;
        private final int priority;
        private final String description;

        public Task(int taskId, int priority, String description) {
            this.taskId = taskId;
            this.priority = priority;
            this.description = description;
        }

        public int getPriority() { return priority; }
        public int getTaskId() { return taskId; }
        public String getDescription() { return description; }
    }

    private final String nodeId;
    private final double[] position;
    private final double[] velocity;
    private final List<SwarmNode> neighbors = new CopyOnWriteArrayList<>();
    private final double communicationRange;
    private final AtomicInteger taskIdCounter = new AtomicInteger(0);
    private final PriorityBlockingQueue<Task> taskQueue;
    private volatile NodeStatus status;
    private volatile double batteryLevel;
    private final Map<String, Object> sensorData = new ConcurrentHashMap<>();
    private final String communicationProtocol;
    private final String swarmId;

    private final VolatileRingBuffer<Message> volatileTelemetryBuffer = new VolatileRingBuffer<>(128);
    private final DatabaseManager dbManager;

    public SwarmNode(String nodeId, double[] position, double[] velocity, 
                     double communicationRange, String communicationProtocol, String swarmId) {
        this.nodeId = nodeId;
        this.position = Arrays.copyOf(position, 3);
        this.velocity = Arrays.copyOf(velocity, 3);
        this.communicationRange = communicationRange;
        this.taskQueue = new PriorityBlockingQueue<>(11, Comparator.comparingInt(Task::getPriority));
        this.status = NodeStatus.IDLE;
        this.batteryLevel = 100.0;
        this.communicationProtocol = communicationProtocol;
        this.swarmId = swarmId;
        this.dbManager = new DatabaseManager();
        this.dbManager.init();
    }

    public synchronized void move(double elapsedTime) {
        for (int i = 0; i < 3; i++) {
            position[i] += velocity[i] * elapsedTime;
        }
    }

    public byte determineTier(String alertContext, boolean isGeofenceBreach) {
        // 1. Critical: Hardware failsafe or battery collapse
        if (this.status == NodeStatus.MALFUNCTIONING || this.batteryLevel < 15.0) {
            return (byte) 2;
        }

        // 2. Critical: Mission/Geofence alert
        if (isGeofenceBreach || (alertContext != null && alertContext.contains("CRITICAL"))) {
            return (byte) 2;
        }

        // 3. Operational: Task backlog load shedding
        if (this.taskQueue.size() > 5) {
            return (byte) 1;
        }

        // Default: Ephemeral Telemetry
        return (byte) 0;
    }
    public synchronized void clampAndBounce(double minX, double maxX, double minY, double maxY) {
    if (position[0] <= minX || position[0] >= maxX) {
        velocity[0] = -velocity[0];
        position[0] = Math.max(minX, Math.min(maxX, position[0]));
    }
    if (position[1] <= minY || position[1] >= maxY) {
        velocity[1] = -velocity[1];
        position[1] = Math.max(minY, Math.min(maxY, position[1]));
    }
}

    public void communicate() {
        String context = (String) this.sensorData.get("alert_description");
        boolean geofenceBreach = Boolean.TRUE.equals(this.sensorData.get("geofence_breach"));

        // Determine tier BEFORE checking status so emergency broadcasts go out
        byte tier = determineTier(context, geofenceBreach);

        double[] currentPos;
        synchronized (this) {
            currentPos = Arrays.copyOf(this.position, 3);
        }

        Message outboundPacket = new Message(
            this.nodeId,
            this.swarmId,
            tier,
            currentPos,
            this.batteryLevel,
            this.status,
            System.currentTimeMillis()
        );

        // Dispatch to in-range peers
        for (SwarmNode neighbor : neighbors) {
            double[] nPos = neighbor.getPosition();
            double dx = currentPos[0] - nPos[0];
            double dy = currentPos[1] - nPos[1];
            double dz = currentPos[2] - nPos[2];
            double distance = Math.sqrt(dx * dx + dy * dy + dz * dz);

            if (distance <= this.communicationRange) {
                neighbor.receiveMessage(outboundPacket);
            }
        }
    }

    public void receiveMessage(Message msg) {
        if (!this.swarmId.equals(msg.swarmId())) {
            return;
        }

        switch (msg.tier()) {
            case 0 -> {
                // TIER 0: In-Memory Ring Buffer (Zero Flash Degradation)
                this.volatileTelemetryBuffer.append(msg);
                this.sensorData.put("peer_" + msg.senderId() + "_pos", msg.position());
            }
            case 1 -> {
                // TIER 1: Operational Task Balancing
                this.taskQueue.offer(new Task(
                    taskIdCounter.incrementAndGet(),
                    2,
                    "Assigned load shed from node " + msg.senderId()
                ));
            }
            case 2 -> {
                // TIER 2: Relational Durability Audit
                System.out.printf("[%s] CRITICAL ALERT from Node %s: Battery=%.1f%%, Status=%s\n",
                    this.nodeId, msg.senderId(), msg.batteryLevel(), msg.status());

                if (this.dbManager != null) {
                    byte[] payload = ("ALERT from " + msg.senderId() + 
                                      ": Battery=" + msg.batteryLevel() + 
                                      ", Status=" + msg.status()).getBytes();
                    this.dbManager.insertMessage(0, msg.tier(), payload);
                }
            }
            default -> {}
        }
    }

    public void addNeighbor(SwarmNode node) {
        if (node != this && !this.neighbors.contains(node)) {
            this.neighbors.add(node);
        }
    }

    public String getNodeId() { return nodeId; }
    public synchronized double[] getPosition() { return Arrays.copyOf(position, 3); }
    public NodeStatus getStatus() { return status; }
    public void setStatus(NodeStatus status) { this.status = status; }
    public double getBatteryLevel() { return batteryLevel; }
    public void setBatteryLevel(double batteryLevel) { this.batteryLevel = batteryLevel; }
}