package swarm;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import StorageLayer.DatabaseManager;

public class SwarmNode {

    public enum NodeStatus {
        IDLE,
        BUSY,
        MALFUNCTIONING
    }

    // Lightweight immutable carrier for communication frames
    public record Message(
        String senderId,
        String swarmId,
        byte tier,              // 0 = Working/Telemetry, 1 = Operational, 2 = Critical
        double[] position,
        double batteryLevel,
        NodeStatus status,
        long timestamp
    ) {}

    // Inner task structure
    public static class Task {
        private final int taskId;
        private final int priority; // Lower or higher based on queue comparator
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

    private String nodeId;
    private double[] position; // [x, y, z]
    private double[] velocity; // [vx, vy, vz]
    private List<SwarmNode> neighbors;
    private double communicationRange;
    private AtomicInteger taskIdCounter = new AtomicInteger(0);
    private PriorityQueue<Task> taskQueue;
    private NodeStatus status;
    private double batteryLevel;
    private Map<String, Object> sensorData;
    private String communicationProtocol;
    private String swarmId;

    private final VolatileRingBuffer<Message> volatileTelemetryBuffer = new VolatileRingBuffer<>(128);
    private DatabaseManager dbManager;

    public SwarmNode(String nodeId, double[] position, double[] velocity, double communicationRange, String communicationProtocol, String swarmId) {
        this.nodeId = nodeId;
        this.position = position;
        this.velocity = velocity;
        this.neighbors = new ArrayList<>();
        this.communicationRange = communicationRange;
        this.taskQueue = new PriorityQueue<>(Comparator.comparingInt(Task::getPriority));
        this.status = NodeStatus.IDLE;
        this.batteryLevel = 100.0;
        this.sensorData = new HashMap<>();
        this.communicationProtocol = communicationProtocol;
        this.swarmId = swarmId;
        this.dbManager = new DatabaseManager();
        this.dbManager.init();
    }

    public void move(double elapsedTime) {
        for (int i = 0; i < 3; i++) {
            position[i] += velocity[i] * elapsedTime;
        }
    }

    /**
     * Scans registered neighbors, calculates Euclidean distance, and transmits
     * telemetry or alerts to all peers within communicationRange.
     */

    public byte determineTier(String alertContext, boolean isGeofenceBreach) {
    // 1. Inspect Node State & Battery
    if (this.status == NodeStatus.MALFUNCTIONING || this.batteryLevel < 15.0) {
        return (byte) 2; // Critical: Hardware failsafe
    }

    // 2. Inspect Inbound Telemetry/Alert Parameters
    if (isGeofenceBreach || (alertContext != null && alertContext.contains("CRITICAL"))) {
        return (byte) 2; // Critical: Mission alert
    }

    // 3. Inspect Task Backlog Pressure
    if (this.taskQueue.size() > 5) {
        return (byte) 1; // Operational / Med
    }

    // Default: Ephemeral High-Frequency Telemetry
    return (byte) 0;
}
    public void communicate() {
    if (this.status == NodeStatus.MALFUNCTIONING) {
        return;
    }

    // Check conditions first
    String context = (String) this.sensorData.get("alert_description");
    boolean geofenceBreach = Boolean.TRUE.equals(this.sensorData.get("geofence_breach"));

    // 1. Calculate tier based on data
    byte tier = determineTier(context, geofenceBreach);

    // 2. Construct the record with the computed tier
    Message outboundPacket = new Message(
        this.nodeId,
        this.swarmId,
        tier,
        Arrays.copyOf(this.position, 3),
        this.batteryLevel,
        this.status,
        System.currentTimeMillis()
    );

    // 3. Dispatch to in-range neighbors
    for (SwarmNode neighbor : neighbors) {
        double dx = this.position[0] - neighbor.position[0];
        double dy = this.position[1] - neighbor.position[1];
        double dz = this.position[2] - neighbor.position[2];
        double distance = Math.sqrt(dx * dx + dy * dy + dz * dz);

        if (distance <= this.communicationRange) {
            neighbor.receiveMessage(outboundPacket);
        }
    }
}
    
    /**
     * Ingestion point for messages received from neighboring nodes.
     */

        // Handle critical alerts (Tier 2, Tier 0)
        public void receiveMessage(Message msg) {
    if (!this.swarmId.equals(msg.swarmId())) {
        return;
    }

    if (msg.tier() == 0) {
        // TIER 0: In-Memory Only (Sub-millisecond write, zero flash degradation)
        this.volatileTelemetryBuffer.append(msg);
        this.sensorData.put("peer_" + msg.senderId() + "_pos", msg.position());

    } else if (msg.tier() == 2) {
        // TIER 2: Relational Disk Durability
        System.out.printf("[%s] ALERT received from Node %s: Battery=%.1f%%, Status=%s\n",
            this.nodeId, msg.senderId(), msg.batteryLevel(), msg.status());

        if (this.dbManager != null) {
            byte[] payload = ("ALERT from " + msg.senderId() + 
                              ": Battery=" + msg.batteryLevel() + 
                              ", Status=" + msg.status()).getBytes();
            this.dbManager.insertMessage(0, msg.tier(), payload);
        }
    }

    }

    public void addNeighbor(SwarmNode node) {
        if (node != this && !this.neighbors.contains(node)) {
            this.neighbors.add(node);
        }
    }

    // Getters
    public String getNodeId() { return nodeId; }
    public double[] getPosition() { return position; }
    public NodeStatus getStatus() { return status; }
    public double getBatteryLevel() { return batteryLevel; }
}