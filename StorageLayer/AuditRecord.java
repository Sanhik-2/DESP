package StorageLayer;

public class AuditRecord {
    private final int id;
    private final int nodeId;
    private final int tier;
    private final String timestamp;

    public AuditRecord(int id, int nodeId, int tier, String timestamp) {
        this.id = id;
        this.nodeId = nodeId;
        this.tier = tier;
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getTier() {
        return tier;
    }

    public String getTimestamp() {
        return timestamp;
    }
}