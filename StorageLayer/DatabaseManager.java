package StorageLayer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DatabaseManager {

    private static final String DB_URL = "jdbc:sqlite:swarm_telemetry.db";

    public DatabaseManager() {}

    public synchronized void init() {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            System.err.println("[DB] SQLite JDBC Driver not found on classpath!");
            return;
        }

        String sql = """
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER,
                tier INTEGER,
                payload BLOB,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            """;

        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            System.err.println("[DB] Schema init error: " + e.getMessage());
        }
    }

    public synchronized void insertMessage(int nodeId, byte tier, byte[] payload) {
        String sql = "INSERT INTO telemetry(node_id, tier, payload) VALUES(?, ?, ?);";

        try (Connection conn = DriverManager.getConnection(DB_URL);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, nodeId);
            pstmt.setByte(2, tier);
            pstmt.setBytes(3, payload);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] Tier 2 write failed: " + e.getMessage());
        }
    }

    public synchronized List<AuditRecord> fetchRecentRecords(int limit) {
        List<AuditRecord> records = new ArrayList<>();
        String query = "SELECT id, node_id, tier, datetime(timestamp, 'localtime') AS ts FROM telemetry ORDER BY id DESC LIMIT ?;";

        try (Connection conn = DriverManager.getConnection(DB_URL);
             PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setInt(1, limit);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    records.add(new AuditRecord(
                        rs.getInt("id"),
                        rs.getInt("node_id"),
                        rs.getInt("tier"),
                        rs.getString("ts")
                    ));
                }
            }
        } catch (SQLException e) {
            // Non-fatal read lock contention during high-frequency writes
        }
        return records;
    }
}