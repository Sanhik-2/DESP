package StorageLayer;

import java.sql.*;

public class DatabaseManager {
    private static final String DB_URL = "jdbc:sqlite:desp_storage.db";
    private Connection conn;

    public void init() {
    try {
        // Force the JVM to load and register the SQLite driver
        Class.forName("org.sqlite.JDBC");

        conn = DriverManager.getConnection(DB_URL);
        try (Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS message_log (" +
                         "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                         "correlation_id INTEGER, " +
                         "tier INTEGER, " +
                         "payload_size INTEGER, " +
                         "payload BLOB, " +
                         "received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                         ");";
            stmt.execute(sql);
        }
        System.out.println("[DB] SQLite database initialized successfully.");
    } catch (ClassNotFoundException e) {
        System.err.println("[DB] Driver class missing from classpath: " + e.getMessage());
    } catch (SQLException e) {
        System.err.println("[DB] Init failed: " + e.getMessage());
    }
}

    public synchronized void insertMessage(int correlationId, byte tier, byte[] payload) {
        if (conn == null) return;
        String sql = "INSERT INTO message_log (correlation_id, tier, payload_size, payload) VALUES (?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, correlationId);
            pstmt.setInt(2, tier);
            pstmt.setInt(3, payload.length);
            pstmt.setBytes(4, payload);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[StorageLayer] DB Insert Error: " + e.getMessage());
        }
    }
    
}