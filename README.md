# DESP
Distributed Edge Streaming Platform (DESP)High-Throughput, Low-Latency Edge Streaming Engine1. Project OverviewDESP is a distributed, append-only message streaming broker designed from scratch in Java for resource-constrained edge computing environments (such as autonomous drone swarms and IoT mesh devices).Instead of heavy enterprise message brokers that consume gigabytes of memory and wear out edge flash drives, DESP uses a lightweight architecture:Custom Binary Wire Protocol: 11-byte zero-overhead frame headers instead of bulky JSON/HTTP text.Non-Blocking Network Engine: Single-threaded java.nio Selector event loop handling concurrent connections via OS-level multiplexing (epoll).Tiered Storage Model: Separates high-frequency ephemeral telemetry from mission-critical audit events to preserve edge device memory and flash endurance.2. System Architecture                    +-------------------------------------+
                    |       Drone Swarm / Edge Nodes      |
                    +-------------------------------------+
                                       |
                                       | TCP Binary Stream
                                       v
                     +-----------------------------------+
                     |         MeshNode (Core)           |
                     |  - Non-blocking Selector Loop     |
                     |  - TCP_NODELAY (Zero Nagle Lag)   |
                     +-----------------------------------+
                                       |
                                       | Chunks
                                       v
                     +-----------------------------------+
                     |      ConnectionSession State      |
                     |  - Glues fragmented TCP packets   |
                     |  - Verifies Magic Byte (0xCA)     |
                     +-----------------------------------+
                                       |
                                       | Reassembled Frames
                                       v
                     +-----------------------------------+
                     |     Core Storage & Dispatcher     |
                     +-----------------------------------+
                                       |
             +-------------------------+-------------------------+
             |                         |                         |
             v                         v                         v
       [ Tier 0: Hot ]           [ Tier 1: Med ]          [ Tier 2: Cold ]
     Volatile In-Memory         Operational TTL           Immutable Log
      (50Hz Gyro / Raw)         (1Hz GPS Logs)           (Critical Audit)
3. Team Responsibilities & Task DelegationTo ensure stability across the engine, the codebase is strictly separated into the Systems Core and Application Layer:Core Engine (Owned by Lead)MeshNode.java: The non-blocking network reactor and connection manager.ConnectionSession.java: Byte-stream framing, partial TCP packet reassembly, and validation.StorageEngine (Upcoming): Append-only .log segments on disk, sparse index mapping, and tiered memory buffers.Application Space (Team Tasks)Your focus is building the user-facing tools, data models, and swarm simulators that produce data for the broker. You do not need to deal with threads, sockets, or byte offsets directly.Task 1: The Telemetry Model (TelemetryData.java)Create a clean Java POJO representing drone status (droneId, latitude, longitude, altitude, batteryPercentage, timestamp).Provide helper methods to turn it into a human-readable string or standard byte array.Task 2: Swarm Flight Simulator (SwarmSimulator.java)Use the provided MeshClient to simulate 3–5 drones flying on patrol.Send low-priority GPS updates on a timer (every 200ms) using TIER_WORKING.Trigger emergency alerts (e.g., battery < 15%) using TIER_IMPORTANT.Task 3: Test Verification ScriptsWrite small test runners to verify that multiple drones can send messages simultaneously without dropping connections.4. How to Use the Client API (MeshClient)The network complexity has been wrapped into MeshClient.java. You only need to call simple methods to connect and publish data:Quick Start ExampleJavaimport StreamLayer.MeshClient;

public class DroneMissionExample {
    public static void main(String[] args) {
        // 1. Connect to the local broker node
        try (MeshClient client = new MeshClient("127.0.0.1", 9001)) {
            client.connect();

            // 2. Publish Working Telemetry (Tier 0: Hot / High Frequency)
            // Use this for fast updates: GPS coordinates, speed, pitch/yaw
            byte[] telemetry = "DRONE-01: LAT=17.3850 LON=78.4867 ALT=110m".getBytes();
            short status = client.produce(MeshClient.TIER_WORKING, telemetry);
            System.out.println("Telemetry sent, Broker ACK: " + status);

            // 3. Publish Critical Alert (Tier 2: Cold / Permanent Flash Storage)
            // Use this for mission-critical events: Geofence breaches, low battery
            byte[] emergencyAlert = "CRITICAL: Geofence breached at Sector 4!".getBytes();
            short alertStatus = client.produce(MeshClient.TIER_IMPORTANT, emergencyAlert);
            System.out.println("Alert committed to disk, Broker ACK: " + alertStatus);

        } catch (Exception e) {
            System.err.println("Communication error: " + e.getMessage());
        }
    }
}
Storage Tiers AvailableConstantNameIntended UsagePersistence BehaviorMeshClient.TIER_WORKINGHot / Volatile50Hz sensor data, motor outputs, real-time pingsKept in volatile memory; overwritten continuously. Zero disk wear.MeshClient.TIER_MEDOperationalWaypoints, flight path progress, 1Hz health telemetryKept in short-lived circular disk segments. Pruned after mission phase.MeshClient.TIER_IMPORTANTCold / AuditTarget recognition, collision triggers, battery failsafesWritten to immutable append-only disk logs permanently.5. Wire Protocol Reference (For Debugging)If you ever need to inspect raw packets using Wireshark or a hex dump, this is the binary structure:[4B Frame Length] [1B Magic 0xCA] [1B ApiKey] [4B Correlation ID] [1B Tier] [Payload...]
|<--------------------------- 11-Byte Header --------------------------->|
Magic Byte (0xCA): Rejects any non-DESP traffic instantly.API Key: 0x00 = PING, 0x01 = PRODUCE, 0x02 = FETCH.Correlation ID: Tracks request/response pairs asynchronously.Tier: 0 = Working, 1 = Med, 2 = Important.6. How to Run the System1. Compile the ProjectFrom the repository root:Bashjavac -d bin src/StreamLayer/*.java
2. Launch the Broker Node (Terminal 1)Bashjava -cp bin StreamLayer.MeshNode
Listens on 127.0.0.1:9001 by default.3. Run the Client Simulator (Terminal 2)Bashjava -cp bin StreamLayer.MeshClient
You should see incoming frames logged on Terminal 1 and instant status ACKs logged on Terminal 2.