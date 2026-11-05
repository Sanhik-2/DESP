# DESP


```markdown
# Distributed Edge Streaming Platform (`DESP`)
> High-Throughput, Low-Latency Edge Streaming Engine built from scratch in Java.

---

## 1. Project Overview

`DESP` is a distributed, append-only message streaming broker designed specifically for resource-constrained edge computing environments (such as autonomous drone swarms and IoT mesh devices).

Instead of relying on heavy enterprise message brokers that consume gigabytes of heap memory and exhaust flash storage cycles, `DESP` operates on a bare-metal, low-level architecture:

* **Custom Binary Wire Protocol**: Deterministic 11-byte frame header eliminating the parsing overhead of bulky JSON or HTTP payloads.
* **Non-Blocking Network Engine**: Single-threaded `java.nio` `Selector` event loop handling concurrent connections via OS-level I/O multiplexing (`epoll` on Linux).
* **Tiered Storage Model**: Decouples ephemeral, sub-second telemetry from mission-critical audit logs to protect edge device RAM and flash storage endurance.

---

## 2. System Architecture

```text
                    +-------------------------------------+
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
                                       | Raw Byte Chunks
                                       v
                     +-----------------------------------+
                     |      ConnectionSession State      |
                     |  - Glues fragmented TCP packets   |
                     |  - Validates Magic Byte (0xCA)    |
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

```

---

## 3. Team Responsibilities & Work Distribution

To maintain stability across the engine, the codebase is strictly separated into the **Systems Core** and **Application Layer**:

### Systems Core *(Owned by Lead)*

* `MeshNode.java`: The non-blocking network reactor and connection manager.
* `ConnectionSession.java`: Byte-stream framing, partial TCP packet reassembly, and state machine validation.
* `StorageEngine` *(Upcoming)*: Append-only `.log` segments on disk, sparse index mapping, and tiered memory buffers.

### Application Space *(Team Tasks)*

Your focus is building the user-facing tools, data models, and swarm simulators that produce data for the broker. **You do not need to deal with low-level sockets, non-blocking channels, or byte offsets directly.**

* **Task 1: Telemetry Data Model (`TelemetryData.java`)**
* Create a Java POJO representing drone status (`droneId`, `latitude`, `longitude`, `altitude`, `batteryPercentage`, `timestamp`).
* Provide serialization helpers to encode the model into a standard byte array.


* **Task 2: Swarm Flight Simulator (`SwarmSimulator.java`)**
* Use the provided `MeshClient` to simulate 3–5 drones flying on patrol.
* Emit low-priority GPS updates on a timer (every 200ms) using `TIER_WORKING`.
* Trigger emergency alerts (e.g., battery < 15%) using `TIER_IMPORTANT`.


* **Task 3: Integration & Stress Verification**
* Write test scripts to simulate multiple drones publishing telemetry concurrently over independent socket connections.



---

## 4. How to Use the Client API (`MeshClient`)

All network multiplexing and framing complexities are encapsulated inside `MeshClient.java`. You can connect, transmit framed payloads, and receive broker acknowledgments directly:

### Quick Start Example

```java
import StreamLayer.MeshClient;

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

```

### Storage Tiers

| Constant | Name | Intended Usage | Persistence Behavior |
| --- | --- | --- | --- |
| `MeshClient.TIER_WORKING` | Hot / Volatile | 50Hz sensor data, motor outputs, real-time pings | Retained in volatile RAM; overwritten continuously. Zero disk wear. |
| `MeshClient.TIER_MED` | Operational | Waypoints, flight path progress, 1Hz health telemetry | Kept in short-lived circular disk segments. Pruned after mission phase. |
| `MeshClient.TIER_IMPORTANT` | Cold / Audit | Target recognition, collision triggers, battery failsafes | Written sequentially to immutable append-only disk logs permanently. |

---

## 5. Wire Protocol Reference (For Debugging)

If you need to inspect raw frames using Wireshark, `tcpdump`, or a terminal hex dump, the binary layout is defined as follows:

```text
[4B Frame Length] [1B Magic 0xCA] [1B ApiKey] [4B Correlation ID] [1B Tier] [Payload...]
|<--------------------------- 11-Byte Header --------------------------->|

```

* **Frame Length (4 Bytes)**: Big-endian integer specifying the total byte count of the frame following this field.
* **Magic Byte (`0xCA`)**: Sanity marker. Packets lacking this marker cause immediate socket termination.
* **API Key (1 Byte)**:
* `0x00`: `PING`
* `0x01`: `PRODUCE`
* `0x02`: `FETCH`


* **Correlation ID (4 Bytes)**: Unique client request identifier mirrored in broker acknowledgments for asynchronous response matching.
* **Tier (1 Byte)**: Destination storage lifecycle (`0x00` = Working, `0x01` = Med, `0x02` = Important).
* **Payload (Variable)**: Raw binary data slice.

---

## 6. How to Run the System

### 1. Compile the Source

From the project root:

```bash
javac -d bin src/StreamLayer/*.java

```

### 2. Launch the Broker Node (Terminal 1)

```bash
java -cp bin StreamLayer.MeshNode

```

*Listens on `127.0.0.1:9001` by default.*

### 3. Run the Client Simulator (Terminal 2)

```bash
java -cp bin StreamLayer.MeshClient

```

*Verification: Terminal 1 logs incoming frames with parsed metadata; Terminal 2 receives and logs status `0` acknowledgments.*

```

```