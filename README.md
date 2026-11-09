# DESP: Distributed Edge Streaming Platform
> A low-latency, event-driven message broker and persistence engine for resource-constrained edge topologies and autonomous mesh networks.

---

## 1. Executive Summary & Problem Formulation

In decentralized edge deployments—such as autonomous UAV swarms, tactical sensor grids, and industrial IoT—standard enterprise streaming platforms (e.g., Apache Kafka, RabbitMQ) exhibit fundamental operational bottlenecks:

* **Resource Footprint:** Heavy JVM heap allocations, garbage collection pauses, and multi-megabyte container footprints exceed the capacity of low-power edge compute units.
* **Storage Inefficiency:** High-frequency raw sensor streams cause rapid flash memory wear and premature solid-state degradation when persisted indiscriminately to non-volatile disk.
* **Network Overhead:** Serialization formats such as JSON or Protocol Buffers over HTTP/1.1 or standard TCP incur non-trivial parsing overhead, high protocol frame bloat, and head-of-line blocking under degraded radio frequency (RF) links.

`DESP` addresses these constraints through a bare-metal, dependency-free Java architecture featuring a non-blocking single-threaded I/O reactor, an explicit 11-byte binary wire protocol, and decoupled tiered persistence.

---

## 2. Architectural Blueprint

The platform decouples inbound network transport from disk persistence using the classic Reactor Pattern and an event-driven worker model:

```text
       [ Autonomous Producers / Edge Sensor Nodes ]
                            │
                            │ TCP Stream (Big-Endian Binary Wire Protocol)
                            ▼
              ┌───────────────────────────┐
              │    StreamLayer: MeshNode  │
              │  - Java NIO Selector Loop │
              │  - Non-blocking Sockets   │
              │  - TCP_NODELAY Enabled    │
              └─────────────┬─────────────┘
                            │ Raw Byte Buffers
                            ▼
              ┌───────────────────────────┐
              │     ConnectionSession     │
              │  - Frame Length Framing   │
              │  - Magic Byte Validation  │
              │  - Partial Read Assembly  │
              └─────────────┬─────────────┘
                            │ Decoded MessageEvent
                            ▼
              ┌───────────────────────────┐
              │    Engine: EventDispatcher│
              │  - Blocking Work Queue    │
              │  - Dispatches Binary ACKs │
              └─────────────┬─────────────┘
                            │
         ┌──────────────────┴──────────────────┐
         ▼                                     ▼
┌─────────────────────────────┐   ┌─────────────────────────────┐
│ Tier 0: Volatile In-Memory  │   │ Tier 2: Relational Audit    │
│ - Sub-second sensor pings   │   │ - Critical alerts, offsets  │
│ - Zero disk write overhead  │   │ - SQLite JDBC Engine        │
└─────────────────────────────┘   └─────────────────────────────┘
#Layer Separation
##StreamLayer (Network Transport):
 Manages channel registration, multiplexed OP_ACCEPT/OP_READ event loops via OS-level primitives (epoll), and chunked TCP segment reassembly without external frameworks.

##Engine (Event Coordination): 

Decouples socket reads from storage locks using an internal producer-consumer queue to prevent disk I/O latency from stalling network throughput.

##StorageLayer (Persistence Engine): 
Provides structured metadata and payload logging via an embedded SQLite JDBC persistence provider.

#3. Wire Protocol Specification
Communication between clients and broker nodes occurs over a deterministic, big-endian binary frame:
0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Frame Length                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   Magic (0xCA)|    ApiKey     |         Correlation ID        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       (Corr ID cont.)         | Priority Tier |               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+               |
|                                                               |
|                        Payload Data...                        |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

Frame Fields
Frame Length (uint32, 4 Bytes): Total remaining payload byte count (N + 7), ensuring deterministic framing across fragmented TCP packets.

Magic Byte (uint8, 1 Byte): Fixed synchronization constant 0xCA. Sockets transmitting misaligned or malformed headers are immediately terminated.

API Key (uint8, 1 Byte):

0x00: HEARTBEAT / Ping

0x01: PRODUCE (Publish payload)

0x02: FETCH (Consume stream)

Correlation ID (uint32, 4 Bytes): Client-assigned monotonic identifier mirrored in broker acknowledgments for asynchronous response matching.

Priority Tier (uint8, 1 Byte): Ingestion routing lifecycle tag:

0x00 (TIER_WORKING): High-frequency telemetry (50 Hz gyro/attitude data); retained in volatile memory.

0x01 (TIER_OPERATIONAL): Intermediate operational waypoints; circular buffer retention.

0x02 (TIER_AUDIT): Critical system events, geofence violations, and fail-safes; committed to non-volatile disk storage.

Payload (byte[], Variable): Raw uncompressed binary data payload.

Broker Acknowledgment Frame (10 Bytes)
Plaintext
[Frame Length: 4B (Value = 6)][Correlation ID: 4B][Status Code: 2B (0x0000 = OK)]
4. Implementation Details & Academic Mapping
The codebase demonstrates low-level systems programming while implementing standard object-oriented and concurrent software engineering principles:

Core Concept	Implementation Detail	Location
Non-blocking I/O & Multiplexing	java.nio.channels.Selector, ServerSocketChannel, non-blocking client registration	StreamLayer.MeshNode
State Machine & Framing	Two-phase state transitions (headerParsed flag), buffer draining, pointer alignment via flip()/clear()	StreamLayer.ConnectionSession
Separation of Concerns / Interfaces	Functional decoupling between network ingestion and storage handlers using FrameConsumer callback	StreamLayer.FrameConsumer
Relational Data Durability	Embedded JDBC persistence engine, parameterized queries, schema automation, BLOB handling	StorageLayer.DatabaseManager
Multithreading & Concurrency	Dedicated event loop thread, thread-safe synchronized persistence handlers, non-interfering client sessions	Engine.EventDispatcher / Main
5. Verification & Experimental Baseline
The platform has been validated under loopback and local network workloads to verify network transport reliability and persistence:

Plaintext
[DB] SQLite database initialized successfully.
[DESP] Broker engine initialized on port 9001.
[MeshNode] Initialized on 127.0.0.1:9001
[MeshNode] Peer connected from /127.0.0.1:46836
Received Frame -> ApiKey: 1, CorrId: 1, Tier: 2, PayloadSize: 44 bytes
Received Frame -> ApiKey: 1, CorrId: 2, Tier: 0, PayloadSize: 42 bytes
Received Frame -> ApiKey: 1, CorrId: 3, Tier: 0, PayloadSize: 42 bytes
Received Frame -> ApiKey: 1, CorrId: 4, Tier: 0, PayloadSize: 42 bytes
Received Frame -> ApiKey: 1, CorrId: 5, Tier: 0, PayloadSize: 42 bytes
Received Frame -> ApiKey: 1, CorrId: 6, Tier: 0, PayloadSize: 42 bytes
Relational Store Durability Record
Plaintext
sqlite> SELECT id, correlation_id, tier, payload_size, received_at FROM message_log;
id  correlation_id  tier  payload_size  received_at
--  --------------  ----  ------------  -------------------
1   1               2     44            2026-11-07 04:21:26
2   2               0     42            2026-11-07 04:21:26
3   3               0     42            2026-11-07 04:21:26
4   4               0     42            2026-11-07 04:21:26
5   5               0     42            2026-11-07 04:21:26
6   6               0     42            2026-11-07 04:21:26
6. Build, Execution & Runtime Guide
Environment Prerequisites
OpenJDK 17 or higher

SQLite 3 native libraries

POSIX shell environment (zsh or bash)

Compilation
From the project root:

Bash
mkdir -p bin
javac -cp "lib/sqlite-jdbc.jar:lib/*:." -d bin $(find . -name "*.java" -not -path "*/.*")
Running the Broker Node
Bash
java -cp "bin:lib/sqlite-jdbc.jar:lib/*:." Main
Running the Test Client
Bash
java -cp "bin:lib/sqlite-jdbc.jar:lib/*:." StreamLayer.MeshClient
7. Roadmap & Academic Deliverables
[x] Core Java NIO Selector-based non-blocking network reactor.

[x] Deterministic 11-byte binary wire protocol with 0xCA magic byte enforcement.

[x] TCP chunk reassembly and connection session state machine.

[x] Relational audit trail persistence via embedded JDBC (StorageLayer.DatabaseManager).

[ ] Swing Monitoring Dashboard: Real-time broker metrics, connection status, and ingested frame counters.

[ ] Swarm Simulator: Multi-client simulated flight telemetry generation for live evaluation demos.

[ ] Append-Only Disk Segment Engine: Custom zero-copy log indexing (FileChannel.transferTo).