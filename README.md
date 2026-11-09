Distributed Edge Streaming Platform (DESP)High-Throughput, Tiered Binary Message Streaming Engine Built from Scratch in Java for Autonomous Swarms and Edge Networks.1. Project Identification & Executive SummaryProject Title: DESP: A High-Throughput Tiered Message Streaming Broker for Edge and Autonomous Swarm TopologiesPrimary Domain: Distributed Systems, Network I/O Multiplexing, Real-Time Edge TelemetryTarget Submission Standard: IEEE / ACM Conference Proceedings Two-Column Format (LaTeX)Target Platforms: Linux (x86_64, aarch64), Embedded JVM EnvironmentsAbstractEdge deployments such as autonomous drone swarms, industrial robotic cells, and remote sensor arrays generate continuous high-frequency telemetry. Existing enterprise message brokers (e.g., Apache Kafka, RabbitMQ) introduce severe overhead on resource-constrained hardware: large JVM heap footprints, multi-megabyte memory buffers, Garbage Collection (GC) pauses, and aggressive random flash disk wear.DESP addresses these constraints through a bare-metal, dependency-free architecture:Zero-Overhead Binary Wire Protocol: An 11-byte deterministic header replacing textual formats (JSON/XML), minimizing serialization latency and packet amplification.Non-Blocking Reactor Core: Single-threaded I/O multiplexing via Java NIO Selector mapped directly to Linux kernel primitives (epoll), maintaining high concurrent connection density with zero thread-per-client overhead.Decoupled Tiered Storage Engine: Network I/O is cleanly separated from disk persistence via asynchronous event queues and consumer interfaces. Ephemeral telemetry lives in volatile ring buffers, while mission-critical audit frames are durably logged to relational metadata storage (SQLite JDBC) and sequential disk segments.2. Academic & LaTeX Report MappingTo streamline the writing of the 15–20 page consolidated report and the 6–8 slide presentation for Dr. Tirthankar Sengupta's evaluation, use this section-by-section architectural breakdown:LaTeX Report Structure (IEEE/ACM Format)
├── 1. Title & Abstract (High-throughput streaming under edge constraints)
├── 2. Introduction & Motivation (1-2 pages: Drone swarms, edge limits, flash wear)
├── 3. Related Work & Research Context (1 page: Kafka vs. MQTT vs. DESP)
├── 4. System Architecture (2 pages: Reactor pattern, UML, framing state machine)
├── 5. Implementation Details (2 pages: NIO, OOP abstraction, JDBC audit sink)
├── 6. Experimental Results & Benchmarks (2 pages: Throughput, latency, SQLite logs)
└── 7. Conclusion & Future Work (1 page: Raft consensus, zero-copy FileChannel)
2.1 Introduction & Motivation (1–2 Pages)The Core Problem: Unmanned Aerial Vehicles (UAVs) and edge microcontrollers operate with strict RAM ceilings (256MB–1GB) and flash endurance limits (eMMC/NAND write cycles).The Failure of Enterprise Brokers:Apache Kafka: Tailored for high-memory datacenter servers; relies on the OS page cache and JVM heap allocation that exhausts embedded nodes.MQTT Brokers (Mosquitto): Lightweight but lack native append-only log primitives and multi-tiered persistence policies for audit compliance.The DESP Solution: Provides deterministic packet framing, single-allocation buffer recycling, and a decoupled storage sink that prevents slow disk writes from stalling network throughput.2.2 Related Work & Comparative Analysis (1 Page)Metric / DimensionApache KafkaEclipse Mosquitto (MQTT)DESP (This Work)Underlying ArchitectureDistributed Append-Only Commit LogCentralized Pub/Sub BrokerSingle-Reactor Ingestion + Tiered StorageTransport ProtocolCustom Binary over TCPMQTT Control PacketsCustom 11-Byte Minimalist Binary FramingNetwork Threading ModelThread Pool + Java NIO SocketServerSingle-threaded epoll Event LoopSingle-Threaded Java NIO Selector (Reactor)Memory FootprintGigabytes (JVM Heap + Page Cache)Minimal (< 10 MB, C native)Ultra-low (< 32 MB JVM Footprint)Payload SerializationUser-defined (Avro, JSON, Schema Reg.)Variable Byte Header + Raw Bytes11-byte Header + Direct Sliced PayloadPersistence TargetPartitioned Segment Files on DiskEphemeral In-Memory / Simple AppendTiered (Volatile RAM vs. SQLite JDBC Audit Log)Flash Wear ProtectionNone (Relies on OS kernel flush intervals)None (Memory-only or sync writes)Strict storage tiering (Volatile ring vs. disk)Key Academic Citations for Report:Kreps, J., Narkhede, N., & Rao, J. (2011). Kafka: A distributed messaging system for log processing. NetDB.Lea, D. (2000). Scalable IO in Java. Reactor Pattern Implementation Reference.Ousterhout, J., et al. (2015). The RAMCloud Storage System. ACM Transactions on Computer Systems (TOCS).3. System Architecture & Technical Specifications               +---------------------------------------------------+
               |             Edge Producer / Drone Swarm           |
               +---------------------------------------------------+
                                         |
                                         | TCP Binary Framing (Port 9001)
                                         v
               +---------------------------------------------------+
               |              StreamLayer: MeshNode                |
               |  - Non-Blocking Selector (epoll)                  |
               |  - Single-Threaded Event Loop (Reactor Pattern)   |
               |  - Client Key Attachment (ConnectionSession)      |
               +---------------------------------------------------+
                                         |
                                         | Partial TCP Chunks
                                         v
               +---------------------------------------------------+
               |          StreamLayer: ConnectionSession           |
               |  - 11-Byte Big-Endian Header Accumulator          |
               |  - Sanity Check: Magic Byte (0xCA)                |
               |  - Dynamic Allocation: PayloadBuffer (Capacity N) |
               +---------------------------------------------------+
                                         |
                                         | FrameConsumer Interface Callback
                                         v
               +---------------------------------------------------+
               |                  Main Engine Layer                |
               |  - Frame Validation & Offset Checkpoint           |
               |  - Binary Acknowledgment Dispatch (sendAck)       |
               +---------------------------------------------------+
                                         |
                         +---------------+---------------+
                         |                               |
                         v                               v
       +-----------------------------------+   +-----------------------------------+
       |     StorageLayer: DatabaseManager |   |  StorageLayer: Volatile Memory    |
       |  - JDBC SQLite Persistence        |   |  - Ephemeral Ring Buffer          |
       |  - Schema: message_log (Audit)    |   |  - 50Hz Fast Telemetry            |
       |  - Synchronized PreparedStatements|   |  - Zero Flash Degradation         |
       +-----------------------------------+   +-----------------------------------+
3.1 The 11-Byte Binary Wire ProtocolTo eliminate serialization overhead, every frame transmitted to DESP adheres to an exact network-byte-order (Big-Endian) layout: 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Frame Length                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   Magic 0xCA  |    ApiKey     |         Correlation ID        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     ...       | Priority Tier |       Payload Bytes ...       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
Field NameSizeJava TypeDescriptionFrame Length4 BytesintLength of the subsequent frame (Total Packet Size - 4).Magic Byte1 BytebyteMust equal 0xCA. Immediate disconnect if mismatch detected.API Key1 BytebyteOpcode: 0x00 (Ping), 0x01 (Produce), 0x02 (Fetch), 0x05 (Replicate).Correlation ID4 BytesintMonotonically increasing request identifier, mirrored in ACK frames.Priority Tier1 BytebyteStorage destination: 0x00 (Working), 0x01 (Med), 0x02 (Important).PayloadVariablebyte[]Raw telemetry bytes (up to 32MB max ceiling).Synchronous Binary Acknowledgment (ACK) FrameEvery successfully parsed frame yields an immediate 10-byte response back down the client channel:[Total Length: 4B = 6][Correlation ID: 4B][Status Code: 2B (0x0000 = SUCCESS)]4. Source Tree & Component Breakdown~/DESP/
├── bin/                             # Compiled class files
├── lib/                             # External runtime jars
│   └── sqlite-jdbc.jar              # Xerial SQLite JDBC Driver (v3.36.0.3)
├── src/
│   ├── StreamLayer/                 # PURE NETWORK REACTOR LAYER
│   │   ├── FrameConsumer.java       # Functional interface decoupling transport from persistence
│   │   ├── ConnectionSession.java   # TCP reassembly state machine & buffer slicer
│   │   ├── MeshNode.java            # Java NIO non-blocking Selector server
│   │   └── MeshClient.java          # Low-level framed client network driver
│   │
│   ├── StorageLayer/                # PERSISTENCE & AUDIT ENGINE
│   │   └── DatabaseManager.java     # SQLite JDBC connector (schema creation & inserts)
│   │
│   ├── PresentationLayer/           # (Teammate Task: GUI Dashboard)
│   │   └── BrokerMonitorGUI.java    # Java Swing real-time telemetry dashboard
│   │
│   └── Main.java                    # System orchestrator & dependency injection root
└── desp_storage.db                  # Local SQLite database (Auto-generated)
5. Academic Checklist FulfillmentSyllabus RequirementProject Implementation DetailObject-Oriented Programming (OOP)- Abstraction: FrameConsumer functional interface decoupling network transport from storage engines.- Encapsulation: Socket buffers, connection state, and parse pointers locked inside ConnectionSession.- Polymorphism: Dynamic lambda bindings in Main.java routing network frames to distinct persistence sinks.Advanced Java Concepts- Java NIO: Selector, ServerSocketChannel, SocketChannel, and SelectionKey event loop.- Direct Memory Manipulation: Explicit ByteBuffer pointer mechanics (flip(), clear(), getInt(), put()).- Multithreading: Decoupled async dispatch, event loop isolation from disk I/O.Database Connectivity (JDBC)Embedded SQLite integration via org.sqlite.JDBC. Dynamic DDL table creation (message_log), parameter-sanitized PreparedStatement insertions, and binary BLOB payload storage.Graphical User Interface (GUI)Java Swing / AWT dashboard displaying active broker status, real-time message tables (JTable), connection meters, and throughput counters.6. How to Build and Execute the PlatformPrerequisitesFedora / Linux (x86_64 or aarch64)OpenJDK 17 or higher (javac, java)SQLite 3 CLI (sqlite3)6.1 Compiling the Entire StackFrom the project root:# 1. Ensure output and library folders exist
mkdir -p bin lib

# 2. Download the standalone SQLite JDBC driver (if not already present)
curl -L -o lib/sqlite-jdbc.jar https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.36.0.3/sqlite-jdbc-3.36.0.3.jar

# 3. Compile all packages
javac -cp "lib/sqlite-jdbc.jar:." -d bin $(find . -name "*.java" -not -path "*/.*")
6.2 Running the Broker Engine (Terminal 1)java -cp "bin:lib/sqlite-jdbc.jar:." Main
Expected console output:[DB] SQLite database initialized successfully.
[DESP] Broker engine initialized on port 9001.
[MeshNode] Initialized on 127.0.0.1:9001
6.3 Running the Client Telemetry Generator (Terminal 2)java -cp "bin:lib/sqlite-jdbc.jar:." StreamLayer.MeshClient
Expected console output:[MeshClient] Connected to broker at 127.0.0.1:9001
Produce Status: 0
Sent packet 0 -> Ack Status: 0
Sent packet 1 -> Ack Status: 0
Sent packet 2 -> Ack Status: 0
Sent packet 3 -> Ack Status: 0
Sent packet 4 -> Ack Status: 0
[MeshClient] Connection closed.
6.4 Inspecting Durability via SQLite CLI (Terminal 3)sqlite3 desp_storage.db -header -column "SELECT id, correlation_id, tier, payload_size, received_at FROM message_log;"
Expected output table:id  correlation_id  tier  payload_size  received_at        
--  --------------  ----  ------------  -------------------
1   1               2     44            2026-11-07 04:21:26
2   2               0     42            2026-11-07 04:21:26
3   3               0     42            2026-11-07 04:21:26
4   4               0     42            2026-11-07 04:21:26
5   5               0     42            2026-11-07 04:21:26
6   6               0     42            2026-11-07 04:21:26
7. Immediate Team Responsibilities & Work DistributionTo finalize the academic deliverables before November 9th, tasks are allocated as follows:Task Track A: Systems Core & Report Lead (Completed / In Review)[x] Java NIO Non-blocking Reactor Engine (MeshNode).[x] Partial TCP Packet Assembly State Machine (ConnectionSession).[x] 11-byte Binary Wire Protocol Framing & Magic Byte Validation (0xCA).[x] Decoupled Interface Architecture (FrameConsumer).[x] Core SQLite JDBC Persistence Pipeline (DatabaseManager).[ ] Finalize Technical Architecture & Experimental sections of the LaTeX report.Task Track B: Teammate Deliverables (Priority for Completion)Swing Monitoring Dashboard (PresentationLayer/BrokerMonitorGUI.java):Construct a JFrame containing:Broker status indicator (Online on 127.0.0.1:9001).A live JTable using DefaultTableModel hooked to incoming events showing Correlation ID, Tier, Payload Size, and Timestamp.A refresh button executing SELECT * FROM message_log to query SQLite.Swarm Flight Simulator (StreamLayer/SwarmSimulator.java):Spin up 3 simulated drones using MeshClient.Transmit periodic GPS coordinates at 200ms intervals using TIER_WORKING (0x00).Trigger emergency battery/geofence warnings using TIER_IMPORTANT (0x02).Report Document & Slide Assembly:Populate the IEEE/ACM Overleaf template using the text, tables, and architecture diagrams documented in this README.Assemble the 6–8 slide presentation deck summarizing Problem Motivation, Architecture, Protocol Design, Implementation Snippets, and Experimental SQLite Output.8. Presentation Slide Deck Layout (6–8 Slides)Slide 1: Title & Author Details: Project title, team member names, registrations, guide/evaluator acknowledgment.Slide 2: Problem Statement & Motivation: Memory/CPU exhaustion of enterprise brokers on embedded edge hardware; flash memory endurance degradation.Slide 3: DESP Core Architecture: The Java NIO Reactor pattern, non-blocking single-threaded event loop, and decoupled storage sinks.Slide 4: Binary Wire Protocol Specification: The 11-byte header breakdown vs. JSON payload overhead; magic byte framing sanity.Slide 5: Object-Oriented & Advanced Java Concepts: Encapsulation in ConnectionSession, abstraction via FrameConsumer, and low-level ByteBuffer manipulation.Slide 6: Persistence & Metadata Layer: SQLite JDBC audit integration, schema layout, and durability guarantees.Slide 7: Experimental Verification: Live demo terminal captures, message throughput verification, and SQL query dumps.Slide 8: Conclusion & Future Scope: Zero-copy Linux sendfile / FileChannel.transferTo(), distributed inter-broker replication (Raft).