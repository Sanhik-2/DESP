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