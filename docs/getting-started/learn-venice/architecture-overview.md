# Architecture Overview

Venice is a derived data platform (data computed from other sources like aggregations, ML features, or materialized
views) that bridges the offline, nearline, and online worlds. Before diving into the quickstarts, understanding these
core concepts will help you get the most out of Venice.

**Prefer video?** Watch the [Open-Sourcing Venice conference talk](https://www.youtube.com/watch?v=pJeg4V3JgYo) for a
comprehensive overview of Venice's architecture, use cases, and real-world deployment at LinkedIn scale.

## Why Venice?

Venice excels at serving derived data with:

- **High throughput ingestion** from batch (Hadoop, Spark) and streaming sources (Kafka, Samza, Flink)
- **Low latency reads** via remote queries (< 10ms) or local caching (< 1ms)
- **Planet-scale deployment** with multi-region active-active replication and CRDT-based conflict resolution

Venice is particularly suitable as the stateful storage layer for **Feature Stores** (like
[Feathr](https://github.com/feathr-ai/feathr)), where ML training outputs need to be served for online inference
workloads.

## Core Concepts

### Stores & Versions

- A **store** is a dataset (like a database table)
- Each store can have multiple **versions** running simultaneously (backup, current, and future during pushes)
- **Batch-only stores**: Versions are immutable snapshots that enable zero-downtime updates via atomic version swaps
- **Hybrid stores**: Combine batch (versioned) and real-time/nearline writes for
  [Lambda/Kappa architecture](merging-batch-and-rt-data.md) patterns
  - Configure **rewind time** to control how far back real-time writes are replayed on top of new batch versions
  - Enables overlaying stream processing outputs on batch job outputs with flexible column update patterns

### Partitions & Replication

- Data is horizontally partitioned for scalability
- Each partition is replicated across multiple nodes for availability
- Supports **active-active replication** between regions with CRDT-based conflict resolution

### Write Modes

Venice supports three write modes with different platform support:

| Write Mode       | Supported Platforms                  | Store Requirement |
| :--------------- | :----------------------------------- | :---------------- |
| Batch Push       | Hadoop, Spark                        | All stores        |
| Incremental Push | Hadoop, Spark (via Venice Push Job)  | Hybrid stores     |
| Streaming Writes | Kafka, Samza, Flink, Online Producer | Hybrid stores     |
| Write Compute    | Any (with Incremental/Streaming)     | Hybrid stores     |

#### Batch Push

Full dataset replacement from batch jobs (Hadoop, Spark, etc.). Creates a new version, loads data in background, then
atomically swaps read traffic for zero-downtime updates. Works with all store types.

#### Incremental Push

Venice push job that takes batch data and inserts into all existing versions (backup, current, future). Useful for bulk
additions without full dataset replacement. **Requires hybrid stores**.

#### Streaming Writes

Real-time updates via Kafka, Samza, Flink, or Online Producer. Writes go to all existing versions (backup, current,
future) to maintain consistency. **Requires hybrid stores**.

#### Write Compute Operations

In addition to full record writes, Venice supports efficient partial updates:

- **Partial update**: Set specific fields within a value without rewriting the entire record
- **Collection merging**: Add or remove entries in sets or maps

### Read Modes

Venice provides three client types with different performance/cost tradeoffs. Clients are designed to share the same
read APIs, enabling cost/performance changes with minimal application changes.

#### Client Types

|                              | Network Hops | Typical latency (p99) |         State Footprint          |
| ---------------------------: | :----------: | :-------------------: | :------------------------------: |
|                  Thin Client |      2       |   < 10 milliseconds   |            Stateless             |
|                  Fast Client |      1       |   < 2 milliseconds    | Minimal (routing metadata only)  |
|  Da Vinci Client (RAM + SSD) |      0       |    < 1 millisecond    | Bounded RAM, full dataset on SSD |
| Da Vinci Client (all-in-RAM) |      0       |   < 10 microseconds   |       Full dataset in RAM        |

All of these clients share the same read APIs described above. This enables users to make changes to their
cost/performance tradeoff without needing to rewrite their applications.

**Network hops explained**:

- Thin Client → Router tier → Server tier (2 hops)
- Fast Client → Server tier directly (1 hop, partition-aware)
- Da Vinci Client → Local storage (0 hops, eagerly loads partitions)

#### Read APIs

All clients support:

- **Single get**: Retrieve value for one key
- **Batch get**: Retrieve values for multiple keys
- **Read compute**: Server-side operations that reduce network traffic:
  - Field projection (return subset of fields)
  - Dot product, cosine similarity, Hadamard product on vectors
  - Collection count operations

#### Change Data Capture (CDC)

Venice supports **streaming change data capture** to subscribe to all data changes (inserts, updates, deletes). A common
use case is populating client-side indexes for ML feature retrieval.
