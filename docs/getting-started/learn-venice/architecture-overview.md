# Architecture Overview

Venice is a derived data platform that bridges the offline, nearline, and online worlds. Before diving into the
quickstarts, understanding these core concepts will help you get the most out of Venice.

**Prefer video?** Watch the [Open-Sourcing Venice conference talk](https://www.youtube.com/watch?v=pJeg4V3JgYo) for a comprehensive overview of Venice's architecture, use cases, and real-world deployment at LinkedIn scale.

## Core Concepts

### Stores & Versions
- A **store** is a dataset (like a table or collection)
- Each store consists of immutable **versions** that enable zero-downtime updates
- When you push new data, Venice creates a new version and atomically swaps read traffic

### Partitions & Replication
- Data is horizontally partitioned for scalability
- Each partition is replicated across multiple nodes for availability
- Supports **active-active replication** between regions with CRDT-based conflict resolution

### Write Modes

Venice supports three write granularities that can be mixed:

- **Batch Push**: Full dataset replacement (from Hadoop, Spark, etc.)
- **Incremental Push**: Insert many rows into existing dataset
- **Streaming Writes**: Real-time updates via Kafka, Samza, or Online Producer
- **Hybrid Stores**: Combine batch + streaming for powerful Lambda/Kappa patterns

### Read Modes

Choose your performance/cost tradeoff:

| Client Type | Network Hops | Latency (p99) | State Footprint |
|-------------|--------------|---------------|-----------------|
| **Thin Client** | 2 | < 10ms | Stateless |
| **Fast Client** | 1 | < 2ms | Minimal (routing metadata) |
| **Da Vinci Client** | 0 | < 1ms (SSD) or < 10μs (RAM) | Full dataset locally |

All clients support the same APIs: single get, batch get, and read compute operations.

## Key Features

- **Active-Active Replication**: Write to multiple regions, automatic conflict resolution
- **Write Compute**: Partial updates and collection merging operations
- **Read Compute**: Server-side projections, vector operations (dot product, cosine similarity)
- **Time-to-Live (TTL)**: Automatic data expiration for compliance and storage efficiency
- **Multi-tenancy**: Share clusters across teams with resource isolation
