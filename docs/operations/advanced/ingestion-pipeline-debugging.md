# Ingestion Pipeline Debugging

This guide provides a deep-dive into the Venice ingestion pipeline for **hybrid stores** — stores that combine batch and
real-time writes.

## Key Concepts

Venice uses two types of Kafka topics per store:

- **Real-time topic (RT)** — where producers write new records.
- **Version topic (VT)** — where resolved data is stored for a specific store version.

Each partition has a **leader** replica and one or more **follower** replicas. The leader consumes from the RT, resolves
data, produces to the VT, and writes to local RocksDB. Followers consume from the VT and write to local RocksDB.

In a multi-region deployment, each region has its own RT and VT. Venice uses **Active-Active replication** — all regions
accept writes simultaneously. Each region's leader consumes from the RT topics of **all regions** (local and remote),
resolves conflicts using deterministic conflict resolution (DCR), and produces the merged result to its local VT. When
the same key is updated in two regions, the higher timestamp wins. Ties are broken by value comparison.

Here's how data flows in Region A when two regions exist:

```
    +---------------+            +---------------+
    |   Region A    |            |   Region B    |
    |   Producers   |            |   Producers   |
    +-------+-------+            +-------+-------+
            |                            |
            v                            v
    +---------------+            +---------------+
    |     RT-A      |            |     RT-B      |
    +-------+-------+            +-------+-------+
            |                            |
            | local                      | remote
            v                            v
    +---------------------+---------------------+
    |            Leader in Region A             |
    |                                           |
    |  1. Consumes from RT-A and RT-B           |
    |  2. Resolves conflicts via DCR            |
    |  3. Produces to VT-A                      |
    +---------------------+---------------------+
                          |
                          v
               +----------+----------+
               |        VT-A         |
               +----------+----------+
                          |
                          v
               +----------+----------+
               | Followers Region A  |
               | Write to RocksDB    |
               +----------+----------+
```

- **Local** — topics in the same region as the replica. For Region A replicas, that's RT-A and VT-A.
- **Remote** — topics in other regions. For example, the leader in Region A consumes from RT-B.

## When to Use This Guide

Use this guide when you observe elevated `heartbeat_delay_ms_leader-<region>` or `heartbeat_delay_ms_follower-<region>`
metrics, when investigating ingestion slowdowns, or when standard troubleshooting like checking recent deployments and
ongoing batch push jobs doesn't explain the issue.

When heartbeats stop making progress, new data records are also not being consumed — the store is serving stale data.

This guide covers the full leader path (7 stages). For follower issues, focus on the stages they share: Kafka fetch
(Stage 1), store buffer (Stage 6), and RocksDB write (Stage 7).

This guide is also useful as a reference for AI agents assisting with Venice debugging — the structured stage-by-stage
format and metric mappings help agents systematically narrow down bottlenecks.

## Pipeline Architecture

The leader ingestion pipeline has 7 stages. Venice measures ingestion health by injecting periodic heartbeat records
into the real-time topic and tracking how long they take to flow through all stages. A stall at **any** stage blocks
heartbeat advancement, because the heartbeat only advances when records flow through the **entire** pipeline.

| Stage                                      | Description                                                          | Key Metrics                                                                                                      |
| ------------------------------------------ | -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| 1. Kafka Consumer Fetch                    | Consume from real-time topic                                         | `source_broker_to_leader_consumer_latency`, `records_consumed`                                                   |
| 2. Key-Level Lock                          | Acquire per-key lock for concurrent multi-region writes              | No direct metric — see Stage 2 below                                                                             |
| 3. Deterministic Conflict Resolution (DCR) | Cache lookup, replication metadata lookup, merge conflict resolution | `leader_preprocessing_latency`, `consumed_record_end_to_end_processing_latency`, `total_dcr`                     |
| 4. Version Topic Produce                   | Produce to local version topic                                       | `producer_to_local_broker_latency`, `nearline_producer_to_local_broker_latency` (nearline = real-time ingestion) |
| 5. Producer Callback                       | Queue records to store buffer                                        | `leader_producer_failure_count`, `leader_bytes_produced`                                                         |
| 6. Store Buffer                            | Bounded queue (backpressure point)                                   | `total_memory_usage`, `max_memory_usage_per_writer`                                                              |
| 7. RocksDB Write                           | Drainer thread writes to disk                                        | `batch_processing_request_latency`, `rocksdb.actual-delayed-write-rate`                                          |
| Heartbeat                                  | Measures lag across all source regions                               | `heartbeat_delay_ms_leader-<region>`, `heartbeat_delay_ms_follower-<region>`                                     |

## Debugging by Stage

### Stage 1: Kafka Consumer Fetch

**Symptom:** High `source_broker_to_leader_consumer_latency`.

**Common causes:**

- Source Kafka broker is overloaded or experiencing high latency.
- Network issues between the consuming datacenter and the source broker.

**What to check:**

- Kafka broker health and latency metrics.
- Network latency between datacenters (for cross-datacenter replication).
- Consumer group lag on the real-time topic.

### Stage 2: Key-Level Lock Acquisition

**Symptom:** High end-to-end processing latency but normal preprocessing latency.

**Common causes:**

- Hot keys receiving concurrent updates from multiple regions in Active-Active stores (stores with multi-region
  replication where all regions accept writes).

**What to check:**

- Whether the store has known hot keys.
- Whether the write rate to specific keys is unusually high.

### Stage 3: Active-Active Message Processing (Deterministic Conflict Resolution)

**Symptom:** High `leader_preprocessing_latency` or `consumed_record_end_to_end_processing_latency`.

**Common causes:**

- Cache misses forcing RocksDB disk I/O for value or replication metadata (RMD) lookups.
- High DCR merge latency due to complex conflict resolution.
- Disk I/O bottleneck causing slow lookups.

**What to check:**

- DCR metrics: `total_dcr`, `update_ignored_dcr`, `timestamp_regression_dcr_error`.
- Cache hit rates for value and RMD lookups.
- RocksDB compaction state and disk I/O metrics on the host.

### Stage 4: Produce to Local Version Topic

**Symptom:** High `producer_to_local_broker_latency` or `nearline_producer_to_local_broker_latency` (nearline refers to
the real-time ingestion path).

**Common causes:**

- Local Kafka broker is slow or overloaded.
- Topic configuration issues (replication factor, ISR (In-Sync Replicas) shrinkage).

**What to check:**

- Kafka broker health for the local version topic.
- Producer error logs for timeout or rejection errors.

### Stage 5: Leader Producer Callback

**Symptom:** `leader_producer_failure_count` is non-zero, or producer callback errors in logs.

**Common causes:**

- Kafka produce failures (broker rejection, serialization errors, topic authorization).
- Callback thread blocked or slow.

**What to check:**

- Server logs for `LeaderProducerCallback` errors or warnings.
- Whether failures are transient (single spike) or persistent.

### Stage 6: Store Buffer Service (Backpressure)

**Symptom:** High `total_memory_usage` or `max_memory_usage_per_writer`. Server logs show `MemoryBoundBlockingQueue`
warnings.

**Common causes:**

- Oversized records filling the buffer — records larger than the notification threshold (default 1MB) can cause the
  callback thread to block on `hasEnoughMemory.await()`.
- The drainer thread (which reads from the buffer and writes to RocksDB) is slow, causing Stage 7 bottleneck to cascade
  upstream.
- Deadlock between the shared Kafka consumer thread, the producer callback thread, and the buffer drainer thread.

**What to check:**

- Server logs for `MemoryBoundBlockingQueue` warnings — these indicate records larger than the notification threshold.
- `total_memory_usage` and `max_memory_usage_per_writer` metrics.
- Whether the issue affects specific partitions (data skew) or is cluster-wide.

### Stage 7: Drainer Thread to RocksDB Write

**Symptom:** High `batch_processing_request_latency` or `rocksdb.actual-delayed-write-rate`.

**Common causes:**

- RocksDB write stalls due to compaction falling behind.
- Disk I/O saturation.
- `PersistenceFailureException` killing the ingestion task.

**What to check:**

- RocksDB logs for compaction warnings, write stalls, or flush issues.
- Host disk I/O metrics (`iostat`, `iotop`).
- Server logs for `PersistenceFailureException` or `RocksDBException`.
- `ingestion_task_errored_gauge` — a non-zero value means ingestion has stopped.

## Common Bottleneck Patterns

| Pattern                                                | Bottleneck  | Likely Root Cause                                                  | Action                                                 |
| ------------------------------------------------------ | ----------- | ------------------------------------------------------------------ | ------------------------------------------------------ |
| Buffer warnings + high version topic produce latency   | Stage 6 + 4 | Oversized records filling buffer while Kafka produce is slow       | Investigate record sizes and Kafka broker health       |
| High version topic produce latency, no buffer warnings | Stage 4     | Kafka broker slowdown                                              | Check Kafka broker health and topic state              |
| High preprocessing + RocksDB errors                    | Stage 3 + 7 | Disk I/O bottleneck causing slow lookups and writes                | Check host disk metrics and RocksDB compaction         |
| High end-to-end but normal preprocessing               | Stage 2     | Key-level lock contention                                          | Check for hot keys in Active-Active stores             |
| High drainer idle time only                            | Stage 6     | Drainer thread blocked or starved (garbage collection, contention) | Check for GC pauses and thread contention              |
| High source broker latency only                        | Stage 1     | Remote Kafka fetch slow                                            | Check source Kafka health and cross-datacenter network |
| All stages spike simultaneously                        | Host-level  | GC pause, OOM (Out of Memory), or node-level issue                 | Check host logs for GC and OOM events                  |
| Producer callback errors                               | Stage 5     | VeniceWriter failures                                              | Check server logs for producer errors                  |
| High DCR latency + regressions                         | Stage 3     | Merge conflict storm from concurrent multi-region updates          | Check DCR metrics and offset regression counts         |
| Buffer warnings on specific partitions                 | Stage 6     | Hot partition with large records                                   | Identify partition from logs, check for data skew      |
