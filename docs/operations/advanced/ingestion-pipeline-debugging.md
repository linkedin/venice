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
the same key is updated in two regions, the higher timestamp wins. When timestamps are equal, Venice breaks the tie by
comparing hash codes of the two values — the higher hash code wins. This tie-break is best-effort: on a hash collision
(or equal hash codes), the outcome depends on input order rather than being fully deterministic.

Venice measures ingestion health using **heartbeat records**. The leader periodically injects heartbeat records into the
RT topic. These heartbeats flow through the entire pipeline — the same 7 stages as regular data. A separate
`HeartbeatMonitoringService` thread measures the delay between when a heartbeat was sent and when it was fully
processed. If the heartbeat delay grows, something in the pipeline is stalling — and regular data records are stalling
too.

Here's how data and heartbeats flow in Region A when two regions exist:

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
           ^   |                            |
heartbeats |   | local                      | remote
           |   v                            v
           +---+----------------------------------------+
               |        Leader in Region A              |
               |                                        |
               | 0. Writes heartbeats to RT-A           |
               | 1. Consumes from RT-A and RT-B         |
               | 2. Resolves conflicts via DCR          |
               | 3. Produces to VT-A                    |
               +-------------------+--------------------+
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

This guide covers the full leader path (7 stages) and includes a
[follower debugging section](#debugging-follower-issues) below.

This guide is also useful as a reference for AI agents assisting with Venice debugging — the structured stage-by-stage
format and metric mappings help agents systematically narrow down bottlenecks.

## Pipeline Architecture

The leader ingestion pipeline has 7 stages. Venice measures ingestion health by injecting periodic heartbeat records
into the real-time topic and tracking how long they take to flow through all stages. A stall at **any** stage blocks
heartbeat advancement, because the heartbeat only advances when records flow through the **entire** pipeline.

| Stage                                      | Description                                                          | Key Metrics                                                                                                                                                                                                                                                               |
| ------------------------------------------ | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Kafka Consumer Fetch                    | Consume from real-time topic                                         | `source_broker_to_leader_consumer_latency`, `records_consumed`, `idle_time`, `bytes_per_poll`, `consumer_poll_result_num`                                                                                                                                                 |
| 2. Key-Level Lock                          | Acquire per-key lock for concurrent multi-region writes              | No direct metric — see Stage 2 below                                                                                                                                                                                                                                      |
| 3. Deterministic Conflict Resolution (DCR) | Cache lookup, replication metadata lookup, merge conflict resolution | `leader_preprocessing_latency`, `total_dcr`, `leader_write_compute_lookup_latency`, `leader_write_compute_update_latency`, `leader_ingestion_active_active_put_latency`, `leader_ingestion_active_active_update_latency`, `leader_ingestion_active_active_delete_latency` |
| 4. Version Topic Produce                   | Produce to local version topic                                       | `leader_produce_latency`, `nearline_producer_to_local_broker_latency` (nearline = real-time ingestion)                                                                                                                                                                    |
| 5. Producer Callback                       | Queue records to store buffer                                        | `producer_callback_latency`, `leader_producer_failure_count`, `leader_bytes_produced`, `consumer_records_producing_to_write_buffer_latency`                                                                                                                               |
| 6. Store Buffer                            | Bounded queue (backpressure point)                                   | `consumer_records_queue_put_latency`, `total_memory_usage`, `max_memory_usage_per_writer`, `internal_processing_latency`                                                                                                                                                  |
| 7. RocksDB Write                           | Drainer thread writes to disk                                        | `internal_preprocessing_latency`, `consumed_record_end_to_end_processing_latency`, `batch_processing_request_latency`, `rocksdb.actual-delayed-write-rate`, `storage_engine_put_latency`, `storage_engine_delete_latency`                                                 |
| Heartbeat                                  | Measures lag across all source regions                               | `heartbeat_delay_ms_leader-<region>`, `heartbeat_delay_ms_follower-<region>`                                                                                                                                                                                              |

## Debugging by Stage

### Stage 1: Kafka Consumer Fetch

**Symptom:** High `source_broker_to_leader_consumer_latency`, low `consumer_poll_result_num`, or high `idle_time`.

**Common causes:**

- Source Kafka broker is overloaded or experiencing high latency.
- Network issues between the consuming datacenter and the source broker.
- Consumer is not receiving records (upstream not producing).

**What to check:**

- Kafka broker health and latency metrics.
- Network latency between datacenters (for cross-datacenter replication).
- Consumer group lag on the real-time topic.
- `idle_time` — high values mean the consumer is idle waiting for records.
- `bytes_per_poll` and `consumer_poll_result_num` — low values indicate the consumer is not fetching much data.
- If idle time is low but records are few, the upstream producer may not be sending data.

### Stage 2: Key-Level Lock Acquisition

**Symptom:** High end-to-end processing latency but normal preprocessing latency.

**Common causes:**

- Hot keys receiving concurrent updates from multiple regions in Active-Active stores (stores with multi-region
  replication where all regions accept writes).

**What to check:**

- Whether the store has known hot keys.
- Whether the write rate to specific keys is unusually high.

### Stage 3: Active-Active Message Processing (Deterministic Conflict Resolution)

**Symptom:** High `leader_preprocessing_latency`.

**Common causes:**

- Cache misses forcing RocksDB disk I/O for value or replication metadata (RMD) lookups.
- High DCR merge latency due to complex conflict resolution.
- Disk I/O bottleneck causing slow lookups.
- For stores using write compute, also known as partial update (updating a single field in a record rather than
  replacing the entire value): slow value lookup or update operations.
- Slow internal preprocessing steps (e.g., DIV (Data Integrity Validator) validation, schema checks).

**What to check:**

- DCR metrics: `total_dcr`, `update_ignored_dcr`, `timestamp_regression_dcr_error`.
- Cache hit rates for value and RMD lookups.
- Write compute latencies: `leader_write_compute_lookup_latency`, `leader_write_compute_update_latency`.
- Active-Active specific latencies: `leader_ingestion_active_active_put_latency`,
  `leader_ingestion_active_active_update_latency`, `leader_ingestion_active_active_delete_latency`.
- RocksDB compaction state and disk I/O metrics on the host.

### Stage 4: Produce to Local Version Topic

**Symptom:** High `leader_produce_latency` or `nearline_producer_to_local_broker_latency` (nearline refers to the
real-time ingestion path).

**Common causes:**

- Local Kafka broker is slow or overloaded.
- Topic configuration issues (replication factor, ISR (In-Sync Replicas) shrinkage).
- Kafka producer queue is saturated.

**What to check:**

- Kafka broker health for the local version topic.
- Producer error logs for timeout or rejection errors.
- Kafka producer JMX metrics: `record-queue-time-avg` / `record-queue-time-max` (time records wait before sending),
  `record-error-total` (produce errors), `buffer-available-bytes` (remaining producer buffer). These are standard Kafka
  client metrics, not Venice-specific.

### Stage 5: Leader Producer Callback

**Symptom:** High `producer_callback_latency`, high `consumer_records_producing_to_write_buffer_latency`,
`leader_producer_failure_count` is non-zero, or producer callback errors in logs.

**Common causes:**

- Kafka produce failures (broker rejection, serialization errors, topic authorization).
- Callback thread blocked or slow.
- Slow handoff from callback to store buffer.

**What to check:**

- `producer_callback_latency` — time spent in the producer callback. High values indicate the callback is blocked.
- `consumer_records_producing_to_write_buffer_latency` — time from consuming a record to writing it to the store buffer.
  High values indicate a bottleneck between consuming and buffering.
- Server logs for `LeaderProducerCallback` errors or warnings.
- Whether failures are transient (single spike) or persistent.

### Stage 6: Store Buffer Service (Backpressure)

**Symptom:** High `consumer_records_queue_put_latency`, `total_memory_usage`, or `max_memory_usage_per_writer`. Server
logs show `MemoryBoundBlockingQueue` warnings.

**Common causes:**

- Oversized records filling the buffer — records larger than the notification threshold (default 1MB) can cause the
  callback thread to block on `hasEnoughMemory.await()`.
- The drainer thread (which reads from the buffer and writes to RocksDB) is slow, causing Stage 7 bottleneck to cascade
  upstream.
- Deadlock between the shared Kafka consumer thread (shared across multiple stores and partitions — a stall here blocks
  ingestion for all of them), the producer callback thread, and the buffer drainer thread.

**What to check:**

- `consumer_records_queue_put_latency` — time to put a record into the buffer queue. High values indicate the buffer is
  full and the producer is waiting.
- Server logs for `MemoryBoundBlockingQueue` warnings — these indicate records larger than the notification threshold.
- `total_memory_usage` and `max_memory_usage_per_writer` metrics.
- `internal_processing_latency` — measures time spent processing records in the drainer. High values indicate the
  drainer is slow.
- Whether the issue affects specific partitions (data skew) or is cluster-wide.

### Stage 7: Drainer Thread to RocksDB Write

**Symptom:** High `batch_processing_request_latency`, `rocksdb.actual-delayed-write-rate`, or
`storage_engine_put_latency`.

**Common causes:**

- RocksDB write stalls due to compaction falling behind.
- Disk I/O saturation.
- Slow storage engine puts or deletes.
- `PersistenceFailureException` killing the ingestion task.

**What to check:**

- `internal_preprocessing_latency` — initial validation and bookkeeping time in the drainer (DIV validation, schema
  checks).
- `consumed_record_end_to_end_processing_latency` — full time from record dequeue to completion of processing. Compare
  against `internal_preprocessing_latency` to isolate whether the bottleneck is in validation or in the actual write.
- RocksDB logs for compaction warnings, write stalls, or flush issues.
- Host disk I/O metrics (`iostat`, `iotop`).
- `storage_engine_put_latency` and `storage_engine_delete_latency` — high values indicate the storage engine itself is
  slow, independent of compaction.
- Server logs for `PersistenceFailureException` or `RocksDBException`.
- `ingestion_task_errored_gauge` — a non-zero value means ingestion has stopped.

## Debugging Follower Issues

Followers have a simpler path: they consume from the local version topic and write to RocksDB. They skip Stages 2-5
(lock, DCR, VT produce, producer callback).

**Symptom:** Elevated `heartbeat_delay_ms_follower-<region>` or followers falling behind the leader.

**Investigation steps:**

1. Check if the leader is healthy first. If the leader is slow, follower lag is a downstream effect — fix the leader.
2. Check if the follower is consuming from the version topic: verify `records_consumed` is non-zero and increasing.
3. Check follower-specific latency metrics:
   - `producer_to_local_broker_latency` — latency from when the leader produced a message to when the local broker
     received it.
   - `local_broker_to_follower_consumer_latency` — latency from when the local broker received the message to when the
     follower consumed it. High values in either indicate replication lag between the leader and follower.
4. Check `consumed_record_end_to_end_processing_latency` on the follower — high values indicate the follower is slow to
   process records it consumes.
5. Check `consumer_records_producing_to_write_buffer_latency` — high values indicate a bottleneck between consuming and
   buffering.
6. Check store buffer health: `consumer_records_queue_put_latency`, `total_memory_usage`, `max_memory_usage_per_writer`,
   `internal_processing_latency`. If the buffer is full, the drainer thread may be slow (same as Stage 6/7 for leaders).
7. Check RocksDB health: `storage_engine_put_latency`, `rocksdb.actual-delayed-write-rate`. Compaction or disk issues
   affect followers the same way they affect leaders.
8. Check if the data is stale by comparing the follower's consumed offset with the leader's produced offset.

**Remediation:**

- If the leader is unhealthy: fix the leader first — see the leader stages above.
- If the follower's drainer or RocksDB is slow: see [Stage 6](#stage-6-store-buffer-service-backpressure) and
  [Stage 7](#stage-7-drainer-thread-to-rocksdb-write).
- If the follower is simply behind: it may catch up on its own. Monitor before taking action.

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
| High record-queue-time + low buffer-available-bytes    | Stage 4     | Kafka producer saturated                                           | Check Kafka producer JMX metrics and broker capacity   |
| High DCR latency + regressions                         | Stage 3     | Merge conflict storm from concurrent multi-region updates          | Check DCR metrics and offset regression counts         |
| High Active-Active put/update/delete latency           | Stage 3     | Active-Active conflict resolution is slow                          | Check cache hit rates and RMD lookup latency           |
| High consumer idle time, low poll results              | Stage 1     | Upstream not producing or consumer blocked                         | Check upstream producer health and consumer state      |
| Buffer warnings on specific partitions                 | Stage 6     | Hot partition with large records                                   | Identify partition from logs, check for data skew      |
