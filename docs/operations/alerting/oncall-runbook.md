# Oncall Runbook

This runbook provides investigation and remediation steps for common Venice alerts. It is organized by component so
operators can quickly find the relevant section when an alert fires.

## How to Use This Runbook

1. **Identify the alert** — Match the firing alert metric name to a section below.
2. **Investigate** — Follow the investigation steps to identify the affected store(s) and host(s).
3. **Remediate** — Apply the recommended fix.

If your investigation uncovers a bug in Venice, please [file an issue](https://github.com/linkedin/venice/issues) on the
Venice repository. For questions or help troubleshooting, post in the
[Venice Slack community](http://slack.venicedb.org).

### General Triage Workflow

For most alerts, the triage flow is:

1. Identify which store(s) and/or host(s) are affected using your metrics dashboard. Aggregate metrics and sort by max
   value descending to find the top contributors.
2. Determine scope: is this a single host or multiple hosts? A single store or cluster-wide?
3. For single-host issues, the problem is often bad host state — a restart may resolve it.
4. For multi-host or cluster-wide issues, investigate systemic causes (deployments, config changes, capacity).
5. Collect diagnostics **before** restarting services. See
   [Collecting Diagnostics](../advanced/collecting-diagnostics.md) for how to capture heap dumps, thread dumps, and JFR
   profiles.

---

## Ingestion and Write Path Alerts

### Ingestion Task Errored Gauge

**Metric:** `current--ingestion_task_errored_gauge`

A value greater than 0 means the ingestion task for a store has stopped due to an error. Hybrid stores will serve stale
data from that point on, and batch stores will stop processing control messages.

**Investigation steps:**

1. Aggregate the metric and sort by max value to find the affected store(s).
2. Check the server logs for the affected store(s) to identify the exception that caused the ingestion task to stop.
3. Check `total--bytes_consumed` for the affected store(s) to confirm ingestion has stopped around the same time.

**Remediation:**

- Restart the affected server node to recover the ingestion task.
- If the error recurs after restart, investigate the root cause in the server logs.

---

### Leader Producer Failure Count

**Metric:** `total--leader_producer_failure_count`

One or more stores have experienced Kafka producer failures on the leader replica. This typically indicates that a push
job or real-time ingestion has failed in the specified datacenter.

**Investigation steps:**

1. Aggregate the metric and sort by max value descending to find the store(s) involved.
2. Check the server logs for producer-related exceptions.
3. Check whether there are corresponding issues with the backup, current, or future store versions.

**Remediation:**

- Investigate the root cause in server logs.
- If caused by a transient Kafka issue, the producer may recover automatically.

---

### Ingestion Failure Count

**Metric:** `total--ingestion_failure`

General ingestion failures have been detected. This is a broad indicator that something in the ingestion pipeline is
failing.

**Investigation steps:**

1. Check server logs for the specific exception(s) causing ingestion failures.
2. Correlate with other alerts (leader offset lag, producer failures) to identify the root cause.

**Remediation:**

- Address the underlying cause identified in the logs.
- Restart the affected server if the issue is caused by transient state.

---

### Timestamp Regression DCR Error

**Metric:** `total--timestamp_regression_dcr_error`

An error was found in the Deterministic Conflict Resolution (DCR) logic used by Active-Active replication. Timestamp
regression means a record arrived with a timestamp older than the existing value, which should not happen under normal
operation.

**Investigation steps:**

1. Check server logs for the specific DCR error details.
2. Identify which store(s) and partition(s) are affected.

**Remediation:**

- This typically requires developer investigation to determine the root cause.

---

### Stuck Consumer

**Metric:** `stuck_consumer_found`

A value greater than 0 means a shared Kafka consumer task is stuck and is not consuming data. Hybrid partitions assigned
to that consumer will have stale data, and batch partitions will not complete ingestion.

**Investigation steps:**

1. Find the host that is firing this alert.
2. [Collect a heap dump](../advanced/collecting-diagnostics.md#heap-dump) on the affected host.
3. Check the server log file for the name of the stuck consumer thread. Look for log lines like:
   ```
   Shared consumer couldn't make any progress for over N ms!
   ```

**Remediation:**

- Restart the Venice server on the affected host to recover the stuck consumer.
- If the issue recurs, check for known Kafka consumer bugs (e.g., corrupt record exceptions that are recoverable via
  consumer restart).

---

## Controller Alerts

### Admin Message Errors

**Metric:** `admin_message_div_error_report_count`

The controller encountered errors processing admin messages. Admin messages are used for cluster coordination operations
(store creation, version swaps, config changes, etc.).

**Investigation steps:**

1. Check controller logs for the specific error.
2. Determine if the error is transient or persistent.

**Remediation:**

- This may require restarting parent controllers and skipping failed admin messages.
- **Important:** Confirm with your development team before skipping admin messages, as this can cause inconsistencies if
  done incorrectly.

---

### Failed Admin Messages

**Metric:** `failed_admin_messages`

Admin messages are failing to be processed by the controller. This can block store operations across the cluster.

**Investigation steps:**

1. Check if the cluster is in **maintenance mode** — failed admin messages are expected during maintenance mode. Verify
   the cluster is not in maintenance mode before investigating other causes.
2. Check controller logs for the specific exception causing the failures.

**Remediation:**

- If caused by maintenance mode: take the cluster out of maintenance mode once the maintenance is complete.
- If not caused by maintenance mode: this may require skipping the failed admin messages.
- **Important:** Confirm with your development team before skipping admin messages.

---

### Controller Error Partition Gauge

**Metric:** `ErrorPartitionGauge` (child and parent controllers, emitted by Apache Helix)

One or more cluster resources (Helix resources) are in an ERROR state. This can affect store availability and partition
assignment.

**Investigation steps:**

1. Check the Helix controller UI or API for cluster resource(s) that are in ERROR state:
   ```
   GET /admin/v2/clusters/<cluster>/resources
   ```
2. Check controller logs to see if an exception was thrown.
3. If this alert fires right after a config or code deployment, consider rolling back.

**Remediation:**

- If caused by a recent deployment: roll back the change.
- If caused by a transient issue: the Helix controller may self-recover once the underlying issue is resolved.

---

### Maintenance Mode

**Metric:** `maintenance_mode` (Helix cluster state)

A Venice cluster has entered maintenance mode. While in maintenance mode, Helix will not perform partition reassignment,
which means down replicas will not be replaced.

**Investigation steps:**

1. Investigate why the cluster entered maintenance mode. Common reasons:
   - Enabled manually by an operator for planned maintenance (cluster expansion, node swap, etc.).
   - A Venice deployment with concurrency higher than `MAX_OFFLINE_INSTANCES_ALLOWED`.
   - Number of down instances exceeded `MAX_OFFLINE_INSTANCES_ALLOWED` (automatic).
   - Node crashes or infrastructure maintenance.
2. Check the maintenance mode reason in ZooKeeper:
   ```
   /venice/<cluster>/CONTROLLER/MAINTENANCE
   ```

**Remediation:**

- If this is **planned maintenance**: no action needed — take the cluster out of maintenance mode when the maintenance
  is complete.
- If this is **unplanned**: recover the down instances and then take the cluster out of maintenance mode.
- Disable maintenance mode via the Helix REST API or your cluster management tooling.

---

### Rebalance Failure Gauge

**Metric:** `RebalanceFailureGauge` (emitted by Apache Helix)

A metric value of 1 indicates that the Helix controller is unable to perform resource creation, assignment, or
rebalance. Common causes include bad rack-aware configurations or bad cluster configurations. If the cluster enters
maintenance mode, rebalance failures will also occur.

**Investigation steps:**

1. Check if the cluster is in maintenance mode — rebalance failures are expected during maintenance mode.
2. Check the Helix controller logs for rebalance-related exceptions. Look for messages like:
   ```
   Failed to calculate best possible states for resource ...
   Error computing assignment ...
   ```
3. Check for recent configuration changes that may have introduced bad rack-aware or cluster configs.

**Remediation:**

- If caused by maintenance mode: resolve the maintenance mode issue first.
- If caused by bad configuration: revert the configuration change.
- If caused by a specific resource: identify and fix the problematic resource.
- If the rebalance failure persists, engage Helix experts to assist with diagnosis.

---

### Protocol Auto-Detection Service Errors

**Metric:** `protocol_version_auto_detection_error`

The protocol version auto-detection service runs periodically (every 10 minutes) to find the minimum admin operation
protocol version across all controller instances. If the service runs successfully, the error count drops to 0. A
non-zero value means the detection is failing.

**Investigation steps:**

1. Check the parent controller logs for `ProtocolVersionAutoDetectionService` entries.
2. A successful run will log:
   ```
   Current good Admin Operation version for cluster <cluster> is N and upstream version is N
   ```
3. If the service is failing, determine whether it is the parent or child controller that is failing.
4. This service relies on leader detection — if there is no leader, investigate why leader election has failed.
5. Check ZooKeeper for the current protocol version:
   ```
   /venice-parent/<cluster>/adminTopicMetadata
   ```

**Remediation:**

- If there is no leader controller, investigate and resolve the leader election issue.
- The service can be disabled via controller configuration if needed as a temporary workaround:
  ```
  controller.protocol.version.auto.detection.service.enabled=false
  ```

---

## Server Resource Alerts

### JVM Heap Usage

**Metric:** `VeniceJVMStats--HeapUsage`

JVM heap usage is approaching the configured maximum. If heap usage reaches 100%, the application will crash with an
`OutOfMemoryError`. This applies to server, router, and controller processes.

**Investigation steps:**

1. Identify the affected node(s) from your monitoring dashboard.
2. Collect a [heap dump](../advanced/collecting-diagnostics.md#heap-dump) and
   [thread dump](../advanced/collecting-diagnostics.md#thread-dump) on the affected node(s) as soon as possible.

**Remediation:**

- Restart the application on the affected node(s) as soon as possible to prevent a crash.
- **Important:** Collect diagnostics before restarting — they are essential for root cause analysis.

---

### Store Buffer Service Memory Usage

**Metric:** `total_memory_usage` or `max_memory_usage_per_writer`

The store buffer service memory usage is high. The store buffer sits between the Kafka consumer and the storage engine,
buffering records before they are written to RocksDB. Metrics are emitted per drainer type (sorted/unsorted). High usage
can indicate a deadlock between the shared-consumer thread, Kafka producer callback thread, and buffer drainer thread.

**Investigation steps:**

1. Identify the affected node(s) from your monitoring dashboard.
2. Collect a [heap dump](../advanced/collecting-diagnostics.md#heap-dump) on the affected node immediately.

**Remediation:**

- Restart the Venice server on the affected node to recover. **Collect diagnostics before restarting** — the heap dump
  is critical for diagnosis.
- See [Stage 6: Store Buffer](../advanced/ingestion-pipeline-debugging.md#stage-6-store-buffer-service-backpressure) for
  deeper analysis of buffer backpressure.

---

### SSD/Disk Health Status

**Metric:** `disk_healthy`

The SSD on a Venice server node may be degraded or failing. Venice servers use local SSDs for RocksDB storage, so disk
health is critical for data serving.

**Investigation steps:**

1. SSH onto the affected host and check disk health:

   ```bash
   # Check kernel messages for NVMe errors
   dmesg -T | grep nvme

   # List NVMe devices (if no device is listed, the disk is dead)
   sudo nvme list

   # Check if the disk mounts properly
   sudo mount -av
   ```

**Remediation:**

- If the SSD shows errors but is still functional:
  1. Try remounting: `sudo umount /mnt/data; sudo mount -a`
  2. If remounting does not fix the issue, reboot the host.
- If the SSD is dead (not detected by `nvme list`):
  1. Swap the node out of the cluster.
  2. File a hardware ticket for disk replacement, then re-image the host.

---

### Filesystem Usage

**Metric:** `filesystem_usage` (OS-level, e.g., node_exporter or collectd)

Disk usage on a server or router node has exceeded the alert threshold. High disk usage can degrade performance and
eventually cause the server to stop accepting writes.

**Investigation steps:**

1. Identify which hosts are affected and check the file usage pattern for the cluster.
2. **Unbalanced partition assignment:** If some hosts have decreased usage while others have increased, this may
   indicate an unbalanced Helix resource assignment.
3. **General high usage:** SSH into a problematic host and check disk usage:

   ```bash
   # Check which filesystem has the issue
   df -h

   # Check log sizes
   du -sh /path/to/logs/

   # Check RocksDB data size
   du -sh /path/to/venice/data/rocksdb/

   # Check for unexpected files in /tmp
   du -sh /tmp/*
   ```

4. For router nodes:
   ```bash
   sudo du -hx --exclude=/proc / | sort -hr | head -n 10
   ```

**Remediation:**

- **Unbalanced assignment:** Add more hosts to the affected zones so partitions are distributed more evenly. Work with
  the Helix team to understand the cause of the imbalance.
- **Log accumulation:** Clean up old log files or adjust log rotation settings.
- **General high usage:** Identify and remove the source of unexpected disk consumption.

---

### CPU Wait

**Metric:** `cpu_wait` (OS-level, e.g., node_exporter or collectd)

The CPU is spending a significant amount of time waiting for I/O operations to complete. This usually indicates hardware
issues on the underlying disk.

**Investigation steps:**

1. Check kernel logs for hardware failures:
   ```bash
   dmesg -T
   ```
2. Check I/O metrics for processes doing unexpected read/write.
3. Use `iotop` to identify the process (if any):
   ```bash
   sudo iotop -o -P -a
   ```
   Press left/right arrow keys to sort by different columns.
4. For Venice servers, check NVMe write statistics to confirm high I/O usage. If the problematic hosts also have higher
   wait time than other hosts, this usually indicates a hardware failure.

**Remediation:**

- If caused by a rogue process: identify and stop the process, then reboot if needed.
- If caused by hardware failure: swap the host out of the cluster and file a hardware repair ticket.

---

### RocksDB Delayed Write Rate

**Metric:** `rocksdb.actual-delayed-write-rate`

RocksDB is throttling writes due to write stalls. This can occur when compaction cannot keep up with the incoming write
rate, causing L0 SST files to accumulate.

Before investigating, check whether there are any offset/time lag alerts on the hybrid current version. If there are
none, a random spike of this metric is benign and does not break SLA guarantees.

**Investigation steps:**

1. Check if there are corresponding offset lag alerts for hybrid stores. If not, this may be benign.
2. To identify stores with high write throughput, check the per-store consumed bytes metrics for the cluster.
3. Check server logs for frequent offset update/sync entries.
4. Review RocksDB logs (typically stored alongside the data directory) for compaction-related warnings.

**Remediation:**

Venice tunes several RocksDB configs to reduce write stalls:

- **Increase total write buffer size** to accommodate more memtables and avoid premature flush.
- **Reduce individual memtable size** to fit more memtables within the total buffer.
- **Increase compaction threads** to speed up compaction.
- **Reduce L1 target size** to match L0 size, speeding up L0-to-L1 compaction.

If these tuning strategies do not reduce the lag, it may be a **capacity issue** — adding more server nodes can help
distribute the write load.

For detailed information on RocksDB write stalls, see the
[RocksDB Write Stalls documentation](https://github.com/facebook/rocksdb/wiki/Write-Stalls). See also
[Stage 7: RocksDB Write](../advanced/ingestion-pipeline-debugging.md#stage-7-drainer-thread-to-rocksdb-write) for how
write stalls affect the ingestion pipeline.

---

### Server Committed Memory

**Metric:** `os/mem.committed_as` (OS-level, from `/proc/meminfo`)

The operating system's committed memory is higher than expected. This can be caused by huge pages not being properly
reserved by the application.

**Investigation steps:**

1. SSH into the affected host and check the HugePages reservation:
   ```bash
   grep -i huge /proc/meminfo
   ```
2. If `HugePages_Rsvd` is 0 but `HugePages_Total` is non-zero, the application is not using the reserved huge pages.

**Remediation:**

- If `HugePages_Rsvd` is 0: restart the Venice server so it properly reserves huge pages on startup.

---

### Metaspace Memory Usage

**Metric:** `metaspace_memory_pool_used` (JVM-level, from JMX `java.lang:type=MemoryPool`)

The JVM metaspace usage has exceeded the alert threshold. This indicates the application is either loading an excessive
number of classes or experiencing a classloader memory leak. This applies to server, router, and controller processes.

**Investigation steps:**

1. Examine historical patterns to identify when the problem first appeared.
2. Review code and dependency changes made around that time period.
3. To pinpoint the problematic change, use a `git bisect` approach — deploy older versions to verify which change
   introduced the regression.

**Remediation:**

- Revert the problematic change if identified.
- Restart the affected node as a temporary mitigation.

---

### File Descriptor Usage

**Metric:** `file_descriptor_usage` (OS-level, from `/proc/<PID>/fd`)

The service is approaching its file descriptor limit. When services run out of file descriptors, they cannot open new
files for writing or accept new network connections. This applies to server and router processes.

**Investigation steps:**

1. Check the file descriptor limit on the host:
   ```bash
   cat /proc/<PID>/limits
   ```
2. Check the current file descriptor usage:
   ```bash
   lsof -p <PID> | wc -l
   ```
3. Determine if the limit is too low or if the service is behaving abnormally.

**Remediation:**

- If the limit is low: increase the file descriptor limit in your system configuration (e.g.,
  `/etc/security/limits.conf`).
- If the service is consuming excessive file descriptors: investigate the root cause (connection leaks, file handle
  leaks, etc.).
- As a temporary mitigation, add additional hosts to the cluster to distribute the load.

---

### Participant Store Consumption Task Stuck or Dead

**Metric:** `<cluster>-participant_store_consumption_task--heartbeat`

The participant store consumption task has stopped emitting heartbeats. This task processes participant messages that
coordinate server-level operations. The alert can trigger because:

- The consumption task is taking too long to process messages (may auto-recover).
- The consumption task has died due to an unhandled exception.

**Investigation steps:**

1. Check server logs for exceptions related to the participant store consumption task.
2. Check if the heartbeat resumes on its own (transient delay).

**Remediation:**

- Restart the Venice server to recover or reproduce the error.

---

## Router Alerts

### Unhealthy Host Count (Router Heartbeat)

**Metric:** `total--unhealthy_host_count_caused_by_router_heart_beat`

The router has detected unhealthy backend server(s) via heartbeat checks. This means the router is unable to route
requests to those servers, reducing serving capacity.

**Investigation steps:**

1. Identify the affected router node(s) and check if the issue is with the router or the backend servers.
2. Check whether there has been a recent upgrade on the router or server.

**Remediation:**

- Collect [diagnostics](../advanced/collecting-diagnostics.md) on the affected router node(s), then restart the router.
- If the problem persists and there was a recent deployment, consider rolling back.

---

### Router CPU Usage

**Metric:** `router_instantaneous_cpu_usage` (OS/container-level, from process CPU monitoring)

Router CPU usage is elevated, which can cause increased latency and request timeouts. Average CPU is not a good
indicator of router saturation — instantaneous/P95 CPU usage is a better signal.

**Investigation steps:**

1. Check if the CPU increase correlates with increased QPS or a new workload being added to the cluster.
2. If the cause is unclear, collect a
   [JFR profile](../advanced/collecting-diagnostics.md#jfr-java-flight-recorder-profile) to identify hot methods
   consuming CPU.
3. Verify that the router fleet has sufficient capacity for peak load.

**Remediation:**

- Add enough routers to the cluster so that P95 CPU usage stays under 90% during peak load. Use linear ratio math based
  on current utilization to estimate the required number of additional routers.
- If sustained growth requires fleet expansion, engage your capacity planning team.

---

### Router Active Connection Count

**Metric:** `connection_pool--total_active_connection_count`

The number of active connections to a router is elevated. This can indicate increased traffic or a single router
receiving disproportionate load.

**Investigation steps:**

1. Check if QPS is increasing alongside the connection count.
2. If only one router in the cluster has significantly higher QPS and connection count, check if a load test or traffic
   migration is in progress.

**Remediation:**

- If caused by uneven load distribution: investigate your load balancer configuration.
- If caused by overall traffic growth: add more routers.

---

### Router Container Memory Usage

**Metric:** `container_memory_usage_bytes` (from your container monitoring, e.g., cAdvisor, kubelet, or Prometheus)

Router memory usage is approaching the container's memory limit. High memory usage can degrade router performance and
eventually cause OOM kills by the container runtime.

**Investigation steps:**

1. Identify which routers have the high memory issue.
2. Common causes:
   - **Planned/unplanned infrastructure maintenance** (e.g., network maintenance on a rack). Check if there is ongoing
     maintenance that may be causing connection pooling issues.
   - **Software defects** — [collect a heap dump](../advanced/collecting-diagnostics.md#heap-dump) first.
   - **Excessive logging** — check if the router log file is growing rapidly, which can be triggered by issues in other
     components.

**Remediation:**

- If only a few hosts are affected: restart the router on those hosts.
- If the entire fleet is affected: engage your development team immediately to investigate.
- For excessive logging: identify the logging pattern in the router log file and root-cause the issue that is triggering
  the excessive log output.
- If the container is being OOM killed repeatedly, increase the container memory limit in your orchestrator's config.
  Ensure the JVM heap size (`-Xmx`) leaves enough headroom for off-heap memory (thread stacks, direct buffers, native
  allocations) — a common misconfiguration is setting `-Xmx` too close to the container limit.

---

### SSL Handshake Thread Pool Saturation

**Metric:** `ssl_handshake_thread_pool--active_thread_number`

The number of active SSL handshake threads is growing continuously, indicating a possible SSL handshake storm. While the
thread pool throttling protects the server from crashing, users will experience read availability drops. This applies to
server and router processes.

**Investigation steps:**

1. Check whether the number of SSL handshakes keeps growing over time.
2. Check server/router logs for SSL-related errors.
3. Collect a [JFR profile](../advanced/collecting-diagnostics.md#jfr-java-flight-recorder-profile) to identify thread
   contention in the SSL handshake pool.
4. Investigate which clients are performing frequent SSL handshakes and why.

**Remediation:**

- Identify and address the root cause of the excessive SSL handshakes (e.g., client misconfiguration, connection pooling
  issues).
- The built-in throttling will protect the server from crashing, but client-side read availability will be degraded
  until the root cause is resolved.

---

## Read Path and Client Alerts

### Compute/Read Latency Spikes (P99)

**Metric:** `compute_storage_engine_read_compute_latency`

Read compute latency on the server has spiked. This affects read-compute operations where the server performs
computation (e.g., dot product, cosine similarity) on stored data before returning results.

Related metrics: `compute_storage_engine_read_compute_deserialization_latency`,
`compute_storage_engine_read_compute_serialization_latency`.

**Investigation steps:**

1. Identify the affected host(s) from your monitoring dashboard.
2. Collect a [JFR profile](../advanced/collecting-diagnostics.md#jfr-java-flight-recorder-profile) on the affected
   host(s), one at a time.

**Remediation:**

- Collecting a JFR profile may itself resolve the issue if it triggers JIT recompilation.
- If JFR does not fix the spike, restart the Venice server on the affected node.

---

### Unhealthy Request Count

**Metric:** `unhealthy_request` (per request type: GET, BATCH_GET, BATCH_GET STREAMING)

This metric tracks unhealthy (failed) requests and is emitted from two perspectives:

- **Router-side** — aggregated across all stores routed through Venice routers.
- **Client-side** — per cluster, as observed by the Venice client. Counts failures that never reach the router (network,
  TLS, connection pool exhaustion, retry exhaustion) in addition to failures the router returns.

**Investigation steps:**

Investigate from the server outward — an unhealthy server makes routers look unhealthy, and slow routers cause request
buildup in the client that can trigger GC pressure and other compounding issues. Working up the stack isolates the real
root cause rather than chasing symptoms.

1. Identify the affected store(s). The aggregated metric does not show which stores are impacted — check per-store
   unhealthy request metrics.
2. **Check server health** for the affected store(s). Review server logs for store-specific exceptions and check server
   health signals (see [Compute/Read Latency Spikes (P99)](#computeread-latency-spikes-p99),
   [Ingestion Task Errored Gauge](#ingestion-task-errored-gauge), [JVM Heap Usage](#jvm-heap-usage)).
3. **If servers are healthy, check router health.** Review router logs for routing or timeout issues and check
   [Router CPU Usage](#router-cpu-usage) and [Router Active Connection Count](#router-active-connection-count).
4. **If routers are healthy, check the network** between the client and the routers.
5. **If all upstream is healthy, the issue is in the client itself** — check client-side GC pauses, CPU usage, and
   resource exhaustion.

**Remediation:**

- Address the underlying cause at the layer where the issue was found (server, router, network, or client).
- For client-side issues, engage the service owner to coordinate investigation.

---

### Leader Heartbeat Delay

**Metric:** `heartbeat_delay_ms_leader-<region>`

The leader heartbeat delay reflects the health of the **entire ingestion pipeline**, not just the heartbeat mechanism.
The heartbeat only advances when records flow through all 7 stages of the pipeline (Kafka fetch, lock acquisition, DCR
processing, VT produce, producer callback, store buffer, RocksDB write). A stall at any stage will cause this metric to
rise.

**Investigation steps:**

1. Identify the problematic store name from the metrics dashboard.
2. Check if the delay correlates with a deployment, push job, config change, or a sudden increase in real-time write
   traffic (from online producers, nearline producers, or incremental pushes). An increase in write volume can overwhelm
   the pipeline across multiple stages.
3. Check server logs for exceptions related to the StoreIngestionTask thread. If the delay is growing linearly, the SIT
   thread is likely dead.
4. If SIT is not the cause, walk through the ingestion pipeline stages to find the bottleneck. See
   [Ingestion Pipeline Debugging](../advanced/ingestion-pipeline-debugging.md) for a stage-by-stage guide.

**Remediation:**

- If the SIT thread is dead: restart the Venice server on the affected host.
- If the bottleneck is in a specific pipeline stage, address that stage (e.g., RocksDB write stalls, Kafka producer
  slowdowns, buffer backpressure). See the
  [common bottleneck patterns](../advanced/ingestion-pipeline-debugging.md#common-bottleneck-patterns) for guidance.

---

## Infrastructure and Kafka Alerts

### Down Instance Gauge

**Metric:** `DownInstanceGauge` (emitted by Apache Helix)

One or more Venice server or controller instances have been down for an extended period of time in a cluster.

**Investigation steps:**

1. Identify which instances are down and what component they run (server or controller).
2. Check if the host is reachable (SSH, ping). If the host is unreachable, it may be a hardware or network failure.
3. If reachable, check server/controller logs on the affected instances for crash or startup errors.
4. Check host health: `dmesg -T` for kernel errors, `df -h` for disk space, `free -h` for memory, `nvme list` for disk
   presence.
5. Check if a recent deployment failed to complete — instances that don't come back after a deployment indicate a
   regression (startup crash, config error).
6. Check ZooKeeper health: are all ZK nodes up and is the quorum healthy?
7. Check Helix controller health: is the Helix controller leader elected? Are there rebalance errors?

**Remediation:**

- If the host is unreachable or has bad hardware: swap it out and replace the host.
- If instances are down due to a crash: address the root cause in the logs, then attempt to reboot.
- If caused by a deployment regression: roll back the deployment.
- If ZooKeeper or Helix is unhealthy: address the ZK/Helix issue first, as instance state may not recover until the
  coordination layer is healthy.

---

### Kafka Partition Count

**Metric:** `kafka_partitions_count` (Kafka broker-level, from JMX or Kafka metrics reporter)

This monitors the total number of Kafka partitions used by Venice. If it approaches the limit of what the Kafka cluster
can handle, it can impact the entire write path. For reference, Venice at LinkedIn runs on approximately 2 million
partitions, but your limits may be significantly lower depending on your Kafka version, hardware, and cluster size.
KRaft-based Kafka clusters support significantly higher partition counts than ZooKeeper-based clusters. Confluent
recommends
[2,000–4,000 partitions per broker](https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/) as
a general guideline for ZooKeeper-based clusters.

Note: The alert threshold is generally set lower than the Kafka cluster's actual limit because early action is needed to
prevent larger problems.

**Investigation steps:**

1. Check if there is an abnormal surge in partition count (e.g., due to a misconfiguration or runaway store creation).
2. If growth is normal (organic), coordinate with your Kafka team to understand the current limits and plan for scaling.

**Remediation:**

- **Abnormal surge:** Investigate and address the root cause (e.g., clean up unused topics, fix misconfiguration).
- **Organic growth:** Work with your Kafka team to:
  1. Project when Venice will reach the Kafka limit.
  2. Scale up the Kafka cluster.
  3. Increase the alert threshold once the Kafka cluster can safely handle the higher count.
