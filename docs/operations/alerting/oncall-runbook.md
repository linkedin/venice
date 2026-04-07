# Oncall Runbook

This runbook provides investigation and remediation steps for common Venice alerts. It is organized by component so
operators can quickly find the relevant section when an alert fires.

## How to Use This Runbook

1. **Identify the alert** — Match the firing alert metric name to a section below.
2. **Investigate** — Follow the investigation steps to identify the affected store(s) and host(s).
3. **Remediate** — Apply the recommended fix.

If your investigation uncovers a bug in Venice, please [file an issue](https://github.com/linkedin/venice/issues) on the
Venice repository. For questions or help troubleshooting, post in the Venice Slack community.

### General Triage Workflow

For most alerts, the triage flow is:

1. Identify which store(s) and/or host(s) are affected using your metrics dashboard. Aggregate metrics and sort by max
   value descending to find the top contributors.
2. Determine scope: is this a single host or multiple hosts? A single store or cluster-wide?
3. For single-host issues, the problem is often bad host state — a restart may resolve it.
4. For multi-host or cluster-wide issues, investigate systemic causes (deployments, config changes, capacity).
5. Collect diagnostics (heap dumps, thread dumps, logs) **before** restarting services.

---

## Ingestion and Write Path Alerts

### Hybrid Leader Offset Lag

**Metric:** `current--hybrid_leader_offset_lag`

The leader replica for a hybrid store is falling behind on consuming from the real-time topic. This causes stale data to
be served from affected partitions.

**Investigation steps:**

1. Aggregate the metric across stores and sort by max value descending to find the store(s) causing the lag.
2. If the affected store is a **system meta store** or **system participant store**, perform an empty push to clear the
   lag.
3. Check store-level dashboards to determine if this is a single host or multiple hosts.
4. For a single host, check host-level metrics (CPU, memory, disk I/O) for signs of resource exhaustion.

**Remediation:**

- For system stores: perform an empty push using the Venice admin tool.
- For a single host with bad state: collect a heap dump, then restart the server on that host.
- Consider enabling maintenance mode before restarting to reduce partition movement.

---

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

### Leader Stalled Hybrid Ingestion

**Metric:** `future--leader_stalled_hybrid_ingestion`

The leader replica for a hybrid store has stalled ingestion for a future version. This blocks version swaps and can lead
to push job timeouts.

**Investigation steps:**

1. Aggregate the metric and sort by max value descending to find the store(s) causing the stall.
2. If the affected store is a system meta store or system participant store, perform an empty push to clear the lag.

**Remediation:**

- For system stores: perform an empty push.
- For user stores: investigate server logs for the root cause of the stall.

---

### Real-Time Lag (Local and Remote)

**Metric:** `local_rt_lag` / `remote_rt_lag`

These alerts apply to hybrid or incremental stores in Active-Active replication mode. They indicate the server is
falling behind on consuming real-time data, either from the local or remote datacenter.

**Investigation steps:**

1. Aggregate the metric and sort by max value descending to find the store(s) causing the lag.
2. If the affected store is a system meta store or system participant store, perform an empty push.
3. Check whether the lag is on the local or remote side to narrow down the source.

**Remediation:**

- For system stores: perform an empty push.
- For user stores: investigate server logs and check for resource constraints on the affected host(s).

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

**Metric:** `total--ingestion_failure.Count`

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

### Leader-Follower Delta

**Metric:** `current--leader_follower_delta`

This alert monitors the record count delta between what the leader has produced and what followers have consumed. A
growing delta means followers are falling behind the leader.

**Investigation steps:**

1. Identify which stores are impacted using a store-level breakdown dashboard for the affected datacenter.
2. For each affected store, check the follower consumer metrics to see if it is one or multiple followers lagging
   behind.
3. If only one follower is lagging, it may be a slow host — check host-level resource metrics (CPU, memory, disk I/O).

**Remediation:**

- For a single slow host: investigate host-level issues and consider restarting the server on that host.
- For multiple followers lagging: this suggests a systemic issue — check for recent deployments or config changes.

---

### Stuck Consumer

**Metric:** `stuck_consumer_found`

A value greater than 0 means a shared Kafka consumer task is stuck and is not consuming data. Hybrid partitions assigned
to that consumer will have stale data, and batch partitions will not complete ingestion.

**Investigation steps:**

1. Find the host that is firing this alert.
2. Take a heap dump on the affected host:
   ```bash
   jmap -dump:live,format=b,file=heapdump.hprof <PID>
   ```
3. Check the server log file for the name of the stuck consumer thread. Look for log lines like:
   ```
   Shared consumer couldn't make any progress for over N ms!
   ```

**Remediation:**

- Restart the Venice server on the affected host to recover the stuck consumer.
- If the issue recurs, check for known Kafka consumer bugs (e.g., corrupt record exceptions that are recoverable via
  consumer restart).

---

### Streaming Ingestion Stalled

**Metric:** `streaming_ingestion_stalled`

Streaming ingestion has stalled, meaning that ingestion for online replicas is paused. This can be caused by errors in
the pub-sub client layer or upstream infrastructure issues.

**Investigation steps:**

1. Check server logs for the specific error causing the stall.
2. Identify affected hosts by searching logs for messages indicating stalled ingestion.

**Remediation:**

- Restart the Venice server on the affected hosts to resume ingestion.
- If caused by an upstream infrastructure issue (e.g., pub-sub broker problems), engage the responsible infrastructure
  team.

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

**Metric:** `failed_admin_messages.Count`

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

**Metric:** `ErrorPartitionGauge` (child and parent controllers)

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

**Metric:** `maintenance_mode`

A Venice cluster has entered maintenance mode. While in maintenance mode, Helix will not perform partition reassignment,
which means down replicas will not be replaced.

**Investigation steps:**

1. Investigate why the cluster entered maintenance mode. Common reasons:
   - Enabled manually by an operator for planned maintenance (cluster expansion, node swap, etc.).
   - Number of down instances exceeded `MAX_OFFLINE_INSTANCES_ALLOWED` (automatic).
   - Node crashes or planned infrastructure maintenance.
2. Check the maintenance mode reason in ZooKeeper:
   ```
   /venice/<cluster>/CONTROLLER/MAINTENANCE
   ```

**Remediation:**

- If this is **planned maintenance**: no action needed — take the cluster out of maintenance mode when the maintenance
  is complete.
- If this is **unplanned**: recover the down instances and then take the cluster out of maintenance mode.
- Use the Venice admin tool to disable maintenance mode:
  ```bash
  ./admin_tool.sh --disable-maintenance-mode --url <controller-url> --cluster <cluster-name>
  ```

---

### Rebalance Failure Gauge

**Metric:** `RebalanceFailureGauge`

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

**Metric:** `admin_protocol_auto_detection_service_error_count`

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
  venice.controller.protocol.version.auto.detection.service.enabled=false
  ```

---

## Server Resource Alerts

### JVM Heap Usage

**Metric:** `VeniceJVMStats--HeapUsage`

JVM heap usage is approaching the configured maximum. If heap usage reaches 100%, the application will crash with an
`OutOfMemoryError`. This applies to server, router, and controller processes.

**Investigation steps:**

1. Identify the affected node(s) from your monitoring dashboard.
2. Take a heap dump and thread dump on the affected node(s) as soon as possible:

   ```bash
   # Heap dump
   jmap -dump:live,format=b,file=heapdump.hprof <PID>

   # Thread dump
   jstack <PID> > threaddump.txt
   ```

**Remediation:**

- Restart the application on the affected node(s) as soon as possible to prevent a crash.
- **Important:** Collect heap and thread dumps before restarting — they are essential for root cause analysis.

---

### Store Buffer Service Memory Usage

**Metric:** `StoreBufferService--memory_usage_for_writer_num`

The store buffer service memory usage is high. The store buffer sits between the Kafka consumer and the storage engine,
buffering records before they are written to RocksDB. High usage can indicate a deadlock between the shared-consumer
thread, Kafka producer callback thread, and buffer drainer thread.

**Investigation steps:**

1. Identify the affected node(s) from your monitoring dashboard.
2. Take a heap dump on the affected node immediately:
   ```bash
   jmap -dump:live,format=b,file=heapdump.hprof <PID>
   ```

**Remediation:**

- Restart the Venice server on the affected node to recover. **Consult with your development team before restarting** if
  possible, as the heap dump is critical for diagnosis.

---

### SSD/Disk Health Status

**Metric:** `server_ssd_health_status`

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
- If this is a **no-data-point alert** (the monitoring agent is not emitting data):
  1. Investigate why the monitoring agent is not emitting data and restart it.

---

### Filesystem Usage

**Metric:** `filesystem_usage`

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

**Metric:** `cpu_wait`

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
[RocksDB Write Stalls documentation](https://github.com/facebook/rocksdb/wiki/Write-Stalls).

---

### Server Committed Memory

**Metric:** `os/mem.committed_as`

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

**Metric:** `metaspace_memory_pool_used`

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

**Metric:** `file_descriptor_usage`

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

**Metric:** `participant_store_consumption_heartbeat`

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

**Metric:** `total--unhealthy_host_count_caused_by_router_heart_beat.Max`

The router has detected unhealthy backend server(s) via heartbeat checks. This means the router is unable to route
requests to those servers, reducing serving capacity.

**Investigation steps:**

1. Identify the affected router node(s) and check if the issue is with the router or the backend servers.
2. Check whether there has been a recent upgrade on the router or server.

**Remediation:**

- Take a heap dump and thread dump on the affected router node(s), then restart the router.
- If the problem persists and there was a recent deployment, consider rolling back.

---

### Router CPU Usage

**Metric:** `router_instantaneous_cpu_usage`

Router CPU usage is elevated, which can cause increased latency and request timeouts. Average CPU is not a good
indicator of router saturation — instantaneous/P95 CPU usage is a better signal.

**Investigation steps:**

1. Check if the CPU increase correlates with increased QPS or a new workload being added to the cluster.
2. Verify that the router fleet has sufficient capacity for peak load.

**Remediation:**

- Add enough routers to the cluster so that P95 CPU usage stays under 90% during peak load. Use linear ratio math based
  on current utilization to estimate the required number of additional routers.
- If sustained growth requires fleet expansion, engage your capacity planning team.

---

### Router Active Connection Count

**Metric:** `router_active_connection_count`

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

### Router Memory (Cgroup) Usage

**Metric:** `cgroup_memory_usage_bytes`

Router memory usage is approaching the container/cgroup memory limit. High memory usage can degrade router performance
and eventually cause OOM kills.

**Investigation steps:**

1. Identify which routers have the high memory issue.
2. Common causes:
   - **Planned/unplanned infrastructure maintenance** (e.g., network maintenance on a rack). Check if there is ongoing
     maintenance that may be causing connection pooling issues.
   - **Software defects** — take a heap dump first.
   - **Excessive logging** — check if the router log file is growing rapidly, which can be triggered by issues in other
     components.

**Remediation:**

- If only a few hosts are affected: restart the router on those hosts.
- If the entire fleet is affected: engage your development team immediately to investigate.
- For excessive logging: identify the logging pattern in the router log file and root-cause the issue that is triggering
  the excessive log output.

---

### SSL Handshake Thread Pool Saturation

**Metric:** `ssl_handshake_thread_pool--active_thread_number`

The number of active SSL handshake threads is growing continuously, indicating a possible SSL handshake storm. While the
thread pool throttling protects the server from crashing, users will experience read availability drops. This applies to
server and router processes.

**Investigation steps:**

1. Check whether the number of SSL handshakes keeps growing over time.
2. Check server/router logs for SSL-related errors.
3. Investigate which clients are performing frequent SSL handshakes and why.

**Remediation:**

- Identify and address the root cause of the excessive SSL handshakes (e.g., client misconfiguration, connection pooling
  issues).
- The built-in throttling will protect the server from crashing, but client-side read availability will be degraded
  until the root cause is resolved.

---

## Read Path and Client Alerts

### Compute/Read Latency Spikes (P99)

**Metric:** `compute_storage_engine_read_compute_latency.99thPercentile`

Read compute latency on the server has spiked. This affects read-compute operations where the server performs
computation (e.g., dot product, cosine similarity) on stored data before returning results.

Related metrics: `total--compute_storage_engine_read_compute_deserialization_latency`,
`compute_storage_engine_read_compute_serialization_latency`.

**Investigation steps:**

1. Identify the affected host(s) from your monitoring dashboard.
2. Try collecting a JFR (Java Flight Recorder) profile on the affected host(s), one at a time:
   ```bash
   jcmd <PID> JFR.start duration=10s filename=profile.jfr
   ```

**Remediation:**

- Collecting a JFR profile may itself resolve the issue if it triggers JIT recompilation.
- If JFR does not fix the spike, restart the Venice server on the affected node.

---

### GET/BATCH_GET/BATCH_GET STREAMING Unhealthy Request Count

**Metric:** `unhealthy_request.Count` (per request type)

These metrics monitor the unhealthy (failed) request count for GET, BATCH_GET, and BATCH_GET STREAMING operations from
the router's perspective. This is an aggregated view across all stores.

**Investigation steps:**

1. The aggregated metric does not show which stores are affected. Check per-store unhealthy request metrics to identify
   the impacted store(s).
2. Search server and router logs for the affected store names to identify what caused the unhealthy requests.

**Remediation:**

- Address the root cause based on the logs (e.g., server-side exceptions, timeout issues, resource exhaustion).

---

### Client-Side Unhealthy Requests

**Metric:** `venice_client_unhealthy_requests`

This metric monitors unhealthy requests from the Venice client's perspective for each cluster. It catches issues between
the client and the router layer that may not be visible from the router side alone.

**Investigation steps:**

1. Check if the issue correlates with router-side unhealthy request metrics.
2. Check for network issues between the client service and the Venice routers.
3. Check if the affected cluster's routers are healthy.

**Remediation:**

- Address the underlying cause (router issues, network issues, client-side issues).
- For client-side issues, engage the service owner to coordinate investigation.

---

### Blackbox Monitoring Unhealthy Requests

**Metric:** `blackbox_monitoring_unhealthy_requests`

An external monitoring client (blackbox probe) is detecting unhealthy GET requests to heartbeat stores in each Venice
cluster. Since this runs outside the Venice infrastructure, it provides an independent signal for router health and
reachability.

**Investigation steps:**

1. Check if the Venice routers for the affected cluster are healthy and reachable.
2. Check if there are corresponding alerts from router-side metrics.
3. Verify that the blackbox monitoring client itself is healthy.

**Remediation:**

- Address the underlying router or network issue.
- If the blackbox client itself is unhealthy, restart it.

---

### Leader Heartbeat Delay

**Metric:** `leader_heartbeat_delay`

The leader replica's heartbeat is delayed, which may indicate that the StoreIngestionTask (SIT) thread for the affected
store has died or is blocked.

**Investigation steps:**

1. Identify the problematic store name from the metrics dashboard.
2. Check the server logs for exceptions related to the StoreIngestionTask thread for that store. Filter by time range
   and exception type.
3. If the delay appears to be growing linearly, it is likely caused by a dead SIT thread.
4. If SIT is not the cause, investigate the ingestion path for other bottlenecks.

**Remediation:**

- Restart the Venice server on the affected host to recover the SIT thread.

---

## Infrastructure and Kafka Alerts

### Down Instance Gauge

**Metric:** `DownInstanceGauge`

Two or more Venice server instances are down in a cluster.

**Investigation steps:**

1. Check if there is planned infrastructure maintenance in progress.
2. Identify which instances are down.

**Remediation:**

- If instances are down due to a crash: attempt to reboot the node.
- If the node has bad hardware: swap it out, file a repair ticket, and engage your infrastructure team for replacement.
- If planned maintenance is in progress: no action needed, but monitor for completion.

---

### Host Hardware Faults

**Metric:** `host_hw_faults`

Hardware faults have been detected on Venice hosts. This alert typically fires a couple of days after detecting a
hardware fault or down host. This applies to server, router, and controller hosts.

**Investigation steps:**

1. Identify the problematic host(s) from the alert.
2. Check the host status using your infrastructure management tools.
3. Check if there is planned maintenance affecting the host.
4. For servers:
   - Try rebooting the host.
   - If reboot fails, try re-imaging the host.
   - If re-imaging fails, swap the host out of the cluster.
5. For routers:
   - Try rebooting the host.
   - If reboot fails, swap the host out and add a replacement.

**Remediation:**

- Reboot or re-image the host.
- If the host cannot be recovered, swap it out, file a hardware repair ticket, and engage your infrastructure team for
  replacement.
- After re-imaging, redeploy all Venice applications to the host.

---

### Rack Diversity Issues

**Metric:** `rack_diversity_issue`

Venice hosts in a cluster are not properly distributed across racks/failure domains. Poor rack diversity reduces the
cluster's resilience to rack-level failures.

**Investigation steps:**

1. This alert can occasionally fire due to missing monitoring data. First, check if the monitoring agent is emitting
   data correctly. Restart the monitoring agent if needed.
2. If the data is valid, run rack-awareness diagnostics:
   ```bash
   ./admin_tool.sh --check-rack-awareness --url <controller-url> --cluster <cluster-name>
   ```
3. Check the monitoring logs for collision entries that identify which hosts are in conflicting racks.
4. Identify if recent host swaps caused the conflicts.

**Remediation:**

- If hosts are genuinely in conflicting racks: swap the conflicting host to a different rack/zone.
- If the monitoring tooling has bugs and reports false conflicts: verify independently and report the tooling issue.
- Engage your infrastructure team if host swaps across racks are needed.

---

### Kafka Partition Count

**Metric:** `kafka_partitions_count`

This monitors the total number of Kafka partitions used by Venice. If it approaches the limit of what the Kafka cluster
can handle, it can impact the entire write path.

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

---

### Pod Restart Count

**Metric:** `pod_restart_count`

A Venice pod has restarted multiple times, indicating service instability. Common causes include `CrashLoopBackOff`,
`OOMKilled`, or startup errors. This applies to server, router, and controller pods in Kubernetes deployments.

**Investigation steps:**

1. Inspect pod logs and events:
   ```bash
   kubectl describe pod <pod-name> -n <namespace> --context <k8s-cluster>
   ```
2. Check container logs for specific errors:
   ```bash
   kubectl logs -n <namespace> <pod-name> --context <k8s-cluster>
   ```

**Remediation:**

- Address the root cause identified in the logs (OOM → increase memory limits, crash → fix the bug or config issue).
- For infrastructure-level pod issues (scheduling, resource limits, node problems), engage your Kubernetes platform
  team.

---

### Log Compaction Repush

**Metric:** `log_compaction_repush`

Log compaction repushes are initiated by the leader parent controller. The scheduler runs periodically and schedules
stores with stale versions for repush. This alert fires when scheduled repush jobs fail.

**Investigation steps:**

1. Check the job scheduler for failed repush jobs.
2. Refer to the [Repush documentation](../data-management/repush.md) for general push job debugging.

**Remediation:**

- If a single store is repeatedly failing repush, exclude it from scheduled repush:
  ```bash
  ./admin_tool.sh --update-store --store <STORE> --url http://<PARENT_CONTROLLER>:1576 \
      --cluster <CLUSTER> --enable-compaction false
  ```
- If multiple stores are failing, pause the scheduler until the root cause is resolved.

---

### Error Replica Count

**Metric:** `current_version_error_replica_count`

One or more replicas of the current serving version are in an ERROR or OFFLINE state. This reduces the serving capacity
and redundancy for the affected store(s).

**Investigation steps:**

1. Identify which stores and partitions have error replicas.
2. Engage your development team and, if necessary, the Helix team to investigate why there are error or offline
   partitions.
3. If the alert is due to no data being emitted from the monitoring agent, restart the monitoring service.

**Remediation:**

- Address the underlying cause of the error replicas (host issues, Helix assignment issues, etc.).
- Restarting the monitoring service may resolve false alerts caused by missing data.
