# Collecting Diagnostics

When investigating Venice issues, collect diagnostics **before** restarting services — they are essential for root cause
analysis. These tools apply to any Venice JVM process (server, router, controller, or client).

## Heap Dump

Use when: high memory usage, suspected memory leaks, store buffer memory issues, or when you need to inspect runtime
state that isn't visible in logs.

```bash
jmap -dump:live,format=b,file=heapdump.hprof <PID>
```

Heap dumps capture the full state of the JVM heap — which objects are consuming memory, and the actual values of
variables and object fields at the time of capture. Analyze with
[HeapDumpStarDiver](https://github.com/ZacAttack/HeapDumpStarDiver), which converts heap dumps to Parquet files for SQL
analysis and includes an MCP server that enables AI agents to query heap data and assist with memory analysis.

## Thread Dump

Use when: suspected deadlocks, stuck threads, high CPU with no obvious cause.

```bash
jstack <PID> > threaddump.txt
```

Thread dumps show what every thread is doing at that moment. Look for threads in `BLOCKED` or `WAITING` state, and for
lock contention between threads.

### Server Netty Request Threads

The server records the OpenTelemetry distribution `thread_pool.thread.active_request_distribution` with
`venice.thread_pool.name=netty_worker_thread_pool` whenever a complete HTTP request enters the request pipeline. It
counts workers concurrently executing the synchronous request pipeline across HTTP/1.1 connections and HTTP/2 streams.
This metric has no Tehuti equivalent.

Use the request-sampled average and maximum to understand request-processing concurrency within the reporting window.
They are not time-weighted utilization or an in-flight request count: asynchronous storage work, response-only writes,
TLS handshakes, and socket I/O outside the request pipeline are excluded. Idle periods produce no samples, and even a
single request contributes a count of one.

### Stalled Drainer Operation Diagnostics

Set the live cluster configuration `server.stalled.drainer.diagnostics.enabled=true` and
`server.stalled.drainer.diagnostics.host.allowlist` through the controller's `update_cluster_config` API to enable
additional stalled-drainer breadcrumbs on selected hosts without restarting servers. The master flag defaults to
`false`; the allowlist defaults to empty. Both a true flag and host membership are required. An empty, missing, or null
allowlist enables no hosts.

Allowlist entries must exactly match the configured `listener.hostname`, including case. Fully qualified domain names
(FQDNs), such as `server-a.example.com`, work when that is the configured listener hostname. Short hostnames are not
automatically expanded to FQDNs; ports, URLs, wildcards, and aliases are not interpreted. The API parameter is a JSON
string array; the admin tool accepts comma-separated hostnames:

```bash
--update-cluster-config --url <child-controller-url> --cluster <cluster-name> \
  --server.stalled.drainer.diagnostics.enabled true \
  --server.stalled.drainer.diagnostics.host.allowlist server-a.example.com,server-b.example.com
```

The server captures `serverConfig.getListenerHostname()` once during ingestion-service construction. This is the
configured `listener.hostname` (default `Utils.getHostName()`), not a node ID with a port or necessarily the underlying
Kubernetes node hostname. Diagnostics add no DNS resolution or remote I/O; membership is a string-set lookup. Missing or
empty listener hostnames select no host. Changing the listener hostname requires a server restart; changing the live
allowlist does not.

Each supplied allowlist replaces the entire list; omitted parameters leave their existing values unchanged. Pass
`--server.stalled.drainer.diagnostics.host.allowlist ""` to clear the list (API: `[]`), remove a hostname by supplying
the remaining list, or set the master flag to `false` to disable tracking on all hosts. Target the child controller for
each region whose server cluster needs diagnostics; this command does not broadcast the change from the parent to all
regions. The existing stall monitor must also be enabled (`server.blocked.drainer.threshold.ms` must be positive).

Servers observe updates through the live cluster configuration watch. Each drainer samples the flag and hostname
allowlist when it starts a queue node. A tracked node keeps tracking and cleans up through completion even if the flag
is disabled or the hostname is removed; subsequent nodes use the updated settings. Enabling or adding a hostname does
not instrument a node already in progress or reconstruct breadcrumbs for an existing stall.

When enabled, a separate diagnostic ERROR accompanies the existing once-per-stall report. It includes the Java thread
name/ID/state, store-version and partition, current operation start/age, and the last completion outcome. `STORAGE_*`
phases include Java synchronization and preparation; `ROCKSDB_*` phases identify write/flush JNI boundaries, not native
mutex ownership. Java thread IDs are not native OS thread IDs.

Tracking allocates short-lived snapshots on the ingestion path. Keep it disabled outside selected diagnostic hosts to
avoid those allocations. Nodes started while disabled install no context, allocate no operation snapshots, and emit no
additional diagnostic ERROR. Untracked RocksDB writes and flushes bypass `trackWrite` and its diagnostic callbacks. The
per-thread enablement check, storage-dispatch wrappers, and one live-config read per node remain. Existing stall logs
and metrics are unchanged.

## JFR (Java Flight Recorder) Profile

Use when: latency spikes, CPU contention, thread contention, or when you need to understand where time is being spent.

```bash
jcmd <PID> JFR.start duration=60s filename=profile.jfr
```

JFR captures CPU profiling, thread contention, lock waits, GC (garbage collection) activity, and I/O with low overhead.
Especially useful for:

- **Latency spikes** — JFR can identify JIT deoptimization. Collecting a profile may itself resolve latency spikes by
  triggering JIT recompilation.
- **Thread contention** — shows which threads are competing for locks (e.g., SSL handshake thread pool saturation).
- **CPU hotspots** — identifies which methods are consuming the most CPU time.

Analyze JFR recordings with [JDK Mission Control](https://github.com/openjdk/jmc).

## Graceful Drop

For the current version and the selected backup when backup graceful drop is enabled, OFFLINE-to-DROPPED waits for the
ingestion service's stop result, removes Customized State, then waits the full configured grace interval before dropping
storage. Time spent OFFLINE does not shorten that interval, and a missing STANDBY-to-OFFLINE transition does not bypass
withdrawal. The stop wait uses the configured stop-consumption timeout. A fresh unsubscribe is an intentional retry: a
successful service acknowledgement can recover from an earlier failed OFFLINE/reset stop, while failure of this attempt
prevents deletion. The storage-drop path skips another routing-state deletion when this transition already withdrew
successfully; no lifecycle cache is required. Interruption during accessor initialization or a deletion retry also
aborts withdrawal, including when the configured grace interval is zero.

Stop/withdrawal failures, timeouts, and interrupted waits abort before storage deletion. Existing ingestion-service
semantics are unchanged: absent or non-running tasks are treated as already stopped, not as proof that task cleanup has
finished. Customized State removal acknowledges the local routing-state write, not convergence of every routing cache;
cached routes or queued reads outliving the grace interval can still produce missing-partition errors.
