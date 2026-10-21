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

## Failed Read Execution

For reads that execute on a server worker and throw an exception during lookup or response construction, look for
`Storage read execution failed` in `StorageReadRequestHandler` logs. The warning records:

- `submittedAtEpochMs`: wall-clock time when the request or parallel batch was submitted to its executor.
- `queueWaitMs` and `executionMs`: monotonic durations before worker execution and from worker start to failure.
- `queueLengthAtSubmission`: queued tasks at submission, not the queue size at failure.
- `resource`, `requestType`, `retry`, `streaming`, and `requestKeyCount`: request context without keys or values.
- `chunkIndex`: zero-based parallel chunk index, or `-1` for sequential execution.
- `requestIdentity`: a best-effort, process-local correlation token also included in the HTTP request-error log. Combine
  it with resource, host, and timestamp; it is not a globally unique request ID.

Compare submission time and queue wait with the replica's OFFLINE transition and physical partition-removal logs. A
submission after removal demonstrates that this read was dispatched after removal; it was not already waiting in the
storage executor before removal. Submission is measured at the storage handler, not at network arrival, and does not
identify the caller's routing snapshot or establish whether a client retry succeeded.

Each failed parallel chunk emits its own warning; these warnings are not unique failed-request counts. Successful reads
and early rejections do not emit this diagnostic. Read-response exceptions, HTTP statuses, and client retry behavior are
unchanged.

### Graceful-Drop Ordering

For replicas covered by graceful drop (the current version, plus the selected backup when backup graceful drop is
enabled), the drain interval starts after unsubscribe completes and Customized State removal succeeds. Look for
`Removed customized state` and the subsequent graceful-drop log containing the routing-withdrawal timestamp. If the
participant did not observe STANDBY-to-OFFLINE, it stops ingestion and removes Customized State before waiting the full
interval. If reset already initiated unsubscribe, withdrawal awaits that same stop future, including returned failures
and synchronous backend exceptions, rather than treating a duplicate stop as completion. A new ingestion lifecycle uses
a fresh stop future.
Stops registered during startup wait for startup to exit before unsubscribing, including cleanup after partial startup
failure. If ingestion-task shutdown overtakes queued unsubscribe actions or stop is requested after shutdown begins,
the stop futures complete after shutdown cleanup or fail with the cleanup error.

OFFLINE remains asynchronous. DROPPED waits for pending routing withdrawal, bounded by the existing 60-minute partition
drop timeout. A failed unsubscribe, failed or timed-out withdrawal, or an interruption during withdrawal or draining
fails the transition before physical deletion. Customized State removal is still attempted if unsubscribe fails, but
normal graceful drop does not delete storage on that failure.

Customized State removal acknowledges the server's routing-state write, not propagation through every controller,
metadata server, or client cache. Routing convergence or queued reads exceeding the configured interval can still
produce post-drop failures. Use the failure timings above to investigate those remaining cases.
