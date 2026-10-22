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

## Graceful Drop

For the current version and the selected backup when backup graceful drop is enabled, OFFLINE-to-DROPPED waits for
the ingestion service's stop result, removes Customized State, then waits the full configured grace interval before
dropping storage. Time spent OFFLINE does not shorten that interval, and a missing STANDBY-to-OFFLINE transition does
not bypass withdrawal. The stop wait uses the configured stop-consumption timeout.
A fresh unsubscribe is an intentional retry: a successful service acknowledgement can recover from an earlier failed
OFFLINE/reset stop, while failure of this attempt prevents deletion.
The storage-drop path skips another routing-state deletion when this transition already withdrew successfully; no
lifecycle cache is required. Interruption during accessor initialization or a deletion retry also aborts withdrawal,
including when the configured grace interval is zero.

Stop/withdrawal failures, timeouts, and interrupted waits abort before storage deletion. Existing ingestion-service
semantics are unchanged: absent or non-running tasks are treated as already stopped, not as proof that task cleanup
has finished. Customized State removal acknowledges the local routing-state write, not convergence of every routing
cache; cached routes or queued reads outliving the grace interval can still produce missing-partition errors.
