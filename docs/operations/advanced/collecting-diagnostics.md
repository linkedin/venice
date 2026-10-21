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

The server records `.netty_worker_thread_pool--active_request_thread_count.Avg` and
`.netty_worker_thread_pool--active_request_thread_count.Max` whenever a complete HTTP request enters the request
pipeline. These metrics count workers concurrently executing the synchronous request pipeline across HTTP/1.1
connections and HTTP/2 streams. The equivalent OpenTelemetry distribution is
`thread_pool.thread.active_request_distribution`, with `venice.thread_pool.name=netty_worker_thread_pool`.

Use the request-sampled average and maximum to understand request-processing concurrency within the reporting window.
They are not time-weighted utilization or an in-flight request count: asynchronous storage work, response-only writes,
TLS handshakes, and socket I/O outside the request pipeline are excluded. Idle periods produce no samples, and even a
single request contributes a count of one.

### Server Netty Event Loops

The listener also monitors its resolved worker group, independently of HTTP traffic. All metrics use the
`netty_worker_thread_pool` pool name, with the same OpenTelemetry dimension as the request-thread metrics above.

| Signal                                 | Tehuti metric under `.netty_worker_thread_pool--`         | OpenTelemetry metric                                  |
| -------------------------------------- | --------------------------------------------------------- | ----------------------------------------------------- |
| Resolved worker capacity               | `max_thread_number.LambdaStat`                            | `thread_pool.thread.max_count`                        |
| Total queued tasks                     | `queued_task_count_gauge.LambdaStat`                      | `thread_pool.queue.task_count`                        |
| Busiest loop's queued tasks            | `max_queued_task_count_gauge.LambdaStat`                  | `thread_pool.queue.max_task_count`                    |
| Probe scheduling delay in milliseconds | `event_loop_scheduling_delay` (Avg, Max, and percentiles) | `thread_pool.event_loop.scheduling_delay` (histogram) |

Capacity comes from the actual NIO or Epoll worker group, including Netty's default sizing when the configured worker
count is zero. It is not the number of workers currently running or consuming CPU.

A separate daemon samples queue sizes once per second and submits at most one outstanding probe per worker. Probing all
workers can start threads that Netty would otherwise initialize lazily. Queue gauges report the latest sample, not a
reporting-window maximum; the busiest-loop gauge is the maximum across workers in that sample. The counts exclude
executing tasks, future scheduled tasks, and socket/kernel buffers, but may include one queued diagnostic probe per
worker. They are not counts of queued HTTP requests.

Scheduling delay measures submission-to-execution time, including time behind other tasks, I/O callbacks, and JVM
pauses. The histogram is updated only when a probe executes; a continuously blocked loop cannot report its completed
delay until it resumes. Other loops and backlog sampling continue without adding more probes to that loop. Monitoring
starts with the listener and stops before its worker group shuts down; it does not cover the boss or gRPC groups.

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
