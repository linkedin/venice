# Server shutdown phase diagnostics

Set `server.shutdown.diagnostics.enabled=true` to enable shutdown-only timing logs (default: `false`). Only the
ingestion service's server-stop path activates the collector. Routine task closes, ingestion, heartbeats, and EOS calls
do not collect timings. Disable the config to roll back the logging.

Each activated SIT emits one final summary. Before the ingestion executor's existing forced interruption, the
coordinator also logs a best-effort snapshot without acquiring SIT or writer partition locks. At most five writer
partition closes taking at least one second are logged individually per SIT. There are no new metrics or timeout,
ordering, checkpoint, or recovery-selection changes.

## Reading the timings

- All intervals use monotonic nanoseconds, displayed as milliseconds.
- `CLOSE_REQUEST_TO_LOOP_EXIT` starts immediately before the service invokes the existing task `close()` and ends when
  the run loop reaches partition shutdown. It includes any delay acquiring the task's close monitor. If an exception
  bypasses partition shutdown, this interval instead ends on entry to internal cleanup and includes exception unwinding;
  existing error logs provide that context.
- `WRITER_CLOSE` covers collection/iteration and other gaps outside the nested writer operations.
- `WRITER_LOCK` measures waiting to acquire the writer's partition monitor for final EOS.
- `EOS_SUBMIT` covers work under that monitor: segment checks, constructing/submitting EOS, and segment bookkeeping. A
  visit does not imply a message was submitted: an absent, unstarted, or ended segment can be skipped, and existing
  send-error handling is unchanged.
- `EOS_FUTURE` measures only the subsequent wait on the returned produce future, outside the monitor.
- `BATCH_UNSUBSCRIBE` measures the batch consumer unsubscribe after the writer sweep.
- `CHECKPOINT_SWEEP` is wall time scheduling/waiting for partition checkpoints, including their drain and sync work.
  Parallel worker times are not summed. `INTERNAL_CLOSE` is the subsequent task cleanup.

Per-phase `totalMs`, `maxMs`, and `finishedVisits` include only visits that have ended. These phase intervals are
non-overlapping on a SIT; add the current `phaseElapsedMs` for an in-progress interval. Do not sum durations across SITs
as if they were sequential: SITs share the existing executor deadline and run concurrently.

`writerCloseInclusiveTotalMs` and `writerCloseInclusiveMaxMs` separately measure the caller's writer-close wrapper,
including existing exception logging when a close throws, and overlap the writer lock/submit/future phases. Do not add
them to phase totals. `writerClosesFinished` includes calls that threw; `writerClosesFailed` counts those escaping
exceptions, not errors handled internally by the writer. Writer-sweep and checkpoint finished/remaining counters use the
partition count at the close request, refreshed at loop exit. Finished checkpoints include failed attempts or disabled
checkpoint work, not necessarily successfully persisted offsets. Workers can outlive a checkpoint timeout, so counters
in a final summary are still best effort.

The current phase, partition (`-1` for task-wide work), elapsed time, and progress counters help distinguish a late loop
exit from sequential writer lock, EOS submission, future, unsubscribe, or checkpoint delays. They do not establish which
phase caused a previously observed shutdown delay. Snapshots can observe counters from adjacent transitions.
