# Time-based ingestion checkpoints

Time-based checkpoints supplement the existing byte-size and control-message checkpoints. They are disabled by default.
When enabled, a checkpoint can be triggered by **either bytes or time**; enabling time checkpoints does not change the
byte thresholds or control-message boundaries. Enabled shutdown checkpoints require successful producer and persistence
dependencies; an unconfirmed final cursor pair is not persisted.

| Property                                | Default             | Meaning                                                                                                           |
| --------------------------------------- | ------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `server.database.sync.interval.enabled` | `false`             | Enable time-based ingestion checkpoints.                                                                          |
| `server.database.sync.interval.seconds` | `7200` (two hours)  | Minimum time after a successful checkpoint before pending user data becomes time-eligible. Must be positive.      |
| `server.database.sync.spread.seconds`   | `600` (ten minutes) | Stable per-subscription delay in `[0, spread)`, added to the interval. Nonnegative and smaller than the interval. |

The feature requires `store.writer.buffer.after.leader.logic.enabled=true` (the existing default). Startup rejects
enabling time checkpoints with inline leader writes, because those writes would not be ordered with a queued checkpoint.

Each source-partition uses a stable delay in whole seconds in `[0, spread)`, derived from the source and replica
identity. The effective interval is the configured interval plus this delay. Its monotonic clock starts on the first
processed record. A successful checkpoint sets the next deadline to completion time plus the same effective interval,
including after a byte-triggered checkpoint or a long stall. Replacing partition state starts a new clock.

**Checks are record-driven, not timer-driven.** A new record goes through the existing ordered processing and checks the
existing byte threshold OR elapsed time since the last successful checkpoint. If input is completely idle, the
checkpoint waits until a new record arrives. Empty polls do nothing. No poll callbacks, timer threads, workers or
executors are added. Disabled partitions allocate no time-checkpoint state. Checkpoints use the existing offsets,
checksums and storage sync. No RocksDB or WAL semantics change. Only successful `storageEngine.sync` AND metadata
persistence refresh the clock; the wall-clock timestamp in the offset record is not the freshness clock.

**Only pending user data qualifies.** Heartbeats, control messages and Global DIV messages do not dirty a subscription.
A newly processed heartbeat can provide the next record opportunity for earlier pending user data, but heartbeat-only or
control-only traffic never qualifies for the time branch. Heartbeat-only stale metadata remains unresolved.

An outstanding source snapshot suppresses further time attempts until it completes. Slow checkpoints, producer work,
throttling, and missing records can delay attempts. Missed intervals are not replayed in a catch-up burst. Successful
byte/control checkpoints refresh freshness for the user-data generations they actually cover. A snapshot cannot
acknowledge newer arrivals or another RT source's progress. Enqueue, producer acknowledgement, cancellation, skipped
commands and failures never acknowledge progress. Unsubscription, replacement of partition state, errors and task
shutdown invalidate deferred commands. Teardown awaits executing as well as queued time checkpoints; reset/replacement
also serializes against executing snapshot commands and inline drainer syncs. Invalidation rejects new work immediately,
then waits up to the configured drain timeout. A timeout prevents state replacement or close-time state clearing;
completed failures remain reported but do not prevent cleanup.

The four existing routes gain a time alternative to their normal predicates:

1. Without Global DIV, the drainer checks processed bytes OR time after processing each record.
2. A Global RT-consuming producing leader checks source bytes OR time and uses the existing RT DIV production and
   produce-completion snapshot path, separately for each source.
3. A Global non-RT producing leader (remote VT or stream reprocessing) checks the existing control/byte predicate OR
   time. The existing data-produce callback pairs the upstream-consumed and produced-local VT positions. It does not
   produce a synthetic RT DIV for a time checkpoint.
4. A Global local-VT follower or nonproducing leader checks the existing complete-DIV/control/twice-byte threshold OR
   time and enqueues its consumer snapshot after the preceding record's persistence dependency.

Source-specific generations remain necessary because Global byte counters reset on send, before durable success. With
time checkpoints and Global DIV enabled, records are validated and processed one at a time per partition, including
completion of deferred view-to-VT producer calls before validating the next record. This keeps validator state and the
snapshot cursor at the same record boundary; whole-poll validation and multi-record parallel batches remain unchanged
when the feature is off. This enabled-only serialization can reduce throughput and must be measured before rollout.
Producer admission and queued/executing snapshots retain partition-state validation and a teardown completion barrier.
Snapshots use the existing typed drainer route, ordered by the source topic (including stream-reprocessing topics).
Time-only snapshots recheck captured-source coverage and freshness before persistence; byte/control/shutdown checkpoints
remain mandatory. No normal-ingestion checkpoint drains or unsubscribes a consumer.

## Validation before enabling

Keep the feature off until staging validation has checked checkpoint duration, ingestion latency and throughput,
level-zero SST creation/compaction pressure, and recovery from quiet as well as busy partitions. No performance overhead
bound is implied by these defaults. These are startup-loaded settings: disable the boolean through a configuration
rollout/restart to return to the existing triggers without reverting code. There is no runtime flag service.
