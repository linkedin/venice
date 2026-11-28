# Time-based ingestion checkpoints

Time-based checkpoints supplement the existing byte-size and control-message checkpoints. They are disabled by default.
When enabled, a checkpoint can be triggered by **either bytes or time**; enabling time checkpoints does not change the
byte thresholds or control-message boundaries. With time checkpoints enabled, the Global shutdown fallback checks the
existing producer-dispatch and leader-persistence futures before pairing the final upstream and local VT cursors.

| Property                                | Default             | Meaning                                                                                                           |
| --------------------------------------- | ------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `server.database.sync.interval.enabled` | `false`             | Enable time-based ingestion checkpoints.                                                                          |
| `server.database.sync.interval.seconds` | `7200` (two hours)  | Minimum time after a successful checkpoint before positive existing bytes become time-eligible. Must be positive. |
| `server.database.sync.spread.seconds`   | `600` (ten minutes) | Stable per-subscription delay in `[0, spread)`, added to the interval. Nonnegative and smaller than the interval. |

The feature requires `store.writer.buffer.after.leader.logic.enabled=true` (the existing default). Startup rejects
enabling time checkpoints with inline leader writes, because those writes would not be ordered with a queued checkpoint.

Each source-partition uses a stable delay in whole seconds in `[0, spread)`, derived from the source and replica
identity. The effective interval is the configured interval plus this delay. Its monotonic clock starts on the first
processed record. A successful checkpoint sets the next deadline to completion time plus the same effective interval,
including after a byte-triggered checkpoint or a long stall. Replacing partition state starts a new clock.

**Checks are record-driven, not timer-driven.** A new record goes through the existing ordered processing and checks the
existing byte threshold OR (elapsed time AND a positive existing byte counter). If input is completely idle, the
checkpoint waits until a new record arrives. Empty polls do nothing. No poll callbacks, timer threads, workers or
executors are added. Disabled partitions allocate no time-checkpoint state. Checkpoints use the existing offsets,
checksums and storage sync. No RocksDB or WAL semantics change. Only successful `storageEngine.sync` AND metadata
persistence refresh the clock; the wall-clock timestamp in the offset record is not the freshness clock.

**Time uses the existing byte accounting.** Without Global DIV, the processed-byte counter excludes control messages and
Global DIV records. With Global DIV, the consumed-byte counter includes their payload sizes wherever the existing poll
path counts them. VT bytes are keyed by version-topic name; RT bytes are keyed by broker URL. A heartbeat or other new
record can provide the next checkpoint opportunity when that counter is positive, including control-only traffic on
Global routes. Counters are not incremented earlier to make the current record eligible: producer predicates may see
bytes from previous records, just as on the byte path. No user-data flag, generation or separate pending state exists.

Global counters retain their legacy reset-on-dispatch behavior, before durability. They mean bytes since the last
dispatched checkpoint, **not** bytes pending until durable success. A later failed or skipped snapshot does not restore
those bytes. New bytes may trigger another due attempt while an earlier snapshot is outstanding; there is no in-flight
coalescing or reservation. Without Global DIV, processed bytes reset only after successful storage sync and metadata
persistence. Failures retain them. Slow checkpoints, producer work, throttling, and missing records can delay attempts.
Missed intervals are not replayed in a catch-up burst. Successful byte/control/time checkpoints refresh the
corresponding source deadline (partition-wide sync refreshes initialized source deadlines). Enqueue, producer
acknowledgement, cancellation, skipped commands and failures do not move deadlines, even when dispatch already reset
Global bytes. Unsubscription, replacement of partition state, errors and task shutdown invalidate deferred snapshot
commands. Reset/replacement and close-time state clearing serialize against executing snapshot commands and inline
drainer syncs, bounded by the configured drain timeout. Invalidation and partition identity checks prevent stale
snapshots from checkpointing replacement state. This is a synchronous execution fence, not an asynchronous admission or
lifetime barrier. Producer, view, buffer-drain and shutdown coordination use the existing byte-checkpoint mechanisms and
completion futures. They do not guarantee that every callback or partial view fanout has terminated; their existing
failure/timeout and cleanup limitations remain.

The four existing routes gain a time alternative to their normal predicates:

1. Without Global DIV, the drainer checks processed bytes OR time after processing each record.
2. A Global RT-consuming producing leader checks source bytes OR time and uses the existing RT DIV production and
   produce-completion snapshot path, separately for each source.
3. A Global non-RT producing leader (remote VT or stream reprocessing) checks the existing control/byte predicate OR
   time. The existing data-produce callback pairs the upstream-consumed and produced-local VT positions. It does not
   produce a synthetic RT DIV for a time checkpoint.
4. A Global local-VT follower or nonproducing leader checks the existing complete-DIV/control/twice-byte threshold OR
   time and enqueues its consumer snapshot after the preceding record's persistence dependency.

Enabling time checkpoints retains whole-poll validation, duplicate filtering, AA/WC parallel batching and asynchronous
view-to-VT dispatch, just like byte checkpoints. There is no per-record producer wait or validation lock. Time is
another trigger at the existing checkpoint locations, not a new synchronization model. Snapshot capture, producer
callbacks and drainer ordering reuse the byte-checkpoint paths; this does not add a record-aligned validator/snapshot
guarantee or fix existing snapshot-alignment risks. Producer dispatch and queued/executing snapshots retain
partition-state validation without reserving unfinished work. Snapshots use the existing typed drainer route, ordered by
the source topic (including stream-reprocessing topics). Once dispatched, snapshots use the existing persistence
dependencies and partition/storage validity guards, without a second time/pending-byte eligibility check.
Byte/control/shutdown checkpoints remain mandatory. No normal-ingestion checkpoint drains or unsubscribes a consumer.

## Validation before enabling

Keep the feature off until staging validation has checked checkpoint duration, ingestion latency and throughput,
level-zero SST creation/compaction pressure, and recovery from quiet as well as busy partitions. No performance overhead
bound is implied by these defaults. These are startup-loaded settings: disable the boolean through a configuration
rollout/restart to return to the existing triggers without reverting code. There is no runtime flag service.
