# Time-based ingestion checkpoints

Time-based checkpoints supplement the existing byte-size and control-message checkpoints. They are disabled by default.
When enabled, a checkpoint can be triggered by **either bytes or time**; enabling time checkpoints does not change the
byte thresholds, control-message boundaries, or graceful-shutdown behavior.

| Property                                | Default             | Meaning                                                                                                           |
| --------------------------------------- | ------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `server.database.sync.interval.enabled` | `false`             | Enable time-based ingestion checkpoints.                                                                          |
| `server.database.sync.interval.seconds` | `7200` (two hours)  | Minimum time after a successful checkpoint before pending user data becomes time-eligible. Must be positive.      |
| `server.database.sync.spread.seconds`   | `600` (ten minutes) | Stable per-subscription delay in `[0, spread)`, added to the interval. Nonnegative and smaller than the interval. |

The feature requires `store.writer.buffer.after.leader.logic.enabled=true` (the existing default). Startup rejects
enabling time checkpoints with inline leader writes, because those writes would not be ordered with a queued checkpoint.

Each subscription chooses one random delay in `[0, spread)`. The first deadline is subscription time plus the interval
and that delay. A successful checkpoint of covered user data sets the next deadline to completion time plus the interval
and the same delay. The delay is retained after long stalls and byte-triggered checkpoints, rather than resetting all
partitions to an unstaggered deadline. Resubscription starts a new schedule. This is a pacing target, not a hard
completion guarantee or a coordinated cluster-wide sweep.

The existing consumer loop checks deadlines between polls, including empty polls, so a quiet partition below the byte
threshold can still checkpoint without receiving another record. There are no additional timer threads or checkpoint
workers. Disabled subscriptions allocate no deadline state and register no poll callback. Checkpoints run through the
partition's existing ordered drainer and use the normal offsets, checksums, and storage sync. No RocksDB or WAL
semantics change. A checkpoint acknowledges progress only after `storageEngine.sync` and metadata persistence succeed.

**Only pending user data qualifies.** Heartbeats, control messages and Global DIV messages do not dirty a subscription.
An empty poll can checkpoint data left pending by an earlier poll. Heartbeat-only stale metadata remains unresolved
under this policy; the timer never invents a fresh heartbeat or flushes a clean partition merely because time passed.

There is at most one outstanding time checkpoint per source-partition subscription. Slow checkpoints, producer work,
throttling, and long polls can delay attempts. Missed intervals are skipped rather than replayed in a catch-up burst.
Successful byte/control checkpoints refresh freshness for the user-data generations they actually cover. A snapshot
cannot acknowledge newer arrivals or another RT source's progress. The consumer check is preliminary: ordered execution
rechecks eligibility so an intervening byte checkpoint suppresses a redundant time request. Enqueue, producer
acknowledgement, cancellation, skipped commands and failures never acknowledge progress. Unsubscription, replacement of
partition state, errors and task shutdown invalidate deferred commands. Teardown awaits executing as well as queued time
checkpoints; reset/replacement also serializes against executing snapshot commands.

With Global RT DIV enabled, followers snapshot consumer-owned VT DIV between polls and persist it after the preceding
records. RT leaders use the existing RT DIV production and produce-completion snapshot path, separately for each
subscribed source. Non-RT producing leaders (remote VT or stream reprocessing) defer until all preceding producer calls
and persistence callbacks finish before pairing the upstream-consumed cursor with the processed local VT position. They
do not emit an RT DIV. Atomic producer-call reservations cover overlapping AA source/key processing, not just the most
recently assigned future. Opted-in source admission is serialized per partition so poll snapshots do not race another
source's batch. No threads, workers or executors are added. No normal-ingestion checkpoint drains or unsubscribes a
consumer.

## Validation before enabling

Keep the feature off until staging validation has checked checkpoint duration, ingestion latency and throughput,
level-zero SST creation/compaction pressure, and recovery from quiet as well as busy partitions. No performance overhead
bound is implied by these defaults. These are startup-loaded settings: disable the boolean through a configuration
rollout/restart to return to the existing triggers without reverting code. There is no runtime flag service.
