# Time-based ingestion checkpoints

Time-based checkpoints supplement the existing byte-size and control-message checkpoints. They are disabled by default.
When enabled, a checkpoint can be triggered by **either bytes or time**; enabling time checkpoints does not change the
byte thresholds, control-message boundaries, or graceful-shutdown behavior.

| Property                                | Default                | Meaning                                                                                                        |
| --------------------------------------- | ---------------------- | -------------------------------------------------------------------------------------------------------------- |
| `server.database.sync.interval.enabled` | `false`                | Enable time-based ingestion checkpoints.                                                                       |
| `server.database.sync.interval.ms`      | `7200000` (two hours)  | Recurrence of checkpoint attempts for each source-partition subscription. Must be positive.                    |
| `server.database.sync.spread.ms`        | `600000` (ten minutes) | Initial random staggering window. Must be nonnegative and smaller than the interval. Zero disables staggering. |

The feature requires `store.writer.buffer.after.leader.logic.enabled=true` (the existing default). Startup rejects
enabling time checkpoints with inline leader writes, because those writes would not be ordered with a queued checkpoint.

Each subscription's first deadline is one interval after subscription plus a random delay in `[0, spread)`. Subsequent
deadlines retain that phase and recur at the configured interval. Subscriptions established together therefore spread
their checkpoint starts across the configured window. This is a pacing target, not a hard completion guarantee or a
coordinated cluster-wide sweep. Resubscription starts a new schedule. A zero spread schedules the first attempt after
exactly one interval.

The existing consumer loop checks deadlines between polls, including empty polls, so a quiet partition below the byte
threshold can still checkpoint without receiving another record. There are no additional timer threads or checkpoint
workers. Disabled subscriptions allocate no deadline state and register no poll callback. Checkpoints run through the
partition's existing ordered drainer, and use the normal heartbeat measurement, offsets, checksums, and storage sync.
The timer never treats wall-clock time as a new heartbeat.

There is at most one outstanding time checkpoint per source-partition subscription. Slow checkpoints, producer work,
throttling, and long polls can delay attempts. Missed intervals are skipped rather than replayed in a catch-up burst.
Byte/control checkpoints do not postpone time deadlines. Unsubscription, replacement of partition state, errors, and
task shutdown invalidate deferred checkpoints; an already executing drainer operation finishes under the existing drain
fence.

With Global RT DIV enabled, followers snapshot consumer-owned VT DIV between polls and persist it after the preceding
records. RT leaders use the existing RT DIV production and produce-completion snapshot path, separately for each
subscribed source. Non-RT producing leaders defer until preceding producer work has completed before pairing the
consumer DIV with the processed local VT position. No normal-ingestion checkpoint drains or unsubscribes a consumer.

## Validation before enabling

Keep the feature off until staging validation has checked checkpoint duration, ingestion latency and throughput,
level-zero SST creation/compaction pressure, and recovery from quiet as well as busy partitions. No performance overhead
bound is implied by these defaults. Disable the feature and restart to roll back to the existing triggers.
