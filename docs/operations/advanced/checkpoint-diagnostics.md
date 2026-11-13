# Shutdown checkpoint diagnostics

`server.ingestion.checkpoint.diagnostics.enabled` defaults to `false`. It adds observability for investigating stale
persisted heartbeats and blob bootstrap after restart; it does not fix recovery, increase timeouts, retry checkpoints,
or change bootstrap thresholds.

## Enabling and deployment scope

The source reads this boolean once in the `VeniceServerConfig` constructor. `VeniceConfigLoader` merges cluster and
server properties and retains that config object. This is a server-process setting, not a dynamic toggle: restart the
affected server after changing it. A process started with the flag enabled captures its next shutdown and the following
startup (if the flag remains enabled). The first restart used to apply the flag does not enable diagnostics in the
outgoing, default-off process. Disabling also requires a restart.

There are no separate host, cluster, or fabric flags. Deployment tooling can supply the same process property to a
selected host, cluster, or fabric. Whether a host-specific override is supported, its precedence, and how it reaches the
process must be verified against the actual deployment configuration topology. This source change does not establish
deployment configuration paths or prove that any deployed override takes effect.

Start with one explicitly selected staging host if deployment topology permits; inspect log volume and correlate
shutdown and startup by host, version topic, partition, and time before considering a broader scope. This change enables
nothing in staging or production.

## Output and interpretation

When enabled:

- Each ingestion task emits one shutdown summary, including total, successfully completed command, skipped, and
  unfinished counts and elapsed milliseconds. Disabled graceful checkpointing is counted as skipped.
- Only unfinished partitions have detail records, in numbered batches of at most 20 entries. Entries contain only
  numeric values and fixed stage names; every unfinished entry is emitted without a truncation cap.
- The inventory is captured before visiting partitions. `NOT_STARTED` identifies untouched or not-yet-executing work;
  `DRAINING` is the last observed drain stage; `COMMAND_SUBMITTED` cannot distinguish queued from executing work;
  `COMPLETION_UNKNOWN` means the command future failed or was cancelled. No per-transition or per-partition success logs
  are added.
- `commandCompleted` means the checkpoint command future completed normally, not independent proof of a disk checkpoint.
  Existing paths can skip a checkpoint (for example, a closed engine or errored replica).
- `liveHeartbeat` is sampled from the existing heartbeat measurement at reporting time, not from disk. Its existing
  invalid timestamp sentinel still applies; a failed measurement is reported as `unavailable`.
- `observedPersistedHeartbeat/checkpoint` is available only after this task observed a successful metadata `put` during
  this shutdown. No extra storage read or steady-state tracking reconstructs earlier durable values. `unavailable` is
  not evidence of a persistence failure. A mutable `OffsetRecord` is never labeled persisted before `put` returns.
- A cancelled or interrupted wait does **not** prove persistence failed. An already-executing command may still
  complete. The report is a best-effort per-partition snapshot, not a globally atomic final inventory; work can progress
  after the snapshot. Late writes do not produce additional log events.
- For the hybrid time-lag bootstrap decision, a gated companion event contains the original saved heartbeat and
  checkpoint timestamp, computed age and threshold, offset lag, and decision. It reuses the existing metadata read
  before bootstrap replaces the partition. Existing decision logs remain unchanged when diagnostics are disabled.

Normal shutdown therefore adds only one event per task. Abnormal shutdown can emit many bounded batches, intentionally
without suppression so unvisited partitions are not lost. Use this flag only for scoped diagnostic sessions, not
fleet-wide routine logging. With the default off there is no inventory, heartbeat scan, command tracking, or added
metadata read.
