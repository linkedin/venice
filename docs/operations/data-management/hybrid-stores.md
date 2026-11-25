# Hybrid Stores

A hybrid store combines a **versioned batch baseline** with a **real-time (RT) overlay**. Full pushes create new
versions; streaming writes and incremental pushes continue through the store's RT topic and are applied as an overlay.
See [Store Usage Patterns](../../getting-started/learn-venice/usage-patterns.md) for when to choose this pattern,
[Merging Batch & Real-Time Data](../../getting-started/learn-venice/merging-batch-and-rt-data.md) for the architecture,
and [Push types](repush.md#push-types) for definitions.

## Rewind

When a new version is pushed, Venice **replays** RT history on top of it before swapping reads, so updates made while
the batch job was running are overlaid on the new version. The **rewind time** (`hybrid-rewind-seconds`) controls how
far back the replay starts. It is a per-store setting that a push can override (relative with
`rewind.time.in.seconds.override`, or absolute with `rewind.epoch.time.in.seconds.override`). If both are set, the
relative override wins. The epoch override requires `REWIND_FROM_SOP`. VenicePushJob calculates an equivalent relative
rewind for compatibility, but also forwards an absolute start of epoch minus the buffer; the buffer is 60 seconds by
default (`rewind.epoch.time.buffer.in.seconds.override`). In multi-region operation, the parent controller forwards the
absolute epoch only when the agreed admin-operation protocol is v106 or newer. Older controllers use the equivalent
relative rewind.

The following anchors apply to a **relative** rewind:

| Buffer replay policy        | Replay starts at                         |
| :-------------------------- | :--------------------------------------- |
| `REWIND_FROM_EOP` (default) | Rewind time before the end of the push   |
| `REWIND_FROM_SOP`           | Rewind time before the start of the push |

How the start and end of the push are determined depends on the replication mode:

- **Active-active:** each leader computes the replay start deterministically from the push's Start/End of Push control
  message producer timestamps, so all regions agree.
- **Otherwise:** the controller computes it. `REWIND_FROM_SOP` uses the version creation time, and `REWIND_FROM_EOP`
  uses the time of the topic switch.

`REWIND_FROM_SOP` also covers the duration of the push, which helps when pushes are long relative to the rewind time. It
replays more records than `REWIND_FROM_EOP` for the same rewind time.

Writes inside the **effective replay window** are eligible for replay; they are not guaranteed to replace the version's
value. Active-active conflict resolution can retain a newer logical timestamp instead. Writes older than the window are
not replayed even if the RT topic still retains them, so a short rewind (including a short per-push override such as in
a repush) can omit writes made early in a long push. Rewind time defines a replay-eligibility window, not a maximum TTL
(see [TTL](ttl.md)).

A per-push override does not extend RT retention, which is derived from the store-level config (see
[RT topic retention](#rt-topic-retention)). Before using an override longer than the store's rewind, check that the
requested history is still retained.

### Choosing a Rewind Time

Use the **shortest window that safely covers the overlap between your batch baseline and the real-time stream**: the
time between the snapshot your batch job reads and the version becoming current, plus margin for delayed or retried
pushes.

!!! warning "Recommendation: keep rewind to 23 days or less"

    Do not configure a rewind time above 23 days, for compliance and operational reasons. Venice OSS does **not**
    enforce this ceiling; a deployment's provisioning or control plane may. Do not rely on a longer rewind to recover
    old data.

A longer rewind costs:

- more **RT storage** in the pub-sub system,
- more **replayed records** per push,
- longer **bootstrap time** for new versions and replicas, and
- more transient **server and pub-sub capacity** during replay.

If you need more history than a short rewind provides, **rebuild the offline/batch baseline** instead of stretching the
rewind. Replaying history from upstream into the RT topic is slow and operationally expensive.

### Defaults

The rewind time is a per-store setting, and there is no universal production default. The value 86,400 seconds (24
hours) appears as two separate OSS fallbacks, neither of which should be assumed for any store:

- When enabling incremental push on a batch-only store converts it to a hybrid store with default settings. The
  conversion also enables active-active replication only if the cluster's default for hybrid stores enables it;
  incremental push requires active-active, and the controller (parent controller in multi-region deployments) can reject
  the request when it is not enabled.
- The push-job fallback for a repush whose source version is hybrid, used only when neither
  `rewind.time.in.seconds.override` nor `rewind.epoch.time.in.seconds.override` is set.

## RT Topic Retention

Venice sets the RT topic retention for an active RT topic, at creation and when reconciling store configs, to:

```
retention = max(5 days, rewind time + bootstrap-to-online timeout + 2 days)
```

- **5 days is a floor**, not a fixed value. Retention grows with rewind time and the store's bootstrap-to-online timeout
  (24 hours by default).
- The formula itself has no upper cap in Venice OSS. Your pub-sub broker or deployment may apply stricter limits.
- Retention is computed from the **store-level** rewind time, not from a per-push override. If a push overrides the
  rewind to a window longer than the store's retention covers (for example, 10 days on a store sized for 5), the
  requested history may already be gone. Size the store-level rewind for any longer replay you intend.
- Changing the bootstrap timeout changes the retention calculation, but does not by itself reconcile an existing RT
  topic. Apply a hybrid-config update as part of the change and wait for reconciliation. That update reconciles the
  primary RT topic; a separate RT topic used for incremental push may keep its previous retention until a later topic
  setup or switch checks it. Before relying on the larger window, verify and, if needed, reconcile broker retention for
  every relevant topic, including the primary and separate incremental-push RT topics.
- Automatic RT-topic cleanup is conditional: Venice must consider a topic truncated or otherwise eligible after
  retirement. A gated partition-count change does not, by itself, guarantee that the obsolete topic is cleaned while the
  store remains hybrid. Monitor obsolete topics through retirement; do not delete them manually.

## Converting from Batch-Only

Directly converting a batch-only store to hybrid requires setting a rewind time and a lag threshold (offset or time)
explicitly. Run a full push after converting so a new version is created with the hybrid configuration.

Enabling incremental push on a batch-only store is an implicit conversion: Venice synthesizes a default hybrid
configuration (default rewind and lag settings, `REWIND_FROM_EOP`) and enables active-active replication when the
cluster default allows it. If the prerequisites are not enabled, the controller (or parent controller) can reject the
request. Once incremental push is in use, you are operating a hybrid store, so review the defaults against
[Choosing a rewind time](#choosing-a-rewind-time).

## Empty Push on a Hybrid Store

An empty push on a hybrid store creates a new version containing **only the RT history replayed from the effective
rewind start**. Everything else, including the whole previous batch baseline, is discarded. The replay window is
determined by the rewind time, not by how much RT data the broker retains: with a one-day rewind, records that exist
only in days 2-5 of a five-day RT topic are not preserved. It is therefore a **data-loss-accepting** option, not a
normal migration or refresh path. For a deliberately RT-only dataset it is a valid way to start fresh; see
[Streaming-only stores](streaming-only-stores.md).

## Choosing and Changing the Partition Count

Size partitions up front: the partition count determines the parallelism of ingestion and reads and cannot be changed
casually on a hybrid store. Under-sizing limits throughput; over-sizing adds overhead per partition.

By default the controller rejects partition-count changes on a hybrid store. Because the RT topic has the same number of
partitions as the store, changing it requires retiring the old RT topic.

Operators can optionally enable the controller config `controller.enable.hybrid.store.partition.count.update=true`. With
it, a partition-count update is accepted directly and later versions use a new RT topic, while the current version's
partition count stays unchanged. This gate does not remove the need for a fresh full push and validation, and it carries
the same data-continuity risks; the default workflow below remains the primary path.

The gate changes the store's topic for later versions, but it does not itself guarantee a seamless writer or replay
handoff. During the transition, push monitoring can resolve replay through the current version's RT topic, and existing
writers can continue to use the old topic and partitioning. Use this path only after a coordinated pause/drain and a
complete refreshed baseline that includes every write through the pause. Verify that the new version resolves the
intended RT topic and partition count, completes replay, and swaps successfully before redirecting or resuming writers.
Refresh or restart producers that retain old topic metadata. If the writers cannot be paused and handed off safely, use
Option B instead. For active-active data, preserve conflict timestamps and delete tombstones in that baseline; if it is
value-only, fence stale pre-handoff writes before replay. RT topics are never deleted manually on either path.

### Option A: In-place Change (requires pausing writers)

Before starting, **record the store's hybrid settings**: rewind time, lag thresholds, replay policy, active-active
replication, incremental push, native-replication source fabric, and data-replication policy. Disabling hybrid also
disables active-active replication and incremental push, so restore them explicitly, using your intended values rather
than silently accepting new defaults. Restore the source fabric before the next repush.

1. **Pause** RT and incremental-push writers.
2. **Wait for catch-up:** every active hybrid version in every region must consume and persist all RT writes up to the
   pause point.
3. **Disable hybrid** on the store.
4. Run enough full pushes or repushes, plus version cleanup, so that **all hybrid versions are retired in every
   region**. The baseline must include every write up to the pause: repush from a fully caught-up source version, or use
   a refreshed upstream batch baseline that contains those writes. For active-active data, preserve conflict metadata
   and delete tombstones, or fence stale pre-pause writes if the replacement is value-only.
5. **Confirm cleanup of every obsolete RT topic**, including topics from earlier gated partition changes. If automatic
   cleanup does not complete, use the deployment's supported cleanup/escalation path or abort to Option B rather than
   waiting with writers paused indefinitely. Do not delete RT topics manually.
6. **Change the partition count.**
7. **Re-enable hybrid** with the recorded settings.
8. Run a **fresh full push** with a baseline that again includes every write up to the pause, and validate it. If the
   store previously used TTL repush, a regular push with records must supply `timestamp.field` or set
   `allow.regular.push.with.ttl.repush=true` (see [TTL](ttl.md#repush-with-ttl)).
9. **Resume** the writers. If long-lived producers or stream-processing sinks cache the RT topic or partition count,
   restart or recreate them first so they pick up fresh metadata.

An empty push is **not** a shortcut for steps 4 or 8. Once hybrid is disabled and the old RT topic is cleaned up, there
is no RT history to replay, so the new version would be effectively empty.

### Option B: Migrate to a New Store (when pausing is unacceptable)

1. Create the new store with the desired partition count and the same schemas. Copy the existing writer capabilities
   before the first initialization or dual write: hybrid config, incremental push, active-active replication, and
   partial-update (write-compute) config and schemas. A hybrid flag alone is not enough. Initialize it with a version so
   it can accept RT writes.
2. Start **dual-writing** the upstream's RT/incremental writes to both stores.
3. Take a snapshot and **full push** it to the new store. The snapshot must cover writes up to the snapshot time, and
   the rewind window must cover the backfill duration plus margin so dual-written writes are overlaid. For an
   active-active store, a value-only snapshot is not enough: preserve the conflict timestamps and delete tombstones
   needed by the target. If that metadata cannot be preserved, coordinate a snapshot/stream boundary that fences stale
   pre-snapshot writes before replay. Rewind overlap alone does not restore missing conflict metadata.
4. Validate, then migrate readers gradually (for example, behind a flag), comparing results between stores.
5. After readers are fully moved, stop writes to the old store and retire it.

## Checklist

- [ ] Settings (active-active, incremental push) are recorded before disabling hybrid.
- [ ] Rewind time is the shortest value that safely covers batch/catch-up overlap, at most 23 days.
- [ ] RT retention, storage, and bootstrap time are forecast from the rewind time.
- [ ] Full pushes run periodically (see [Repush](repush.md) and [TTL](ttl.md)).
- [ ] Source of truth is replayable upstream, and old versions are cleaned up.
- [ ] Schema changes are backward compatible; collection fields have bounded growth.
- [ ] Lag, bootstrap time, and replay volume are monitored.
