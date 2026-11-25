# Hybrid Stores

A hybrid store combines a **versioned batch baseline** with a **real-time (RT) overlay**. Full pushes create immutable
versions; streaming writes and incremental pushes flow through the store's RT topic and are applied on top. See
[Store Usage Patterns](../../getting-started/learn-venice/usage-patterns.md) for when to choose this pattern,
[Merging Batch & Real-Time Data](../../getting-started/learn-venice/merging-batch-and-rt-data.md) for the architecture,
and [Push types](repush.md#push-types) for definitions.

## Rewind

When a new version is pushed, Venice **replays** RT history on top of it before swapping reads, so updates made while
the batch job was running are overlaid on the new version. The **rewind time** (`hybrid-rewind-seconds`) controls how
far back the replay starts. It is a per-store setting that a push can override (relative with
`rewind.time.in.seconds.override`, or absolute with `rewind.epoch.time.in.seconds.override`).

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

Only writes inside the **effective replay window** are guaranteed to be overlaid. Writes older than the window are not
replayed even if they are still retained in the RT topic, so a short rewind (including a short per-push override such as
in a repush) can drop writes made early in a long push. Rewind time is therefore the **minimum** retained overlay, not a
maximum TTL (see [TTL](ttl.md)).

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
- RT topics are cleaned up automatically by Venice; do not delete them manually.

## Converting from Batch-Only

Converting a batch-only store to hybrid requires setting a rewind time and a lag threshold (offset or time) explicitly.
Run a full push after converting so a new version is created with the hybrid configuration.

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

### Option A: In-place Change (requires pausing writers)

Before starting, **record the store's hybrid settings**: rewind time, lag thresholds, replay policy, active-active
replication, and incremental push. Disabling hybrid also disables active-active replication and incremental push, so
they must be restored explicitly (or verified against your deployment defaults) when hybrid is re-enabled.

1. **Pause** RT and incremental-push writers.
2. **Wait for catch-up:** every active hybrid version in every region must consume and persist all RT writes up to the
   pause point.
3. **Disable hybrid** on the store.
4. Run enough full pushes or repushes, plus version cleanup, so that **all hybrid versions are retired in every
   region**. The baseline must include every write up to the pause: repush from a fully caught-up source version, or use
   a refreshed upstream batch baseline that contains those writes.
5. **Wait for Venice's automatic RT topic cleanup.** Do not delete RT topics manually.
6. **Change the partition count.**
7. **Re-enable hybrid** with the recorded settings.
8. Run a **fresh full push** with a baseline that again includes every write up to the pause, and validate it.
9. **Resume** the writers. If long-lived producers or stream-processing sinks cache the RT topic or partition count,
   restart or recreate them first so they pick up fresh metadata.

An empty push is **not** a shortcut for steps 4 or 8. Once hybrid is disabled and the old RT topic is cleaned up, there
is no RT history to replay, so the new version would be effectively empty.

### Option B: Migrate to a New Store (when pausing is unacceptable)

1. Create the new store with the desired partition count and the same schemas. Configure it as hybrid and initialize it
   with a version so it can accept RT writes.
2. Start **dual-writing** the upstream's RT/incremental writes to both stores.
3. Take a snapshot and **full push** it to the new store. The snapshot must cover writes up to the snapshot time, and
   the rewind window must cover the backfill duration plus margin so dual-written writes are overlaid.
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
