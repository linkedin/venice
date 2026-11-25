# Hybrid Stores

A hybrid store combines a **versioned batch baseline** with a **real-time (RT) overlay**. Full pushes create immutable
versions; streaming writes and incremental pushes flow through the store's RT topic and are applied on top. See
[Store Usage Patterns](../../getting-started/learn-venice/usage-patterns.md) for when to choose this pattern,
[Merging Batch & Real-Time Data](../../getting-started/learn-venice/merging-batch-and-rt-data.md) for the architecture,
and [Push types](repush.md#push-types) for definitions.

## Rewind

When a new version is pushed, Venice **replays** the retained RT history on top of it before swapping reads, so updates
made while the batch job was running are not lost. The **rewind time** (`hybrid-rewind-seconds`) controls how far back
the replay starts. It is a per-store setting.

| Buffer replay policy        | Replay starts at                                                                              |
| :-------------------------- | :-------------------------------------------------------------------------------------------- |
| `REWIND_FROM_EOP` (default) | Rewind time before the end of the batch push (End of Push) was received by the controller     |
| `REWIND_FROM_SOP`           | Rewind time before the start of the batch push (Start of Push) was received by the controller |

`REWIND_FROM_SOP` replays a window that also covers the whole duration of the push, which can help when pushes are long
relative to the rewind time. It replays more records than `REWIND_FROM_EOP` for the same rewind time.

Rewind time is also the **minimum** time an RT record is guaranteed to be overlaid on a new version; it is not a maximum
TTL (see [TTL](ttl.md)).

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

The rewind time is a per-store setting. There is no universal production default. In particular, 86,400 seconds (24
hours) is only an OSS **fallback** used when enabling incremental push on a batch-only store automatically converts it
to an active-active hybrid store with default settings. It should not be assumed for any store.

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

An empty push on a hybrid store creates a new version containing **only the replayed RT history** (within the rewind
window). Everything older is discarded. It is therefore a **data-loss-accepting** option, not a normal migration or
refresh path. For a deliberately RT-only dataset it is a valid way to start fresh; see
[Streaming-only stores](streaming-only-stores.md).

## Choosing and Changing the Partition Count

Size partitions up front: the partition count determines the parallelism of ingestion and reads and cannot be changed
casually on a hybrid store. Under-sizing limits throughput; over-sizing adds overhead per partition.

By default the controller rejects partition-count changes on a hybrid store. Because the RT topic has the same number of
partitions as the store, changing it requires retiring the old RT topic.

### Option A: In-place Change (requires pausing writers)

1. **Pause** RT and incremental-push writers.
2. **Disable hybrid** on the store.
3. Run enough full pushes or repushes, plus version cleanup, so that **all hybrid versions are retired in every
   region**.
4. **Wait for Venice's automatic RT topic cleanup.** Do not delete RT topics manually.
5. **Change the partition count.**
6. **Re-enable hybrid** (with the rewind time and lag threshold).
7. Run a **fresh full push**, and validate it.
8. **Resume** the writers.

An empty push can replace steps 3 and 7, but it discards all existing data and replays only retained RT history, so use
it only when data loss is acceptable.

### Option B: Migrate to a New Store (when pausing is unacceptable)

1. Create a new store with the desired partition count and the same schemas.
2. **Dual-write** from the upstream to both stores (both batch and RT, if applicable) and backfill the new store with a
   full push.
3. Migrate readers gradually (for example, behind a flag), comparing results between stores.
4. After readers are fully moved, stop writes to the old store and retire it.

## Checklist

- [ ] Rewind time is the shortest value that safely covers batch/catch-up overlap, at most 23 days.
- [ ] RT retention, storage, and bootstrap time are forecast from the rewind time.
- [ ] Full pushes run periodically (see [Repush](repush.md) and [TTL](ttl.md)).
- [ ] Source of truth is replayable upstream, and old versions are cleaned up.
- [ ] Schema changes are backward compatible; collection fields have bounded growth.
- [ ] Lag, bootstrap time, and replay volume are monitored.
