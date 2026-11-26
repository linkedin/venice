# Batch-Only Stores

A batch-only store is written exclusively by pushes from an upstream batch job. It has no real-time buffer. See
[Store Usage Patterns](../../getting-started/learn-venice/usage-patterns.md) for when to choose this pattern and
[Push types](repush.md#push-types) for definitions of the terms used below.

## How It Works

1. A **full push** writes the entire dataset into a new, immutable store version.
2. Venice ingests the version in the background while reads continue to be served from the current version.
3. After the new version is validated and ready, reads are swapped to it. Venice retires old backup versions
   automatically according to the version retention policy. By default, the policy retains two versions in steady state:
   the current version and one rollback backup.

## Recommendations

- Rebuild the dataset upstream and **full push on a schedule** matching your freshness needs. A periodic push also
  refreshes data, applies new store configs, and keeps the store healthy (see [Repush](repush.md)).
- A batch-only store is written by full pushes only. Incremental pushes write through the RT topic, so they require a
  [hybrid store](hybrid-stores.md) (enabling incremental push on a batch store implicitly converts it to hybrid with
  default settings). Even then, use them only for limited changes; they are not a substitute for rebuilding the source
  dataset.
- Validate before swapping reads (for example, record-count and schema checks in the push job).
- Start at **5-10 GB of data per Venice partition**; see
  [partition sizing](hybrid-stores.md#choosing-and-changing-the-partition-count). Repartitioning with a full push or
  repush is easier before the store becomes hybrid.

## Converting to Hybrid

Directly converting a batch-only store to hybrid requires setting a rewind time (and a lag threshold) explicitly.
Enabling incremental push also converts it implicitly with default hybrid settings, if cluster prerequisites allow.
Either way it becomes a hybrid store; follow [Hybrid stores](hybrid-stores.md#converting-from-batch-only).

## Checklist

- [ ] Dataset is rebuilt from a replayable upstream on a schedule.
- [ ] Push validation and version-swap behavior are understood, and a backup version exists for rollback.
- [ ] Schema changes are backward and forward compatible with clients and retained data.
- [ ] Capacity covers the current and incoming versions during a push. If capacity is limited to two versions, confirm
      the retention policy removes the previous backup before the new version needs that space.
- [ ] Push failures and stale-data (time since last successful push) are monitored.
