# Batch-Only Stores

A batch-only store is written exclusively by pushes from an upstream batch job. It has no real-time buffer. See
[Store Usage Patterns](../../getting-started/learn-venice/usage-patterns.md) for when to choose this pattern and
[Push types](repush.md#push-types) for definitions of the terms used below.

## How It Works

1. A **full push** writes the entire dataset into a new, immutable store version.
2. Venice ingests the version in the background while reads continue to be served from the current version.
3. After the new version is validated and ready, reads are swapped to it. Previous versions are kept as backups until
   they are cleaned up.

## Recommendations

- Rebuild the dataset upstream and **full push on a schedule** matching your freshness needs. A periodic push also
  refreshes data, applies new store configs, and keeps the store healthy (see [Repush](repush.md)).
- A batch-only store is written by full pushes only. Incremental pushes write through the RT topic, so they require a
  [hybrid store](hybrid-stores.md) (enabling incremental push on a batch store implicitly converts it to hybrid with
  default settings). Even then, use them only for limited changes; they are not a substitute for rebuilding the source
  dataset.
- Validate before swapping reads (for example, record-count and schema checks in the push job) and keep at least one
  backup version available for rollback.
- Make sure old versions are cleaned up so retained backups do not accumulate and consume storage.
- Plan the [partition count](hybrid-stores.md#choosing-and-changing-the-partition-count) up front. It is much easier to
  change on a batch-only store (with a full push or repush) than after it has become hybrid.

## Converting to Hybrid

Directly converting a batch-only store to hybrid requires setting a rewind time (and a lag threshold) explicitly.
Enabling incremental push also converts it implicitly with default hybrid settings, if cluster prerequisites allow.
Either way it becomes a hybrid store; follow [Hybrid stores](hybrid-stores.md#converting-from-batch-only).

## Checklist

- [ ] Dataset is rebuilt from a replayable upstream on a schedule.
- [ ] Push validation and version-swap behavior are understood, and a backup version exists for rollback.
- [ ] Schema changes are backward compatible with data already in retained versions.
- [ ] Storage and ingestion capacity are forecast for two or more versions coexisting during a push.
- [ ] Push failures and stale-data (time since last successful push) are monitored.
