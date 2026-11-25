# Streaming-Only Stores

A streaming-only store is populated and updated exclusively by streaming writes (for example, stream processors or the
online producer). It is a [hybrid store](hybrid-stores.md) initialized with an empty push, then kept current through its
RT topic rather than recurring full pushes from an upstream batch dataset. See
[Store Usage Patterns](../../getting-started/learn-venice/usage-patterns.md) for the comparison and
[Push types](repush.md#push-types) for definitions.

## When to Use It

Streaming-only is common, but **discouraged for important, durable data** unless an external, replayable upstream can
rebuild the dataset. It fits event-derived data that expires naturally or that can be recomputed by replaying an event
stream.

## Risks

- **Recovery depends on what Venice still holds.** RT topics retain only a bounded window (see
  [RT topic retention](hybrid-stores.md#rt-topic-retention)). Streaming writes are also persisted into version topics,
  so a Kafka-input [repush](repush.md) from a usable version can avoid a full upstream replay.
- **No batch baseline to fall back on.** An empty push does not copy data from the previous version. RT replay can
  recover only values that the retained writes reconstruct. For example, a partial update without its base value can
  leave untouched fields at their schema defaults. If the retained history is insufficient, rebuild from an upstream
  snapshot or replay the source stream, which can be slow and operationally expensive.
- **Eventual consistency.** Updates are applied asynchronously, so readers can briefly observe stale values (visibility
  lag). Separately, in active-active stores conflicts are resolved by write timestamps: a later-arriving update with an
  older timestamp can be ignored, so arrival order alone does not decide the applied value.

## Recommendations

- Keep a replayable upstream source of truth and know the end-to-end time to replay it.
- Prefer a **batch rebuild path** for data that must be recoverable. Periodically pushing a complete batch baseline
  makes the store a regular hybrid store and reduces reliance on replay for recovery.
- Use [TTL](ttl.md) for data that should expire, and bound the growth of collection fields.
- Size the [partition count](hybrid-stores.md#choosing-and-changing-the-partition-count) up front and forecast storage
  from write rate and value size, since the dataset is not periodically rebuilt.
- Keep the rewind time as short as possible while still meeting the recovery requirement (at most 23 days; see
  [Choosing a rewind time](hybrid-stores.md#choosing-a-rewind-time)).

## Checklist

- [ ] An external, replayable upstream exists, and a full replay has been planned and timed.
- [ ] A recurring batch/full-push recovery path exists, or the data loss risk has been explicitly accepted.
- [ ] Empty push is used only when starting fresh or when data loss is acceptable.
- [ ] Lag, bootstrap time, and replay volume are monitored.
