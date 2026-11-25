# Streaming-Only Stores

A streaming-only store is populated and updated exclusively by streaming writes (for example, stream processors or the
online producer). It has no recurring batch baseline: the store is created and kept current from its RT topic. It is
technically a [hybrid store](hybrid-stores.md) that is initialized with an empty push and never full-pushed again. See
[Store Usage Patterns](../../getting-started/learn-venice/usage-patterns.md) for the comparison and
[Push types](repush.md#push-types) for definitions.

## When to Use It

Streaming-only is common, but **discouraged for important, durable data** unless an external, replayable upstream can
rebuild the dataset. It fits event-derived data that expires naturally or that can be recomputed by replaying an event
stream.

## Risks

- **Recovery depends on what Venice still holds.** RT topics retain only a bounded window (see
  [RT topic retention](hybrid-stores.md#rt-topic-retention)). Streaming writes are also persisted into version topics,
  so while a usable version topic survives, a Kafka-input [repush](repush.md) can rebuild the store without replaying
  your upstream. If every required version topic is unusable or expired, or the history you need is absent, the only
  recovery is a **full replay from your upstream**, which is slow and operationally expensive.
- **No batch baseline to fall back on.** An empty push creates a version containing only RT history within the rewind
  window (not all broker-retained RT data), so older records are lost.
- **Eventual consistency.** Updates are applied asynchronously and in the order of the RT stream; readers can briefly
  observe stale values.

## Recommendations

- Keep a replayable upstream source of truth and know the end-to-end time to replay it.
- Prefer a **recurring batch/full-push recovery path** (turning the store into a regular hybrid store) so that a
  complete baseline exists. Even an infrequent full push bounds recovery time.
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
