# Write APIs

Venice supports multiple write mechanisms to accommodate different data freshness and scale requirements. These are
independent of how a store is read, and a store can combine them; see
[Store Usage Patterns](../../getting-started/learn-venice/usage-patterns.md) for how to choose between batch-only,
hybrid, and streaming-only usage.

## Batch Push

[Batch Push](batch-push.md) creates new store versions from Hadoop/Spark jobs. Venice atomically swaps versions for
zero-downtime updates. Best for:

- Full dataset replacement
- ML feature stores
- Data from batch pipelines

## Stream Processor

[Stream Processor](stream-processor.md) writes real-time updates via Apache Samza. Requires hybrid stores. Best for:

- Near real-time freshness
- Event-driven updates
- Streaming pipelines

## Online Producer

[Online Producer](online-producer.md) enables direct writes from online services with durability guarantees. Requires
hybrid stores with active-active replication. Best for:

- Low-latency writes from services
- User-generated content
- Real-time counters and aggregates
