# Write APIs

Venice supports multiple write patterns to accommodate different data freshness and scale requirements.

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
