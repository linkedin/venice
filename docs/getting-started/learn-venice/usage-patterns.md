# Store Usage Patterns

Venice is an online **derived-data serving system**. It serves datasets that are computed elsewhere, with low-latency
reads and optional online updates. It is not:

- a **source of truth** (keep a replayable upstream copy of everything you write to Venice),
- a cache, a relational database, or a blob store, or
- a batch-processing engine (batch computation happens upstream; Venice serves the result).

## Write Mechanisms vs. Read Clients

Batch-only and hybrid are Venice store types. Streaming-only is a way to use a hybrid store without recurring batch
pushes. Choose how data enters the store separately from how applications read it.

| Dimension        | Options                                                                                                                                                         |
| :--------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Write mechanisms | Batch (full) push, incremental push, and streaming writes (see [Write APIs](../../user-guide/write-apis/index.md))                                              |
| Serving clients  | [Thin](../../user-guide/read-apis/thin-client.md), [Fast](../../user-guide/read-apis/fast-client.md), [Da Vinci](../../user-guide/read-apis/da-vinci-client.md) |
| Change streams   | [CDC](../../user-guide/read-apis/cdc.md) for hybrid nearline changes; use serving clients for batch-only snapshots                                              |

### Partial Updates

Partial updates are common when a producer owns only a few fields, such as a stream processor refreshing recent
engagement signals in a ranking record. On a hybrid store with partial updates enabled, send only those fields through
the [Online Producer](../../user-guide/write-apis/online-producer.md#api),
[`VeniceSystemProducer`](../../user-guide/write-apis/stream-processor.md#apache-samza-integration), or an
[incremental push](../../user-guide/write-apis/batch-push.md#incremental-push). This reduces the payload but typically
costs more to ingest than a full `PUT`: Venice reads the current value, merges the changes, and writes the result.
Prefer a full `PUT` when the producer already has the complete, authoritative value.

## Choosing a Usage Pattern

| Pattern            | Writes                                                                              | Freshness                      | Typical fit                                   | Operations guide                                                                   |
| :----------------- | :---------------------------------------------------------------------------------- | :----------------------------- | :-------------------------------------------- | :--------------------------------------------------------------------------------- |
| **Batch-only**     | Full pushes only (incremental push requires hybrid)                                 | As fresh as the last push      | Datasets recomputed on a schedule             | [Batch-only stores](../../operations/data-management/batch-only-stores.md)         |
| **Hybrid**         | Full pushes as a baseline, plus streaming writes and/or incremental pushes overlaid | Seconds to minutes for updates | Large baseline plus online updates            | [Hybrid stores](../../operations/data-management/hybrid-stores.md)                 |
| **Streaming-only** | Streaming writes only; no recurring batch baseline                                  | Seconds to minutes             | Event-derived data with a replayable upstream | [Streaming-only stores](../../operations/data-management/streaming-only-stores.md) |

Rules of thumb:

- Start with **batch-only** if the data can be recomputed on a schedule and staleness equal to the refresh interval is
  acceptable.
- Choose **hybrid** when you need both a complete, periodically rebuilt dataset and online updates. This is the most
  robust option for important data that must also be fresh.
- Choose **streaming-only** only when the data is naturally event-derived and an external, replayable upstream can
  rebuild it. It is common but discouraged for important durable data (see
  [Streaming-only stores](../../operations/data-management/streaming-only-stores.md)).

## Write Freshness Targets

Use these as planning targets for your deployment, not guarantees from Venice OSS:

- **Full pushes (batch-only and hybrid):** Budget **24 hours** from the start of the Venice Push Job (VPJ) to the new
  version serving reads. Allow for maintenance and, on hybrid stores, nearline catch-up. If your application needs a
  tighter target, validate it with your operators under expected load and maintenance conditions.
- **Nearline writes (hybrid):** Plan for **10 minutes at p99** from publishing a write to read visibility in the serving
  region, including cross-region replication when the write originates elsewhere. This measures freshness, not the time
  to acknowledge a write.

## Examples

These are illustrative; the right pattern depends on your recompute cost, freshness needs, and recovery options.

| Use case                      | Typical pattern                                          | Why                                                                                   |
| :---------------------------- | :------------------------------------------------------- | :------------------------------------------------------------------------------------ |
| Recommendation / feed ranking | Hybrid                                                   | Offline-trained scores rebuilt periodically, adjusted online with recent engagement   |
| Ads                           | Hybrid, or batch-only                                    | Periodic campaign or model outputs, with online budget or eligibility updates         |
| Trust and safety              | Hybrid                                                   | Baseline risk labels refreshed in batch, with low-latency overlays for new signals    |
| Search / jobs                 | Batch-only, or hybrid                                    | Index-adjacent features rebuilt on a schedule, with optional online freshness signals |
| Feature / embedding serving   | Batch-only for model outputs; hybrid for online features | Model artifacts change per training run; online features need quick updates           |
| Short-lived event counters    | Streaming-only (with a replayable upstream)              | Naturally event-derived and expiring, rebuilt by replaying the event stream           |

## How Batch and Real-Time Data Merge

Hybrid stores overlay real-time writes on a versioned batch baseline. See
[Merging Batch & Real-Time Data](merging-batch-and-rt-data.md) for the architectural patterns and
[Hybrid stores](../../operations/data-management/hybrid-stores.md) for rewind and retention guidance. For multi-region
stores, see [Active-active writes](../../operations/data-management/hybrid-stores.md#active-active-writes).
