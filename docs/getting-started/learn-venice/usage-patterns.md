# Store Usage Patterns

Venice is an online **derived-data serving system**. It serves datasets that are computed elsewhere, with low-latency
reads and optional online updates. It is not:

- a **source of truth** (keep a replayable upstream copy of everything you write to Venice),
- a cache, a relational database, or a blob store, or
- a batch-processing engine (batch computation happens upstream; Venice serves the result).

## Write Mechanisms vs. Read Clients

Venice has no "store types" in the sense of different products. A store is shaped by **how it is written**, and **how it
is read** is an independent choice.

| Dimension        | Options                                                                                                                                                         |
| :--------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Write mechanisms | Batch (full) push, incremental push, streaming writes, write compute (see [Write APIs](../../user-guide/write-apis/index.md))                                   |
| Read clients     | [Thin](../../user-guide/read-apis/thin-client.md), [Fast](../../user-guide/read-apis/fast-client.md), [Da Vinci](../../user-guide/read-apis/da-vinci-client.md) |

Any read client can read any usage pattern below.

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
[Hybrid stores](../../operations/data-management/hybrid-stores.md) for rewind and retention guidance.
