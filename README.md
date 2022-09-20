Venice
======

Venice is a derived data storage platform, providing the following characteristics:

1. High throughput asynchronous ingestion from batch and streaming sources (e.g. [Hadoop](https://github.com/apache/hadoop) and [Samza](https://github.com/apache/samza)).
2. Low latency online reads via remote queries or in-process caching.
3. Active-active replication between regions with CRDT-based conflict resolution.
4. Multi-cluster support within each region with operator-driven cluster assignment.
5. Multi-tenancy, horizontal scalability and elasticity within each cluster.

The above makes Venice particularly suitable as the stateful component backing a Feature Store, such as [Feathr](https://github.com/linkedin/feathr). AI applications feed the output of their ML training jobs into Venice and then query the data for use during online inference workloads.

Write Path
----------

The Venice write path can be broken down into three granularities: full dataset swap, insertion of many rows into an existing dataset, and updates of some columns of some rows. All three granularities are supported by Hadoop and Samza, thus leading to the below full matrix of supported operations:

|                                                 | Hadoop                                   | Samza                             |
| ----------------------------------------------- | ---------------------------------------- | --------------------------------- |
| Full dataset swap                               | Full Push Job                            | Reprocessing Job                  |
| Insertion of some rows into an existing dataset | Incremental Push Job                     | Real-Time Job                     |
| Updates to some columns of some rows            | Incremental Push Job doing Write Compute | Real-Time Job doing Write Compute |

### Hybrid Stores
Moreover, the three granularities of write operations can all be mixed within a single dataset. A dataset which gets full dataset swaps in addition to row insertion or row updates is called _hybrid_.

As part of configuring a store to be _hybrid_, an important concept is the _rewind time_, which defines how far back should recent real-time writes be rewound and applied on top of the new generation of the dataset getting swapped in.

Leveraging this mechanism, it is possible to overlay the output of a stream processing job on top of that of a batch job. If using partial updates, then it is possible to have some of the columns be updated in real-time and some in batch, and these two sets of columns can either overlap or be disjoint, as desired.

### Write Compute
Write Compute includes two kinds of operations, which can be performed on the value associated with a given key:

- **Partial update**: set the content of a field within the value.
- **Collection merging**: add or remove entries in a set or map.  

Read Path
---------

Venice supports the following read APIs:

- **Single get**: get the value associated with a single key
- **Batch get**: get the values associated with a set of keys
- **Read compute**: project some fields and/or compute some function on the fields of values associated with a set of keys.

### Read Compute
When using the read compute DSL, the following functions are currently supported:

- **Dot product**: perform a dot product on the float vector stored in a given field, against another float vector provided as query param, and return the resulting scalar.
- **Cosine similarity**: perform a cosine similarity on the float vector stored in a given field, against another float vector provided as query param, and return the resulting scalar.
- **Hadamard product**: perform a Hadamard product on the float vector stored in a given field, against another float vector provided as query param, and return the resulting vector.
- **Collection count**: return the number of items in the collection stored in a given field.

### Client Modes

There are two main client modes for accessing Venice data:

- **Classical Venice**: perform remote queries against Venice's distributed backend service. In this mode, read compute queries are pushed down to the backend and only the computation results are returned to the client. 
- **Da Vinci**: eagerly load some or all partitions of the dataset and perform queries against the resulting local cache. Future updates to the data continue to be streamed in and applied to the local cache.

Previously Published Content
============================

The following blog posts have previously been published about Venice:

- 2015: [Prototyping Venice: Derived Data Platform](https://engineering.linkedin.com/distributed-systems/prototyping-venice-derived-data-platform)
- 2017: [Building Venice with Apache Helix](https://engineering.linkedin.com/blog/2017/02/building-venice-with-apache-helix)
- 2017: [Building Venice: A Production Software Case Study](https://engineering.linkedin.com/blog/2017/04/building-venice--a-production-software-case-study)
- 2018: [Venice Performance Optimization](https://engineering.linkedin.com/blog/2018/04/venice-performance-optimization)
- 2021: [Taming memory fragmentation in Venice with Jemalloc](https://engineering.linkedin.com/blog/2021/taming-memory-fragmentation-in-venice-with-jemalloc)
- 2022: [Supporting large fanout use cases at scale in Venice](https://engineering.linkedin.com/blog/2022/supporting-large-fanout-use-cases-at-scale-in-venice)

The following talks have been given about Venice:

- 2018: [Venice with Apache Kafka & Samza](https://www.youtube.com/watch?v=Usz8E4S-hZE)
- 2019: [People You May Know: Fast Recommendations over Massive Data](https://www.infoq.com/presentations/recommendation-massive-data/)
- 2019: [Enabling next generation models for PYMK Scale](https://www.youtube.com/watch?v=znd-Q6IvCqY)

Keep in mind that older content reflects an earlier phase of the project and may not be entirely correct anymore.

Community Resources
===================

Feel free to engage with the community using our:
- [Slack workspace](https://join.slack.com/t/venicedb/shared_invite/zt-1dz75yvlh-nXj9wIA7RVyQY7bMDUlZog)
- [LinkedIn group](https://www.linkedin.com/groups/14129519/)
- [Twitter handle](https://twitter.com/VeniceDataBase)
- GitHub [issues](https://github.com/linkedin/venice/issues) and [discussions](https://github.com/linkedin/venice/discussions)

[//]: # (TODO: Add link to setup and execution guide)