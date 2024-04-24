<html>
    <h1 align="center">
      Venice
    </h1>
    <h3 align="center">
      Derived Data Platform for Planet-Scale Workloads<br/>
    </h3>
    <div align="center">
        <a href="https://blog.venicedb.org/stable-releases">
          <img src="https://img.shields.io/docker/v/venicedb/venice-router?label=stable&color=green&logo=docker" alt="Stable Release">
        </a>
        <a href="https://github.com/linkedin/venice/actions?query=branch%3Amain">
          <img src="https://img.shields.io/github/actions/workflow/status/linkedin/venice/VeniceCI-StaticAnalysisAndUnitTests.yml" alt="CI">
        </a>
        <a href="https://venicedb.org/">
          <img src="https://img.shields.io/badge/docs-grey" alt="Docs">
        </a>
    </div>
    <div align="center">
        <a href="https://github.com/linkedin/venice">
          <img src="https://img.shields.io/badge/github-%23121011.svg?logo=github&logoColor=white" alt="GitHub">
        </a>
        <a href="https://www.linkedin.com/company/venicedb/">
          <img src="https://img.shields.io/badge/linkedin-%230077B5.svg?logo=linkedin&logoColor=white" alt="LinkedIn">
        </a>
        <a href="https://twitter.com/VeniceDataBase">
          <img src="https://img.shields.io/badge/Twitter-%231DA1F2.svg?logo=Twitter&logoColor=white" alt="Twitter">
        </a>
        <a href="http://slack.venicedb.org">
          <img src="https://img.shields.io/badge/Slack-4A154B?logo=slack&logoColor=white" alt="Slack">
        </a>
    </div>
</html>

Venice is a derived data storage platform, providing the following characteristics:

1. High throughput asynchronous ingestion from batch and streaming sources (e.g. [Hadoop](https://github.com/apache/hadoop) and [Samza](https://github.com/apache/samza)).
2. Low latency online reads via remote queries or in-process caching.
3. Active-active replication between regions with CRDT-based conflict resolution.
4. Multi-cluster support within each region with operator-driven cluster assignment.
5. Multi-tenancy, horizontal scalability and elasticity within each cluster.

The above makes Venice particularly suitable as the stateful component backing a Feature Store, such as [Feathr](https://github.com/feathr-ai/feathr). 
AI applications feed the output of their ML training jobs into Venice and then query the data for use during online 
inference workloads.

# Overview
Venice is a system which straddles the offline, nearline and online worlds, as illustrated below.

![High Level Architecture Diagram](assets/images/high_level_architecture.drawio.svg)

## Write Path

The Venice write path can be broken down into three granularities: full dataset swap, insertion of many rows into an 
existing dataset, and updates of some columns of some rows. All three granularities are supported by Hadoop and Samza.
In addition, any service can asynchronously produce single row inserts and updates as well, using the 
[Online Producer](./user_guide/write_api/online_producer.md) library. The table below summarizes the write operations 
supported by each platform:

|                                                  | [Hadoop](./user_guide/write_api/push_job.md) | [Samza](./user_guide/write_api/stream_processor.md) | [Any Service](./user_guide/write_api/online_producer.md) |
|-------------------------------------------------:|:--------------------------------------------:|:---------------------------------------------------:|:--------------------------------------------------------:|
|                                Full dataset swap |                      ✅                       |                          ✅                          |                                                          |
|  Insertion of some rows into an existing dataset |                      ✅                       |                          ✅                          |                            ✅                             |
|             Updates to some columns of some rows |                      ✅                       |                          ✅                          |                            ✅                             |

### Hybrid Stores
Moreover, the three granularities of write operations can all be mixed within a single dataset. A dataset which gets 
full dataset swaps in addition to row insertion or row updates is called _hybrid_.

As part of configuring a store to be _hybrid_, an important concept is the _rewind time_, which defines how far back 
should recent real-time writes be rewound and applied on top of the new generation of the dataset getting swapped in.

Leveraging this mechanism, it is possible to overlay the output of a stream processing job on top of that of a batch 
job. If using partial updates, then it is possible to have some of the columns be updated in real-time and some in 
batch, and these two sets of columns can either overlap or be disjoint, as desired.

### Write Compute
Write Compute includes two kinds of operations, which can be performed on the value associated with a given key:

- **Partial update**: set the content of a field within the value.
- **Collection merging**: add or remove entries in a set or map.  

N.B.: Currently, write compute is only supported in conjunction with active-passive replication. Support for 
active-active replication is under development. 

## Read Path

Venice supports the following read APIs:

- **Single get**: get the value associated with a single key
- **Batch get**: get the values associated with a set of keys
- **Read compute**: project some fields and/or compute some function on the fields of values associated with a set of 
  keys. When using the read compute DSL, the following functions are currently supported:
  - **Dot product**: perform a dot product on the float vector stored in a given field, against another float vector 
    provided as query param, and return the resulting scalar.
  - **Cosine similarity**: perform a cosine similarity on the float vector stored in a given field, against another 
    float vector provided as query param, and return the resulting scalar.
  - **Hadamard product**: perform a Hadamard product on the float vector stored in a given field, against another float 
    vector provided as query param, and return the resulting vector.
  - **Collection count**: return the number of items in the collection stored in a given field.

### Client Modes

There are two main modes for accessing Venice data:

- **Classical Venice** (stateless): You can perform remote queries against Venice's distributed backend service. If 
  using read compute operations in this mode, the queries are pushed down to the backend and only the computation
  results are returned to the client. There are two clients capable of such remote queries:
  - **Thin Client**: This is the simplest client, which sends requests to the router tier, which itself sends requests
    to the server tier.
  - **Fast Client**: This client is partitioning-aware, and can therefore send requests directly to the correct server
    instance, skipping the routing tier. Note that this client is still under development and may not be as stable nor
    at functional parity with the Thin Client.
- **Da Vinci** (stateful): Alternatively, you can eagerly load some or all partitions of the dataset and perform queries 
  against the resulting local cache. Future updates to the data continue to be streamed in and applied to the local 
  cache.

The table below summarizes the clients' characteristics:

|                                |  Network Hops  |  Typical latency (p99)  |          State Footprint          |
|-------------------------------:|:--------------:|:-----------------------:|:---------------------------------:|
|                    Thin Client |       2        |    < 10 milliseconds    |             Stateless             |
|                    Fast Client |       1        |    < 2 milliseconds     |  Minimal (routing metadata only)  |
|    Da Vinci Client (RAM + SSD) |       0        |     < 1 millisecond     | Bounded RAM, full dataset on SSD  |
|   Da Vinci Client (all-in-RAM) |       0        |    < 10 microseconds    |        Full dataset in RAM        |

All of these clients share the same read APIs described above. This enables users to make changes to their 
cost/performance tradeoff without needing to rewrite their applications.

# Resources

The Open Sourcing Venice [blog](https://engineering.linkedin.com/blog/2022/open-sourcing-venice--linkedin-s-derived-data-platform)
and [conference talk](https://www.youtube.com/watch?v=pJeg4V3JgYo) are good starting points to get an overview of what
use cases and scale can Venice support. For more Venice posts, talks and podcasts, see our [Learn More](./user_guide/learn_more.md)
page.

## Getting Started
Refer to the [Venice quickstart](./quickstart/quickstart.md) to create your own Venice cluster and play around with some 
features like creating a data store, batch push, incremental push, and single get. We recommend sticking to our latest 
[stable release](https://blog.venicedb.org/stable-releases).

## Community
Feel free to engage with the community using our:
- [Slack workspace](http://slack.venicedb.org)
  - Archived and publicly searchable on [Linen](http://linen.venicedb.org)
- [LinkedIn group](https://www.linkedin.com/groups/14129519/)
- [GitHub issues](https://github.com/linkedin/venice/issues)
- [Contributor's guide](./dev_guide/how_to/how_to.md)
- [Contributor's agreement](./dev_guide/how_to/CONTRIBUTING.md)

Follow us to hear more about the progress of the Venice project and community:
- [Official blog](https://blog.venicedb.org)
- [LinkedIn page](https://www.linkedin.com/company/venicedb)
- [Twitter handle](https://twitter.com/VeniceDataBase)
- [YouTube channel](https://youtube.com/@venicedb)
