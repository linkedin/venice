---
layout: default
title: Repush
parent: Operator Guides
permalink: /docs/ops_guide/repush
---

# Repush

In Venice, there are many things which happen under the hood as part of Full Pushes. Besides serving to refresh data,
Full Pushes also serve to make certain configs take effect, and also more generally keep the system tidy and healthy.

Many Venice users choose to periodically do Full Pushes for the sake of refreshing their data, in which case the
auxiliary goals of Full Pushes end up being fulfilled organically.

In other cases, users do Full Pushes either infrequently, or not at all. In these cases, the operator may want to 
manually trigger a Repush, without involving the user.

## Use Cases
The various motivations for operator-driven Repushes are described in more details below.

### Reconfiguration
In Venice, some store configs can be changed live, while some others take effect when the next store-version is pushed.
Configs which can be changed with a Repush (or a regular Full Push) include:

1. Partition-related settings
   1. Partition count
   2. Amplification factor
   3. Custom partitioner implementation
2. Replication settings
3. Whether a store is hybrid

Having some configs be immutable within the scope of a store-version makes the development and maintenance of the system
easier to reason about. In the past, the Venice team has leveraged Repushes to execute large scale migrations as new
modes were introduced (e.g., migrating from active-passive to active-active replication, or migrating from the 
offline/bootstrap/online state model to the leader/follower state model). Repush enabled operators to roll out (and 
occasionally roll back) these migrations in a way that was invisible to users of the platform. Although these migrations 
are behind us, it is possible that future migrations may also be designed to be executed this way.

### Sorting
When doing a Full Push (and also when Repushing), the data gets sorted inside the Map Reduce job, prior to pushing. The 
Start of Push control message is annotated with the fact that the incoming data is sorted, which triggers an alternative 
code path within servers (and Da Vinci clients) in order to generate the storage files more efficiently.

When doing a Stream Reprocessing (SR), on the other hand, data does not get sorted. More generally, if the last Full 
Push happened a while ago, and lots of nearline writes happened since then, there could be a large portion of the data 
which is unsorted, and the operator may find that rebalance performance degrades as a result.

While it is not considered necessary to perform a Repush after every SR just for the sake of sorting, it can sometimes 
be useful if the last SR or Full Push happened quite a while ago.

### Repair
In some cases where a datacenter (DC) suffered an outage, it is possible that pushes succeeded in some DCs and failed in
others. Alternatively, it's possible that correlated hardware failures resulted in some DC losing all replicas of some 
partitions. In these cases, as long as one of the DCs is healthy, it can be used as the source of a repush to repair the
data in other DCs.

### [Experimental] Time to Live (TTL)
It's usually required to evict stale entries from the store in order to achieve GDPR compliance or other business requirements.
The repush with TTL functionality will replay and scan through the entries in the current version, 
and use store-level rewind time as TTL time to evict stale records based on the write timestamp. See [TTL](../user_guide/ttl)
for the comparison among various TTL options.

## Usage
The following Venice Push Job config is used to enable Repush functionality:
```
source.kafka = true
```

### Optional Configs
The following Venice Push Job configs provide more control over the Repush functionality:

To specify which Kafka topic (i.e. which store-version) to use as the source of the Repush. If unspecified, it will
default to the highest available store-version.
```
kafka.input.topic = store-name_v7
```

To specify which datacenter to pull from. If unspecified, it will default to one of the DCs which contains the highest
available store-version.
```
kafka.input.fabric = dc-name
```

To specify the address of the Kafka cluster to pull from. If unspecified, it will use the one returned by the controller
of the datacenter chosen to pull from.
```
kafka.input.broker.url = kafka-url:port
```

To specify the size of the offset range each mapper task will be responsible for. Each Kafka partition is fetched in
parallel by multiple mappers, each taking care of distinct ranges. If unspecified, the default is 5M.
```
kafka.input.max.records.per.mapper = 1000000
```

To specify whether to enable the Combiner of the Map Reduce job, in order to prune the shuffling traffic between mappers
and reducers. This is an experimental functionality that has not been vetted for production usage yet. If unspecified,
it defaults to false.
```
kafka.input.combiner.enabled = true
```

To specify whether to enable TTL for the repush job. This is an experimental functionality.
If unspecified, it defaults to false.
```
repush.ttl.enable = true
```

## Future Work

One potential enhancement would be to add support for changing the compression setting of a store, which currently
requires a Full Push, and which Repush does not support yet.