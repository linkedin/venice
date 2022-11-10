---
layout: default
title: Time to Live (TTL)
parent: User Guides
permalink: /docs/user_guide/ttl
---

# Time to Live (TTL)
For Venice stores that receive nearline writes, it is possible to set up a periodic purge of records that are older than
some threshold.

The way this works is by leveraging the real-time buffer replay mechanism of hybrid stores. As part of pushing a new 
store-version, after the push finishes, but before swapping the read traffic over to the new version, there is a buffer
replay phase where the last N seconds of buffered real-time data is replayed. The replayed data gets overlaid on top of
the data from the Full Push.

This can be leveraged in order to achieve a TTL behavior. Currently, there are two options that leverages this mechanism. 

### Empty push
By scheduling periodic empty pushes and configuring how far back to replay the real-time data (N), one can control the 
TTL parameters. The time to live is defined by N, which acts as a "minimum TTL", while the "maximum TTL" is N + delay 
between each empty push. For example, if you schedule a daily empty push, and N = 6 days, then the oldest data in your 
store will be at least 6 days old, and at most 7 days old.

### [Experimental] Repush with TTL
Similarly to empty push, the repush with TTL can also be configured and scheduled periodically to achieve the TTL behavior.
The major difference between this and empty push is empty push would create a fresh store and replay the real-time data (N),
whereas the repush firstly scans through entries in the current version, and uses store-level rewind time as TTL time to 
evict stale records based on the write timestamp, and finally replays the real-time data. This brings two major benefits:
1. The version topic enables log compaction, so the total amount of entries produced to the new store version will be reduced.
2. The data format produced by the repush with TTL will be more efficient since it will be sorted and de-duped.

Please note that this feature only supports Venice stores that enable active-active replication, empty push doesn't have
this requirement.

## Use Cases
TTL can serve as a storage efficiency optimization for use cases where the data is transient in nature, and not useful
beyond a certain age.

TTL can also be useful in certain scenarios where the data should not be retained longer than prescribed for compliance
reasons. Note that for these scenarios, it is important to understand (as explained above) that the rewind time is the 
minimum, not maximum, TTL. It is also advisable to schedule the periodic purging such that there is a margin of safety 
in case of infra delays or failures.

## Usage
At the moment, there are two ways to perform empty pushes:
1. Via the `empty-push` command in the admin tool.
2. Via the Venice Push Job, executed from a Hadoop grid, but with an input directory that contains a file with no 
   records in it.

In order to enable repush with ttl, refer to the [Repush](../ops_guide/repush.md) guide.