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

This can be leveraged in order to achieve a TTL behavior. By scheduling periodic empty pushes and configuring how far 
back to replay the real-time data (N), one can control the TTL parameters. The time to live is defined by N, which acts 
as a "minimum TTL", while the "maximum TTL" is N + delay between each empty push. For example, if you schedule a daily 
empty push, and N = 6 days, then the oldest data in your store will be at least 6 days old, and at most 7 days old.

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

## Future Work
A more efficient implementation of TTL is currently under development, which will leverage [Repush](../ops_guide/repush.md)
in order to do an offline compaction and sorting of the data and thus minimize the impact on servers. This guide will be
updated when the functionality is ready to try.