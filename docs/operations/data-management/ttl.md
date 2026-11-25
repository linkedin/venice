# Time to Live (TTL)

## Use Cases

TTL can serve as a storage efficiency optimization for use cases where the data is transient in nature, and not useful
beyond a certain age.

TTL can also be useful in certain scenarios where the data should not be retained longer than prescribed for compliance
reasons. Note that for an [empty push](#empty-push), the store rewind time is the minimum, not maximum, window of
history reconstructed (see [Hybrid stores](hybrid-stores.md#rewind)). For [repush with TTL](#repush-with-ttl), the
cutoff comes from `repush.ttl.seconds` or `repush.ttl.start.timestamp`, applied to the input records' replication
metadata, and the replay that follows can reintroduce older records unless its start is aligned. It is also advisable to
schedule the periodic purging such that there is a margin of safety in case of infra delays or failures.

## Usage

There are two ways to achieve TTL compliance, via [Repush with TTL](#repush-with-ttl) or via [empty push](#empty-push).
The major differences between both approaches is the original data source and how real-time buffer is replayed.

|                 | Source Data for TTL    | Real-time buffer replay                                               |
| --------------- | ---------------------- | --------------------------------------------------------------------- |
| Empty push      | None                   | Replay real-time buffer with `hybrid-rewind-seconds` config           |
| Repush with TTL | Existing version topic | Replay real-time buffer with `rewind.time.in.seconds.override` config |

## Repush with TTL

Repush with TTL runs as a VenicePushJob that reads the data in the existing version of the store, and writes it back as
a new version. The TTL is enforced by the VenicePushJob, which only writes records that are younger than the specified
TTL. Repush with TTL can also be configured and scheduled periodically to enforce TTL.

The store-level rewind time (in hybrid store configs) is used as the default TTL time. A custom TTL time can be
specified by setting one of the following configs on the VenicePushJob:

| Config                       | TTL behavior                                                         |
| ---------------------------- | -------------------------------------------------------------------- |
| `repush.ttl.seconds`         | Records older than the specified value will expire                   |
| `repush.ttl.start.timestamp` | Records that were written before the specified timestamp will expire |

The TTL filter applies only to the repush **input**. RT history is replayed afterward and can reintroduce records older
than the TTL cutoff. Set the effective replay start (`rewind.time.in.seconds.override`, or
`rewind.epoch.time.in.seconds.override`) to align with the cutoff while still covering writes made during the push. A
24-hour relative fallback applies to a hybrid repush only when neither override is set; do not treat it as a universal
value.

Repush with TTL requires replication metadata, so it expires only records written through active-active RT writes. Batch
records carrying the sentinel timestamp are retained regardless of age. It is not a generic purge of an old batch
baseline: remove expired batch records upstream and refresh with a full push.

This brings two major benefits:

1. The repush job de-dupes writes to the same key, so that the servers need to ingest fewer events.
2. The repush job sorts the data, thus allowing servers to ingest in a more optimized way.

### How to enable

In order to enable repush with ttl, a VenicePushJob needs to be configured with the following configs:

```
source.kafka = true
repush.ttl.enable = true
```

For more options related to repush, refer to the [Repush](repush.md) guide.

## Empty push

For Venice stores that receive nearline writes, it is possible to set up a periodic purge of records that are older than
some threshold.

The way this works is by leveraging the real-time buffer replay mechanism of hybrid stores. As part of pushing a new
store-version, after the push finishes, but before swapping the read traffic over to the new version, there is a buffer
replay phase where the last N seconds of buffered real-time data is replayed. The replayed data gets overlaid on top of
the data from the Full Push.

This can be leveraged in order to enforce TTL on the dataset. By scheduling periodic empty pushes and configuring how
far back to replay the real-time data (N), one can control the TTL parameters. The time to live is defined by N, which
acts as a "minimum TTL", while the "maximum TTL" is N + delay between each empty push. For example, if you schedule a
daily empty push, and N = 6 days, then the oldest data in your store will be at least 6 days old, and at most 7 days
old.

N can be set up by configuring the `hybrid-rewind-seconds` using venice-admin-tool. Keep N to 23 days or less (see
[Choosing a rewind time](hybrid-stores.md#choosing-a-rewind-time)).

!!! warning

    An empty push on a hybrid store discards the whole previous baseline; the new version contains only RT history from
    the effective rewind start. Use it for TTL only when that loss is acceptable.

### How to enable

At the moment, there are two ways to perform empty pushes:

1. Via the `empty-push` command in the admin tool.
2. Via the Venice Push Job, executed from a Hadoop grid, but with an input directory that contains a file with no
   records in it.
