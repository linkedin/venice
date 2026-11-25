# Time to Live (TTL)

## Use Cases

TTL can serve as a storage efficiency optimization for use cases where the data is transient in nature, and not useful
beyond a certain age.

TTL can also be useful in certain scenarios where the data should not be retained longer than prescribed for compliance
reasons. Note that for an [empty push](#empty-push), the effective rewind time is the minimum, not maximum, window of
history reconstructed; it can be shorter than the store setting when the push supplies an override (see
[Hybrid stores](hybrid-stores.md#rewind)). For [repush with TTL](#repush-with-ttl), the cutoff comes from the job start
time minus the store's hybrid rewind by default. `repush.ttl.seconds` or `repush.ttl.start.timestamp` can override it,
applied to the input records' replication metadata. The replay that follows can reintroduce older records unless its
start is aligned. It is also advisable to schedule the periodic purging such that there is a margin of safety in case of
infra delays or failures.

## Usage

There are two ways to achieve TTL compliance, via [Repush with TTL](#repush-with-ttl) or via [empty push](#empty-push).
The major differences between both approaches is the original data source and how real-time buffer is replayed.

|                 | Source data for TTL    | RT buffer replay                                                                     |
| --------------- | ---------------------- | ------------------------------------------------------------------------------------ |
| Empty push      | None                   | Replay from the effective rewind (store config or a per-push rewind override)        |
| Repush with TTL | Existing version topic | Replay from the effective relative or epoch override, subject to the push-job policy |

## Repush with TTL

Repush with TTL runs as a VenicePushJob that reads the data in the existing version of the store, and writes it back as
a new version. The TTL is enforced by the VenicePushJob, which only writes records that are younger than the specified
TTL. Repush with TTL can also be configured and scheduled periodically to enforce TTL.

If neither override is set, VenicePushJob derives the cutoff as the job start time minus the store's hybrid rewind. Set
at most one of the following mutually exclusive configs to override that default:

| Config                       | TTL behavior                                                    |
| ---------------------------- | --------------------------------------------------------------- |
| `repush.ttl.seconds`         | Records older than the specified value will expire              |
| `repush.ttl.start.timestamp` | Records before this **epoch-millisecond** timestamp will expire |

The TTL filter applies only to the repush **input**. RT history is replayed afterward and can reintroduce records older
than the TTL cutoff. Set the effective replay start (`rewind.time.in.seconds.override`, or
`rewind.epoch.time.in.seconds.override`, which requires `REWIND_FROM_SOP` and adds a buffer) to align with the cutoff
while still covering writes made during the push. Replay is positioned by pub-sub timestamp, but TTL filtering uses the
record's replication-metadata (logical) timestamp, so aligning windows does not guarantee that late-arriving writes with
expired timestamps are not replayed. Prevent such writes or purge them separately. The TTL start timestamp is in
**milliseconds**, while `rewind.epoch.time.in.seconds.override` is in **seconds**; convert between them and account for
the epoch buffer before using them together. A 24-hour relative fallback applies to a hybrid repush only when neither
override is set; do not treat it as a universal value.

Repush with TTL acts on replication-metadata timestamps and uses an exclusive cutoff: a timestamp equal to the cutoff is
retained; only an older timestamp expires. An expired non-collection value or field may be removed or reset to its
schema default. Collection fields are filtered at the entry level, so newer entries and deletion tombstones can remain
instead of resetting the whole list or map.

Records carrying the batch sentinel timestamp are exempt from value-level filtering, but that exemption does not apply
to field-level metadata. Batch records pushed with a record-level timestamp (`timestamp.field`) can expire. With
field-level timestamps, an untouched non-collection field can still carry a zero/sentinel timestamp and be reset; newer
fields remain. Do not rely on TTL to retain or purge batch data; remove expired batch records upstream and refresh with
a full push.

After a TTL repush, a regular (non-empty) full push is rejected unless the input provides `timestamp.field` record
timestamps or `allow.regular.push.with.ttl.repush=true` is set. The override skips the push job's rejection check; it
does not itself clear store metadata. The controller clears the TTL-repush flag when it processes the recognized push ID
produced for a regular push with that override.

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

For an admin-tool empty push, N comes from `hybrid-rewind-seconds`. A VenicePushJob empty push can replace that store
setting with a shorter per-push rewind override, so use the effective rewind when calculating TTL. Keep the effective N
to 23 days or less (see [Choosing a rewind time](hybrid-stores.md#choosing-a-rewind-time)).

!!! warning

    An empty push on a hybrid store discards the whole previous baseline; the new version contains only RT history from
    the effective rewind start. Use it for TTL only when that loss is acceptable.

### How to enable

At the moment, there are two ways to perform empty pushes:

1. Via the `empty-push` command in the admin tool.
2. Via the Venice Push Job, executed from a Hadoop grid, but with an input directory that contains a file with no
   records in it.
