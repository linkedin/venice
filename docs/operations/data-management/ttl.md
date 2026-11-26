# Time to Live (TTL)

## Use Cases

Periodically removing stale data can reduce serving storage and help meet data lifecycle requirements. These push-based
mechanisms do not provide a strict per-record expiration deadline. Account for delayed or failed pushes and check
[RT topic retention](hybrid-stores.md#rt-topic-retention) separately.

## Usage

The two approaches differ in what they copy into the new version and how real-time (RT) data is replayed.

| Approach                            | Source data                      | RT replay                                                         |
| :---------------------------------- | :------------------------------- | :---------------------------------------------------------------- |
| [Empty push](#empty-push)           | No baseline                      | Starts at the effective rewind; only retained history is replayed |
| [Repush with TTL](#repush-with-ttl) | Existing version topic, filtered | Starts at the effective rewind; replayed records are not filtered |

## Repush with TTL

Repush with TTL filters source-version data by replication-metadata timestamps before writing a new version. It can run
on a schedule.

If neither override is set, VenicePushJob derives the cutoff as the job start time minus the store's hybrid rewind. Set
at most one of the following mutually exclusive configs to override that default:

| Config                       | Cutoff                                            |
| :--------------------------- | :------------------------------------------------ |
| `repush.ttl.seconds`         | Job start time minus this many seconds            |
| `repush.ttl.start.timestamp` | The specified timestamp in **epoch milliseconds** |

The TTL filter applies only to the repush **input**. RT replay can reintroduce expired data. Set the effective replay
start (`rewind.time.in.seconds.override`, or `rewind.epoch.time.in.seconds.override` with `REWIND_FROM_SOP` and its
buffer) to align with the cutoff while still covering writes made during the push. Replay seeks by pub-sub timestamp; it
does not filter individual records by timestamp. Even with aligned starts, prevent or separately purge replayed writes
with logical timestamps older than the cutoff. The TTL start timestamp is in **milliseconds**, while
`rewind.epoch.time.in.seconds.override` is in **seconds**; account for the epoch buffer when using them together. A
24-hour relative fallback applies to a hybrid repush only when neither rewind override is set.

Repush with TTL acts on replication-metadata timestamps and uses an exclusive cutoff: a timestamp equal to the cutoff is
retained; only an older timestamp expires. An expired non-collection value or field may be removed or reset to its
schema default. Collection fields are filtered at the entry level, so newer entries and deletion tombstones can remain
instead of resetting the whole list or map.

Before scheduling a TTL repush, verify that the source version contains non-empty replication metadata for the records
being filtered. The filter throws if that metadata is missing or empty; the zero-valued batch sentinel is still
replication metadata and does not exempt records that contain none. This preflight also applies to untimestamped batch
records in an active-active source.

Records carrying the batch sentinel timestamp are exempt from value-level filtering, but that exemption does not apply
to field-level metadata. Batch records pushed with a record-level timestamp (`timestamp.field`) can expire. With
field-level timestamps, an untouched non-collection field can still carry a zero/sentinel timestamp and be reset; newer
fields remain. Do not rely on TTL to retain or purge batch data; remove expired batch records upstream and refresh with
a full push.

A TTL repush can expose an older value if the filter keeps it but drops a newer write or delete for the same key, even
without RT replay. When current values and deletes must be preserved, rebuild from an authoritative upstream snapshot
that reflects both and [prevent stale writes from being replayed](hybrid-stores.md#backfills).

For a regular full push with records after a TTL repush, the push job requires `timestamp.field` or
`allow.regular.push.with.ttl.repush=true`. Use the Spark writer to persist `timestamp.field` as replication metadata and
verify it in the resulting version topic. The default MapReduce writer can pass the guard with `timestamp.field` set but
does not persist that field as replication metadata. The `allow.regular.push.with.ttl.repush` override skips the guard;
the controller clears the TTL-repush flag when it processes the resulting regular push ID.

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

An empty push starts with no batch data and reconstructs a new version by replaying from the
[rewind start positions](hybrid-stores.md#rewind). With periodic empty pushes, a relative rewind of N seconds is a rough
minimum age for expiry by replay, not a maximum TTL. For example, a six-day rewind and daily successful swaps with
negligible anchor-to-swap delay give roughly six to seven days before expiry for an unchanged record. This assumes the
relevant RT history is retained and pub-sub timestamps follow partition offset order. A delayed or failed swap can keep
data in the serving version for longer.

For an admin-tool empty push, N comes from `hybrid-rewind-seconds`. A VenicePushJob empty push can replace that store
setting with a shorter per-push rewind override, so use the effective rewind when calculating TTL. An absolute
`rewind.epoch.time.in.seconds.override` fixes one start time; it is not a rolling N-second window. Scheduled pushes that
use it must advance the epoch for each run. Keep the effective N to 23 days or less (see
[Choosing a rewind time](hybrid-stores.md#choosing-a-rewind-time)).

!!! warning

    An empty push discards the whole previous baseline. The new version contains only what its retained RT writes can
    reconstruct. Use it for TTL only when that loss is acceptable.

### How to enable

At the moment, there are two ways to perform empty pushes:

1. Via the `empty-push` command in the admin tool.
2. Via the Venice Push Job, executed from a Hadoop grid, but with an input directory that contains a file with no
   records in it.
