# System Stores

Venice has several "system stores". These are used to store data and metadata needed by the system itself, in opposition
to "user stores" which store the data users are interested in. For the most part, the existence of these system stores
should not matter, but it can be useful for operators to know about them.

This page explains the various types of system stores and their function.

## System Store Types

Below are a few distinct types of system stores:

| Type                   | Cardinality    | Content       |
| ---------------------- | -------------- | ------------- |
| Schema system store    | 1 / deployment | Metadata only |
| Global system store    | 1 / deployment | Data          |
| Cluster system store   | 1 / cluster    | Data          |
| Per-store system store | 1 / user store | Data          |

### Schema System Stores

These are stores created in just one of the clusters of a deployment. They contain no data at all (and therefore no
store-versions) and thus cannot be queried. They only serve to store schemas. These schemas are those which the system
needs for its own
[internal protocols](https://venicedb.org/javadoc/com/linkedin/venice/serialization/avro/AvroProtocolDefinition.html).

The schema system stores are used for the sake of protocol forward compatibility. When deploying Venice code, its jars
contain resource files for the current protocol versions as well as all previous protocol versions, but not future
protocol versions. Given that Venice is a distributed system, it is important to have the flexibility of deploying
different versions of the software across different nodes. When a node running a newer version of the software persists
data written with a newer protocol version, and that data is then read by another node running an older version of the
software which does not know about that newer protocol version, then it can query the schema of the appropriate system
store and use this to decode the newer payload, and translate them into the version they understand (a process known as
Schema Evolution).

### Global System Stores

These are stores created in just one of the clusters of a deployment, but in opposition to schema system stores, they do
contain data. Their global nature makes them convenient to store data about all clusters, but it also means that the
clusters are not isolated from one another as far as this kind of system store is concerned. Therefore, it is best for
this type of system store to serve only in "best effort" purposes, so that if for any reason they become unavailable,
critical operations can still continue. An example of this would be to store heuristics or other non-critical data.
These include:

- Push Job Details Store
- Batch Job Heartbeat Store

### Cluster System Stores

These are stores created once per cluster, and which do contain data. It is appropriate to use such stores for critical
operations, given that one cluster's availability issues would not bleed into other clusters. These include:

- Participant Store
- Cluster Da Vinci Push Status Store (opt-in migration target)

### Per-store System Stores

These are stores created once per user store, and which do contain data. These include:

- Da Vinci Push Status Store
- Meta Store

## System Store List

Below is a list of actual system stores, along with their type and function (excluding schema stores):

| Name                       | Type      | Function                                                                                                                                                                                                         |
| -------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Push Job Details Store     | Global    | Stores historical status and characteristics about all [Push Jobs](../../user-guide/write-apis/batch-push.md) that ever ran.                                                                                     |
| Batch Job Heartbeat Store  | Global    | Stores heatbeats emitted from running push jobs, in order to detect if any push job terminated abruptly without cleaning itself up.                                                                              |
| Participant Store          | Cluster   | Intended as a signaling mechanism for commands emitted by controllers, to be executed by servers, including kill job commands.                                                                                   |
| Da Vinci Push Status Store | Per-store | Stores per-instance consumption status of [Da Vinci Clients](../../user-guide/read-apis/da-vinci-client.md). Written to by DVC, and read by controllers, in order to determine whether push jobs have succeeded. |
| Meta Store                 | Per-store | Stores a copy of the store config (also found in Zookeeper). Written to by controllers, and read by DVC. Deprecated, will be removed eventually.                                                                 |

## Migrating Da Vinci Push Status to Cluster Stores

The migration target is `venice_system_store_cluster_davinci_push_status_store_<cluster>`, created independently
in each child region. It uses the existing push status key/value protocols, with the user store name appended
to every composite key, including heartbeats. Keys from different user stores cannot collide. Each cluster store
has 200 partitions and write computation enabled; legacy per-store push status stores retain one partition.

All migration settings default to `false`; existing stores and reporting flags remain unchanged.

| Setting | Where to set it | Effect |
| --- | --- | --- |
| `cluster.push.status.store.enabled` | Child controller cluster properties | Provision the cluster store, schemas, and initial version on cluster leadership initialization. |
| `cluster.push.status.store.dual.write.enabled` | Da Vinci backend, storage server, and child controller properties | Write heartbeats, batch/incremental statuses, ongoing incremental push updates, and status deletions to both stores. |
| `cluster.push.status.store.read.enabled` | Controller cluster and router properties | Combine cluster and legacy reports, with legacy authoritative for overlapping instances during dual writes. |

Roll out in this order:

1. Enable provisioning and verify the cluster store is online and ingesting its local real-time topic in every
   child region. The controller's maximum partition count must allow 200 partitions. Provisioning alone does
   not change reads or writes. Initialization rejects incompatible serving versions or real-time topics
   before changing metadata, including a serving store with write computation disabled. Ingestion tasks
   snapshot that setting at startup; changing store settings does not repair an existing task or version.
   Resolve such incompatible resources before enabling dual writes.
2. Before enabling dual writes, provision Kafka write access for participating Da Vinci and server identities and
   router read access for controllers and routers. Legacy per-store ACLs do not grant access to the new shared
   resource. Review the shared-topic authorization boundary: Kafka topic ACLs cannot isolate individual user-store
   keys. Do not enable this migration where per-store write isolation is required.
3. Enable dual writes for controllers, servers, and Da Vinci clients. Verify successful cluster-topic production
   and that heartbeats, bootstrap sentinels, batch reports, and incremental reports are readable. Keep legacy
   auto-materialization and reporting enabled during mixed-client rollout. Cluster update failures are logged
   without interrupting legacy reporting; dual-store deletion failures still propagate. Request-based Da Vinci
   discovery runs on a dedicated bounded worker, with one discovery attempt and a 30-second failure backoff.
   Cluster writes are skipped while routing is unresolved; legacy reporting does not wait for discovery.
   Cluster writer creation, updates, and tombstones share one lazy worker with a 1,024-operation FIFO queue.
   Saturation logs dropped cluster updates and fails deletion futures instead of blocking legacy reporters.
   Shutdown cancels pending cluster operations; writer-cache cleanup is deferred and logged if an in-flight
   operation has not exited, preventing late writer creation after cache cleanup.
   Storage servers close their status writer when Helix participation stops, after ingestion reporting stops.
4. Enable migrated reads. Reports from both stores remain visible, with instance names normalized consistently.
   Legacy remains authoritative for overlapping statuses and non-null heartbeats, including bootstrap sentinels:
   a failed best-effort cluster update must not hide a legacy terminal report or active heartbeat. Cluster-only
   reports are retained; neither status values nor heartbeat timestamps establish cross-store generation order.
   A non-null legacy heartbeat returns without requesting cluster data; only missing legacy timestamps use
   the cluster heartbeat fallback.
   Cluster discovery failures, worker saturation, or a one-second cluster-read timeout are logged and fall back
   to legacy data. Blocked cluster requests have no queued backlog. Legacy read failures still propagate:
   silently discarding legacy reports could incorrectly complete a push.
5. Verify push completion, error handling, incremental pushes, version deletion, user-store deletion, and store
   migration in a canary cluster before expanding rollout. Cluster writer caches are shared and are not closed
   when one user store's legacy writer is removed. Version cleanup deletes both version- and partition-level
   batch status keys and waits for their legacy and cluster tombstones within the controller cleanup timeout.
   Cleanup targets the cluster owning the removed version, subject to that cluster's dual-write setting, not
   the store's discovery destination. Successful and aborted migration cleanup delete only the removed
   cluster's records, preserving shared legacy reports; with cluster dual writes disabled, migration cleanup
   leaves both stores untouched.

Rollback by disabling migrated reads, then dual writes; legacy stores continue receiving reports throughout
this migration. Legacy records and skipped cluster updates are not backfilled, so cluster records do not prove
that every client has migrated. Store migration changes the reporting destination; readers and reporting
writers resolve the store's current cluster, while version cleanup uses its explicit owning cluster.

Physical legacy-store removal is deliberately not part of this rollout. Before implementing retirement, verify
that no deployed clients or servers depend on legacy stores, establish ordering or reconciliation for cluster-only
reads, stop legacy auto-materialization and health repair, and separate the reporting gate from the
legacy-store-enabled flag. Provide an independently gated, retryable deletion operation that closes legacy
writers, deletes versions and push-monitor state, truncates real-time
topics, and removes metadata without touching the cluster store. The existing
`--disable-davinci-push-status-store` option only clears the flag; it does not delete resources and must not be
used as a substitute for retirement.
