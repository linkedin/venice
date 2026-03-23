# Venice Feature Flag Catalog

## 1. Introduction & How to Use This Catalog

Venice has **618 configuration keys** across routers, servers, controllers, push jobs, and clients (as of the
ConfigKeys.java enumeration). This catalog organizes them into dependency clusters and cross-layer prerequisites, making
it easier to reason about coverage gaps and understand how flags interact.

**How to use this catalog:**

- To understand a flag: find it by name or browse its component section
- To understand dependencies: check Section 4 (Dependency Graphs) and Section 5 (Store Profile Prerequisites)
- To understand mutual exclusions: check Section 6
- To plan test coverage: use the 42-Dimension Feature Matrix in Section 9

**Feature Maturity Levels:**

- **MATURE** — Always-on in production; tested at fixed production value only
- **MATURING** — Active development; tested in both on/off states
- **EXPERIMENTAL** — New features; tested in both on/off states

**Source files:**

| File                                                                                                         | Count | Category      |
| ------------------------------------------------------------------------------------------------------------ | ----- | ------------- |
| `internal/venice-common/src/main/java/com/linkedin/venice/ConfigKeys.java`                                   | ~618  | Master list   |
| `internal/venice-common/src/main/java/com/linkedin/venice/meta/Store.java`                                   | ~40   | Store-level   |
| `internal/venice-common/src/main/java/com/linkedin/venice/meta/Version.java`                                 | ~25   | Version-level |
| `services/venice-router/src/main/java/com/linkedin/venice/router/VeniceRouterConfig.java`                    | ~60   | Router        |
| `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceControllerClusterConfig.java` | ~80   | Controller    |
| `clients/da-vinci-client/src/main/java/com/linkedin/davinci/config/VeniceServerConfig.java`                  | ~100  | Server        |
| `clients/venice-client/src/main/java/com/linkedin/venice/fastclient/ClientConfig.java`                       | ~30   | Fast Client   |
| `clients/venice-thin-client/src/main/java/com/linkedin/venice/client/store/ClientConfig.java`                | ~25   | Thin Client   |
| `clients/da-vinci-client/src/main/java/com/linkedin/davinci/client/DaVinciConfig.java`                       | ~10   | DaVinci       |
| `clients/venice-push-job/src/main/java/com/linkedin/venice/hadoop/PushJobSetting.java`                       | ~50   | Push Job      |

---

## 2. Environment/Platform Configs

### 2.1 Store Profile Prerequisites

Controller flags that gate store-level features. These must be enabled at the cluster/environment level before the
corresponding store feature can be activated.

#### `ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller
- **Path Impact**: Write
- **Maturity**: MATURE — always-on in production
- **Dependencies**: Native replication infrastructure
- **Dependents**: Store-level AA replication
- **Description**: When enabled, new hybrid stores default to active-active replication mode.

#### `ENABLE_PARTIAL_UPDATE_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller
- **Path Impact**: Write
- **Dependencies**: AA replication enabled
- **Dependents**: Auto-enables write compute for hybrid+AA stores
- **Description**: Auto-enables partial update (write compute) for hybrid active-active user stores. See
  ParentControllerConfigUpdateUtils:40-92.

#### `ENABLE_INCREMENTAL_PUSH_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller
- **Path Impact**: Write
- **Dependencies**: AA replication enabled
- **Dependents**: Auto-enables incremental push for hybrid+AA stores
- **Description**: Auto-enables incremental push for hybrid active-active user stores.

#### `ENABLE_SEPARATE_REAL_TIME_TOPIC_FOR_STORE_WITH_INCREMENTAL_PUSH`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller, Server
- **Path Impact**: Write
- **Dependencies**: Incremental push enabled
- **Dependents**: Separate RT topic creation
- **Description**: Creates a separate real-time topic for stores with incremental push to isolate incremental data.

#### `CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller
- **Path Impact**: Write
- **Dependencies**: None
- **Dependents**: Store-level deferred version swap
- **Description**: Enables the deferred version swap service in the controller, allowing push jobs to defer version
  activation.

#### `PUSH_STATUS_STORE_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller, DaVinci
- **Path Impact**: Both
- **Dependencies**: None
- **Dependents**: DaVinci push status system store
- **Description**: Enables push status system stores for tracking DaVinci client bootstrap status.

#### `CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller
- **Path Impact**: Both
- **Dependencies**: None
- **Dependents**: Meta system store for client metadata
- **Description**: Auto-materializes meta system stores for new stores, enabling client-cached metadata.

#### `CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller, DaVinci
- **Path Impact**: Both
- **Dependencies**: PUSH_STATUS_STORE_ENABLED
- **Dependents**: DaVinci push status tracking
- **Description**: Auto-materializes DaVinci push status system stores for new stores.

### 2.2 Replication & Multi-Region Infrastructure

#### `NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES`

- **Source**: ConfigKeys.java
- **Type**: string (fabric name)
- **Default**: empty
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller, Server
- **Path Impact**: Write
- **Description**: Default source fabric for native replication of batch-only stores.

#### `NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES`

- **Source**: ConfigKeys.java
- **Type**: string (fabric name)
- **Default**: empty
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller, Server
- **Path Impact**: Write
- **Description**: Default source fabric for native replication of hybrid stores.

#### `NATIVE_REPLICATION_FABRIC_ALLOWLIST`

- **Source**: ConfigKeys.java
- **Type**: string (comma-separated)
- **Default**: empty
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller
- **Path Impact**: Write
- **Description**: Allowlist of fabrics eligible for native replication.

#### `ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST`

- **Source**: ConfigKeys.java
- **Type**: string (comma-separated)
- **Default**: empty
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller, Server
- **Path Impact**: Write
- **Description**: List of fabrics that serve as real-time sources in active-active replication.

#### `AGGREGATE_REAL_TIME_SOURCE_REGION`

- **Source**: ConfigKeys.java
- **Type**: string
- **Default**: empty
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller, Server
- **Path Impact**: Write
- **Description**: Region that aggregates real-time data in multi-region setups.

#### `ADMIN_TOPIC_SOURCE_REGION`

- **Source**: ConfigKeys.java
- **Type**: string
- **Default**: empty
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller
- **Path Impact**: Write
- **Description**: Source region for the admin topic in multi-region setups.

### 2.3 Router: Read Path Controls

#### `ROUTER_ENABLE_READ_THROTTLING`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: true
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Router
- **Path Impact**: Read
- **Maturity**: MATURE — default=true, always-on
- **Description**: Enables read request throttling on the router.

#### `ROUTER_EARLY_THROTTLE_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Router
- **Path Impact**: Read
- **Dependencies**: ROUTER_ENABLE_READ_THROTTLING
- **Description**: Enables early throttle checks before scatter-gather.

#### `ROUTER_SMART_LONG_TAIL_RETRY_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Router
- **Path Impact**: Read
- **Maturity**: MATURE — always-on in production
- **Description**: Enables smart long-tail retry that aborts retries when the router is overloaded.

#### `ROUTER_SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS`

- **Source**: ConfigKeys.java
- **Type**: numeric (ms)
- **Default**: varies
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Router
- **Path Impact**: Read
- **Dependencies**: ROUTER_SMART_LONG_TAIL_RETRY_ENABLED
- **Description**: Threshold for aborting smart long-tail retries.

#### `ROUTER_CLIENT_DECOMPRESSION_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: true
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Router, Client
- **Path Impact**: Read
- **Maturity**: MATURE — default=true, always-on
- **Description**: When enabled, router instructs clients to perform decompression instead of the router.

#### `ROUTER_HTTP2_INBOUND_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Router
- **Path Impact**: Read
- **Description**: Enables HTTP/2 for inbound connections to the router.

#### `ROUTER_LATENCY_BASED_ROUTING_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Router
- **Path Impact**: Read
- **Description**: Enables latency-based routing for selecting storage nodes.

#### `ROUTER_HELIX_ASSISTED_ROUTING_GROUP_SELECTION_STRATEGY`

- **Source**: ConfigKeys.java
- **Type**: string (enum)
- **Default**: LEAST_LOADED
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Router
- **Path Impact**: Read
- **Description**: Strategy for selecting routing groups in Helix-assisted routing.

#### `ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Router
- **Path Impact**: Read
- **Description**: Enables connection warming to pre-establish connections to storage nodes.

### 2.4 Server: Ingestion & Storage

#### `SERVER_ENABLE_PARALLEL_BATCH_GET`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Server
- **Path Impact**: Read
- **Description**: Enables parallel processing of batch get requests on the server.

#### `SERVER_COMPUTE_FAST_AVRO_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Server
- **Path Impact**: Read
- **Maturity**: MATURE — always-on in production
- **Description**: Enables fast Avro serialization/deserialization for compute operations.

#### `ENABLE_BLOB_TRANSFER`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Server
- **Path Impact**: Write
- **Description**: Enables blob transfer for bootstrapping partitions via direct peer-to-peer transfer instead of Kafka.

#### `SERVER_QUOTA_ENFORCEMENT_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Server
- **Path Impact**: Read
- **Description**: Enables per-store read quota enforcement on the server.

### 2.5 Server: AA/WC Workload Optimization

#### `SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Server
- **Path Impact**: Write
- **Dependencies**: Store must have AA+WC enabled
- **Description**: Enables parallel processing of active-active write compute workloads during ingestion.

#### `SERVER_ADAPTIVE_THROTTLER_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Server
- **Path Impact**: Both
- **Description**: Enables adaptive throttling that adjusts ingestion speed based on read latency signals.

### 2.6 Server: Kafka Fetch & Consumer

#### `KAFKA_FETCH_QUOTA_BYTES_PER_SECOND`

- **Source**: ConfigKeys.java
- **Type**: numeric
- **Default**: -1 (unlimited)
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Server
- **Path Impact**: Write
- **Description**: Kafka fetch quota in bytes per second for ingestion consumers.

#### `KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND`

- **Source**: ConfigKeys.java
- **Type**: numeric
- **Default**: -1 (unlimited)
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Server
- **Path Impact**: Write
- **Description**: Kafka fetch quota in records per second for ingestion consumers.

#### `KAFKA_FETCH_QUOTA_TIME_WINDOW_MS`

- **Source**: ConfigKeys.java
- **Type**: numeric (ms)
- **Default**: 1000
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Server
- **Path Impact**: Write
- **Description**: Time window for Kafka fetch quota measurement.

### 2.7 Controller: Cluster & Store Lifecycle

#### `CONTROLLER_SCHEMA_VALIDATION_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller
- **Path Impact**: Write
- **Maturity**: MATURE — always-on in production
- **Description**: Enables schema validation checks on the controller.

#### `CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS`

- **Source**: ConfigKeys.java
- **Type**: numeric (ms)
- **Default**: varies
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller
- **Path Impact**: Both
- **Maturity**: MATURE — always-on in production
- **Description**: Default retention period for backup versions before cleanup.

#### `CONTROLLER_PARENT_EXTERNAL_SUPERSET_SCHEMA_GENERATION_ENABLED`

- **Source**: ConfigKeys.java
- **Type**: boolean
- **Default**: false
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Controller
- **Path Impact**: Write
- **Description**: Enables superset schema generation via external callback in parent controller.

### 2.8 Controller: System Stores & Schema

See Section 2.1 for `CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE` and
`CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE`.

### 2.9 PubSub Infrastructure

#### `PUBSUB_BROKER_ADDRESS`

- **Source**: ConfigKeys.java
- **Type**: string
- **Default**: none (required)
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: All
- **Path Impact**: Both
- **Description**: Address of the PubSub broker (Kafka).

#### `PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS`

- **Source**: ConfigKeys.java
- **Type**: string (class name)
- **Default**: ApacheKafkaProducerAdapterFactory
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Server, Controller
- **Path Impact**: Write
- **Description**: Factory class for creating PubSub producer adapters.

#### `PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS`

- **Source**: ConfigKeys.java
- **Type**: string (class name)
- **Default**: ApacheKafkaConsumerAdapterFactory
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Server, Controller
- **Path Impact**: Write
- **Description**: Factory class for creating PubSub consumer adapters.

### 2.10 Independent Environment Knobs

#### `PERSISTENCE_TYPE`

- **Source**: ConfigKeys.java
- **Type**: enum (ROCKS_DB, IN_MEMORY, BLACK_HOLE)
- **Default**: ROCKS_DB
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: Server
- **Path Impact**: Both

#### `CLUSTER_NAME`

- **Source**: ConfigKeys.java
- **Type**: string
- **Default**: none (required)
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: All

#### `ZOOKEEPER_ADDRESS`

- **Source**: ConfigKeys.java
- **Type**: string
- **Default**: none (required)
- **Scope**: Cluster
- **Set By**: Operator
- **Affects**: All

---

## 3. Client-Side / Per-Store Configs

### 3.1 Store-Level Flags (Store.java)

#### `nativeReplicationEnabled`

- **Source**: Store.java
- **Type**: boolean
- **Default**: false
- **Scope**: Store
- **Set By**: Operator / Controller (auto)
- **Affects**: Server, Controller
- **Path Impact**: Write
- **Description**: Enables native replication where data is pulled from source fabric Kafka.

#### `activeActiveReplicationEnabled`

- **Source**: Store.java
- **Type**: boolean
- **Default**: false
- **Scope**: Store
- **Set By**: Operator / Controller (auto)
- **Affects**: Server, Controller
- **Path Impact**: Write
- **Dependencies**: nativeReplicationEnabled
- **Description**: Enables active-active replication with conflict resolution.

#### `writeComputationEnabled`

- **Source**: Store.java
- **Type**: boolean
- **Default**: false
- **Scope**: Store
- **Set By**: Operator / Controller (auto)
- **Affects**: Server, Client
- **Path Impact**: Write
- **Dependencies**: activeActiveReplicationEnabled (for hybrid stores)
- **Description**: Enables write compute (partial update) for the store.

#### `readComputationEnabled`

- **Source**: Store.java
- **Type**: boolean
- **Default**: false
- **Scope**: Store
- **Set By**: Developer
- **Affects**: Server, Router, Client
- **Path Impact**: Read
- **Description**: Enables read compute operations (project, dotProduct, etc.) on the store.

#### `chunkingEnabled`

- **Source**: Store.java
- **Type**: boolean
- **Default**: false
- **Scope**: Store
- **Set By**: Operator / Controller (auto when WC)
- **Affects**: Server, Client
- **Path Impact**: Both
- **Description**: Enables chunking for large values. Auto-enabled when write compute is on.

#### `rmdChunkingEnabled`

- **Source**: Store.java
- **Type**: boolean
- **Default**: false
- **Scope**: Store
- **Set By**: Operator / Controller (auto when WC+AA)
- **Affects**: Server
- **Path Impact**: Write
- **Dependencies**: chunkingEnabled AND activeActiveReplicationEnabled
- **Description**: Enables chunking for replication metadata. Auto-enabled when WC+AA.

#### `incrementalPushEnabled`

- **Source**: Store.java
- **Type**: boolean
- **Default**: false
- **Scope**: Store
- **Set By**: Operator / Controller (auto)
- **Affects**: Server, PushJob
- **Path Impact**: Write
- **Dependencies**: activeActiveReplicationEnabled
- **Description**: Enables incremental push for the store.

#### `hybridStoreConfig`

- **Source**: Store.java
- **Type**: HybridStoreConfig (object)
- **Default**: null (batch-only)
- **Scope**: Store
- **Set By**: Developer / Operator
- **Affects**: Server, Controller
- **Path Impact**: Write
- **Description**: Hybrid store configuration with rewind seconds, offset lag threshold, etc.

#### `compressionStrategy`

- **Source**: Store.java
- **Type**: enum (NO_OP, GZIP, ZSTD_WITH_DICT)
- **Default**: NO_OP
- **Scope**: Store
- **Set By**: Developer
- **Affects**: Server, Router, Client, PushJob
- **Path Impact**: Both
- **Description**: Compression strategy for store data.

#### `clientDecompressionEnabled`

- **Source**: Store.java
- **Type**: boolean
- **Default**: true
- **Scope**: Store
- **Set By**: Operator
- **Affects**: Router, Client
- **Path Impact**: Read
- **Description**: When true, clients decompress data; when false, router decompresses.

#### `blobTransferEnabled`

- **Source**: Store.java
- **Type**: boolean
- **Default**: false
- **Scope**: Store
- **Set By**: Operator
- **Affects**: Server
- **Path Impact**: Write
- **Dependencies**: Server-level ENABLE_BLOB_TRANSFER
- **Description**: Enables blob transfer for this specific store.

#### `separateRealTimeTopicEnabled`

- **Source**: Store.java
- **Type**: boolean
- **Default**: false
- **Scope**: Store
- **Set By**: Controller (auto) / Operator
- **Affects**: Server, Controller
- **Path Impact**: Write
- **Dependencies**: incrementalPushEnabled
- **Description**: Uses a separate real-time topic for incremental push data.

#### `storageQuotaInByte`

- **Source**: Store.java
- **Type**: long
- **Default**: unlimited
- **Scope**: Store
- **Set By**: Developer
- **Affects**: Controller, Server
- **Path Impact**: Write

#### `readQuotaInCU`

- **Source**: Store.java
- **Type**: long
- **Default**: varies
- **Scope**: Store
- **Set By**: Developer
- **Affects**: Router, Server
- **Path Impact**: Read

#### `backupStrategy`

- **Source**: Store.java
- **Type**: enum (KEEP_MIN_VERSIONS, DELETE_ON_NEW_PUSH_START)
- **Default**: KEEP_MIN_VERSIONS
- **Scope**: Store
- **Set By**: Developer
- **Affects**: Controller

#### `numVersionsToPreserve`

- **Source**: Store.java
- **Type**: int
- **Default**: 0
- **Scope**: Store
- **Set By**: Developer
- **Affects**: Controller

#### `accessControlled`

- **Source**: Store.java
- **Type**: boolean
- **Default**: true
- **Scope**: Store
- **Set By**: Developer
- **Affects**: Router, Controller

### 3.2 Version-Level Flags (Version.java)

#### `compressionStrategy` (version)

- **Source**: Version.java
- **Type**: enum
- **Default**: inherited from store
- **Scope**: Version
- **Description**: Compression strategy for this specific version.

#### `chunkingEnabled` (version)

- **Source**: Version.java
- **Type**: boolean
- **Default**: inherited from store
- **Scope**: Version

#### `rmdChunkingEnabled` (version)

- **Source**: Version.java
- **Type**: boolean
- **Default**: inherited from store
- **Scope**: Version

#### `blobTransferEnabled` (version)

- **Source**: Version.java
- **Type**: boolean
- **Default**: inherited from store
- **Scope**: Version

#### `pushType`

- **Source**: Version.java
- **Type**: enum (BATCH, INCREMENTAL, STREAM, STREAM_REPROCESSING)
- **Default**: BATCH
- **Scope**: Version

#### `nativeReplicationSourceFabric`

- **Source**: Version.java
- **Type**: string
- **Default**: inherited from cluster default
- **Scope**: Version

### 3.3 Thin Client Configs (ClientConfig.java)

#### `veniceURL`

- **Source**: clients/venice-thin-client ClientConfig.java
- **Type**: string
- **Default**: none (required)
- **Scope**: Client-instance

#### `enableLongTailRetryForSingleGet`

- **Source**: ClientConfig.java
- **Type**: boolean
- **Default**: false
- **Scope**: Client-instance

#### `enableLongTailRetryForBatchGet`

- **Source**: ClientConfig.java
- **Type**: boolean
- **Default**: false
- **Scope**: Client-instance

### 3.4 Fast Client Configs

#### `routingStrategy`

- **Source**: clients/venice-client ClientConfig.java
- **Type**: enum (LEAST_LOADED, HELIX_ASSISTED)
- **Default**: LEAST_LOADED
- **Scope**: Client-instance

### 3.5 DaVinci Client Configs

#### `storageClass`

- **Source**: DaVinciConfig.java
- **Type**: enum (DISK, MEMORY_BACKED_BY_DISK, MEMORY)
- **Default**: MEMORY_BACKED_BY_DISK
- **Scope**: Client-instance

#### `recordTransformerEnabled`

- **Source**: DaVinciConfig.java
- **Type**: boolean
- **Default**: false
- **Scope**: Client-instance

### 3.6 Push Job Configs

Key push job settings from `PushJobSetting.java` and `VenicePushJobConstants.java`:

#### Push type and source

- `VENICE_STORE_NAME_PROP` - target store name
- `INPUT_PATH_PROP` - input data path
- `KEY_FIELD_PROP` / `VALUE_FIELD_PROP` - schema fields
- `PUSH_JOB_STATUS_UPLOAD_ENABLE` - enable status upload

#### Push behavior

- `INCREMENTAL_PUSH` - boolean, incremental push mode
- `SOURCE_KAFKA` - boolean, use Kafka as input source (for repush)
- `TARGETED_REGION_PUSH_ENABLED` - boolean, push to specific region
- `TARGETED_REGION_PUSH_LIST` - target regions
- `DEFERRED_VERSION_SWAP` - boolean, defer version swap

#### Compression

- `COMPRESSION_STRATEGY` - NO_OP / GZIP / ZSTD_WITH_DICT
- `ZSTD_COMPRESSION_LEVEL` - ZSTD compression level

#### TTL

- `REPUSH_TTL_ENABLED` - boolean, enable TTL-based repush
- `REPUSH_TTL_IN_SECONDS` - TTL value

---

## 4. Dependency Graphs

### Chain 1: Replication Hierarchy

```
Native Replication (NR)
  |-- Active-Active (AA) [requires NR - VeniceParentHelixAdmin validates]
       |-- Incremental Push [requires AA]
       |-- Write Compute [auto for hybrid+AA - ParentControllerConfigUpdateUtils:40-92]
       |    |-- Chunking [auto when WC - ParentControllerConfigUpdateUtils:94-115]
       |    |-- RMD Chunking [auto when WC+AA - ParentControllerConfigUpdateUtils:117-138]
       |-- Separate RT Topic [optional, for inc push]
```

### Chain 2: Hybrid Store

```
HybridStoreConfig (rewindSeconds + offsetLagThreshold)
  |-- RT Topic creation [auto]
  |-- Separate RT Topic [if incrementalPushEnabled]
  |-- Disk Quota applicability
  |-- Log Compaction eligibility
```

### Chain 3: Compression

```
CompressionStrategy = ZSTD_WITH_DICT
  |-- Dictionary training [PushJob generates dictionary from input data]
  |-- Dictionary stored in version metadata
  |-- Router dictionary retrieval [for server-side decompression]
  |-- Client decompression [when clientDecompressionEnabled=true]
```

### Chain 4: Blob Transfer (cross-layer)

```
Store.blobTransferEnabled = true
  |-- Version.blobTransferEnabled = true [inherited]
       |-- Server ENABLE_BLOB_TRANSFER = true [must be set at cluster level]
```

### Chain 5: Chunking

```
Store.chunkingEnabled = true
  |-- Large values split into chunks at ingestion
  |-- Chunks reassembled at read time (server or client)
  |-- Store.rmdChunkingEnabled = true [requires chunking + AA]
       |-- RMD (replication metadata) also chunked
```

---

## 5. Store Profile Prerequisites Map

| Store Feature            | Required Environment Config                                                                  |
| ------------------------ | -------------------------------------------------------------------------------------------- |
| AA Replication           | `NATIVE_REPLICATION_SOURCE_FABRIC` must be set                                               |
| AA as default for hybrid | `ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE`                               |
| Write Compute (auto)     | `ENABLE_PARTIAL_UPDATE_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES`                                 |
| Incremental Push (auto)  | `ENABLE_INCREMENTAL_PUSH_FOR_HYBRID_ACTIVE_ACTIVE_USER_STORES`                               |
| Separate RT Topic (auto) | `ENABLE_SEPARATE_REAL_TIME_TOPIC_FOR_STORE_WITH_INCREMENTAL_PUSH`                            |
| Deferred Version Swap    | `CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED`                                           |
| DaVinci Push Status      | `PUSH_STATUS_STORE_ENABLED` + `CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE` |
| Meta System Store        | `CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE`                                              |
| Blob Transfer            | Server `ENABLE_BLOB_TRANSFER`                                                                |

---

## 6. Mutual Exclusions & Constraints

### Mutually Exclusive Enum Values

| Dimension              | Possible Values                                 | Notes               |
| ---------------------- | ----------------------------------------------- | ------------------- |
| CompressionStrategy    | NO_OP, GZIP, ZSTD_WITH_DICT                     | Only one per store  |
| PushType               | BATCH, INCREMENTAL, STREAM, STREAM_REPROCESSING | Per version         |
| DaVinci StorageClass   | DISK, MEMORY_BACKED_BY_DISK, MEMORY             | Per client instance |
| StoreMetadataFetchMode | SERVER_BASED_METADATA, CLIENT_CACHED_METADATA   | Per client          |
| BackupStrategy         | KEEP_MIN_VERSIONS, DELETE_ON_NEW_PUSH_START     | Per store           |
| PersistenceType        | ROCKS_DB, IN_MEMORY, BLACK_HOLE                 | Per server          |

### Hard Constraints

1. **AA requires NR**: `activeActiveReplicationEnabled=true` requires `nativeReplicationEnabled=true`
2. **WC requires AA** (for hybrid): Write compute on hybrid stores requires active-active
3. **RMD chunking requires chunking + AA**: `rmdChunkingEnabled=true` requires `chunkingEnabled=true` AND
   `activeActiveReplicationEnabled=true`
4. **Incremental push requires AA**: `incrementalPushEnabled=true` requires `activeActiveReplicationEnabled=true`
5. **Early throttle requires read throttling**: `ROUTER_EARLY_THROTTLE_ENABLED` requires `ROUTER_ENABLE_READ_THROTTLING`
6. **ZSTD_WITH_DICT not for nearline-only**: Dictionary compression requires batch push to generate dictionary
7. **Deferred version swap requires service**: Store-level deferred swap requires
   `CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED`
8. **Separate RT topic requires incremental push**: `separateRealTimeTopicEnabled` requires `incrementalPushEnabled`

---

## 7. Auto-Enablement Rules

These rules are enforced in `ParentControllerConfigUpdateUtils`:

| Trigger Condition                 | Auto-Enabled Flag            | Source Location                           |
| --------------------------------- | ---------------------------- | ----------------------------------------- |
| Hybrid + AA + env flag            | writeComputationEnabled      | ParentControllerConfigUpdateUtils:40-92   |
| Hybrid + AA + env flag            | incrementalPushEnabled       | ParentControllerConfigUpdateUtils         |
| writeComputationEnabled = true    | chunkingEnabled = true       | ParentControllerConfigUpdateUtils:94-115  |
| writeComputationEnabled + AA      | rmdChunkingEnabled = true    | ParentControllerConfigUpdateUtils:117-138 |
| incrementalPushEnabled + env flag | separateRealTimeTopicEnabled | ParentControllerConfigUpdateUtils         |

---

## 8. Component Impact Matrix

| Flag Category       | Router | Server  | Controller | FastClient | ThinClient | DaVinci     | PushJob  |
| ------------------- | ------ | ------- | ---------- | ---------- | ---------- | ----------- | -------- |
| Replication (NR/AA) | -      | Write   | Config     | -          | -          | Write       | -        |
| Write Compute       | -      | Write   | Config     | -          | -          | Write       | -        |
| Read Compute        | Route  | Compute | Config     | Read       | Read       | Read        | -        |
| Chunking            | -      | Both    | Config     | Read       | Read       | Read        | -        |
| Compression         | Decomp | Store   | Config     | Decomp     | Decomp     | Decomp      | Compress |
| Hybrid Config       | -      | Ingest  | Config     | -          | -          | Ingest      | -        |
| Incremental Push    | -      | Ingest  | Config     | -          | -          | Ingest      | Push     |
| Blob Transfer       | -      | Boot    | Config     | -          | -          | Boot        | -        |
| Quota/Throttling    | Route  | Enforce | Config     | -          | -          | -           | -        |
| Routing Strategy    | Route  | -       | -          | Route      | Route      | -           | -        |
| Long-tail Retry     | Route  | -       | -          | Retry      | Retry      | -           | -        |
| Connection Warming  | Route  | -       | -          | -          | -          | -           | -        |
| DaVinci Storage     | -      | -       | -          | -          | -          | Storage     | -        |
| System Stores       | -      | Ingest  | Config     | Meta       | Meta       | Push Status | -        |

---

## 9. 42-Dimension Feature Matrix

This matrix defines the input for the combinatorial integration test. See `tests/venice-feature-matrix-test/` for the
test implementation.

**Feature Maturity Levels:**

- **MATURE** — Always-on in production; fixed to production value in PICT model (not varied)
- **MATURING** — Active development; tested in both on/off states

### Write Path (13 dimensions)

| ID  | Dimension             | Values                              | Maturity | Constraint                         |
| --- | --------------------- | ----------------------------------- | -------- | ---------------------------------- |
| W1  | Data Flow Topology    | Batch-only / Hybrid / Nearline-only | MATURING | --                                 |
| W2  | Native Replication    | **on** (fixed)                      | MATURE   | Implied by W3=on                   |
| W3  | Active-Active         | **on** (fixed)                      | MATURE   | Always-on in production            |
| W4  | Write Compute         | on / off                            | MATURING | W3=on, W1!=Batch-only              |
| W5  | Chunking              | on / off                            | MATURING | Auto when W4=on                    |
| W6  | RMD Chunking          | on / off                            | MATURING | W5=on AND W3=on                    |
| W7  | Incremental Push      | on / off                            | MATURING | W3=on, W1!=Nearline-only           |
| W8  | Compression           | NO_OP / GZIP / ZSTD_WITH_DICT       | MATURING | ZSTD invalid when W1=Nearline-only |
| W9  | Deferred Version Swap | on / off                            | MATURING | C5=on                              |
| W10 | Target Region Push    | on / off                            | MATURING | W2=on                              |
| W11 | Push Engine           | MapReduce / Spark                   | MATURING | --                                 |
| W12 | TTL Repush            | on / off                            | MATURING | W1!=Nearline-only                  |
| W13 | Separate RT Topic     | on / off                            | MATURING | W7=on                              |

### Read Path (6 dimensions)

| ID  | Dimension                  | Values                               | Maturity | Constraint      |
| --- | -------------------------- | ------------------------------------ | -------- | --------------- |
| R1  | Client Type                | Thin / Fast / DaVinci                | MATURING | --              |
| R2  | Read Compute               | on / off                             | MATURING | --              |
| R3  | DaVinci Storage Class      | MEMORY_BACKED_BY_DISK / MEMORY / N/A | MATURING | R1=DaVinci      |
| R4  | Fast Client Routing        | LEAST_LOADED / HELIX_ASSISTED / N/A  | MATURING | R1=Fast         |
| R5  | Long-tail Retry            | on / off                             | MATURING | R1=Thin or Fast |
| R6  | DaVinci Record Transformer | on / off / N/A                       | MATURING | R1=DaVinci      |

### Server (6 dimensions)

| ID  | Dimension                 | Values         | Maturity | Constraint      |
| --- | ------------------------- | -------------- | -------- | --------------- |
| S1  | Parallel Batch Get        | on / off       | MATURING | --              |
| S2  | Fast Avro                 | **on** (fixed) | MATURE   | default=true    |
| S3  | AA/WC Parallel Processing | on / off       | MATURING | W3=on AND W4=on |
| S4  | Blob Transfer             | on / off       | MATURING | --              |
| S5  | Quota Enforcement         | on / off       | MATURING | --              |
| S6  | Adaptive Throttler        | on / off       | MATURING | --              |

### Controller (9 dimensions)

| ID  | Dimension                         | Values         | Maturity | Constraint   |
| --- | --------------------------------- | -------------- | -------- | ------------ |
| C1  | AA Default for Hybrid             | **on** (fixed) | MATURE   | Always-on    |
| C2  | WC Auto for Hybrid+AA             | on / off       | MATURING | --           |
| C3  | Inc Push Auto for Hybrid+AA       | on / off       | MATURING | --           |
| C4  | Separate RT Auto for Inc Push     | on / off       | MATURING | --           |
| C5  | Deferred Version Swap Service     | on / off       | MATURING | --           |
| C6  | Schema Validation                 | **on** (fixed) | MATURE   | default=true |
| C7  | Backup Version Cleanup            | **on** (fixed) | MATURE   | Always-on    |
| C8  | System Store Auto-Materialization | on / off       | MATURING | --           |
| C9  | Superset Schema Generation        | on / off       | MATURING | --           |

### Router (8 dimensions) — EXCLUDED from PICT

Router properties are not yet supported in `VeniceMultiRegionClusterCreateOptions`. All RT dimensions use their defaults
until `routerProperties` support is added.

| ID  | Dimension             | Values                        | Maturity | Constraint | Status   |
| --- | --------------------- | ----------------------------- | -------- | ---------- | -------- |
| RT1 | Read Throttling       | on / off                      | MATURE   | --         | Excluded |
| RT2 | Early Throttle        | on / off                      | MATURING | RT1=on     | Excluded |
| RT3 | Smart Long-tail Retry | on / off                      | MATURE   | --         | Excluded |
| RT4 | Routing Strategy      | LEAST_LOADED / HELIX_ASSISTED | MATURING | --         | Excluded |
| RT5 | Latency-based Routing | on / off                      | MATURING | --         | Excluded |
| RT6 | Client Decompression  | on / off                      | MATURE   | --         | Excluded |
| RT7 | HTTP/2 Inbound        | on / off                      | MATURING | --         | Excluded |
| RT8 | Connection Warming    | on / off                      | MATURING | --         | Excluded |

### PICT Constraints

With W2=on, W3=on fixed and RT dimensions excluded, several constraints are trivially satisfied and removed:

```
IF [W6_RmdChunking] = "on" THEN [W5_Chunking] = "on";
IF [W1_Topology] = "Nearline-only" THEN [W8_Compression] <> "ZSTD_WITH_DICT";
IF [W9_DeferredSwap] = "on" THEN [C5_DeferredSwapService] = "on";
IF [W13_SeparateRTTopic] = "on" THEN [W7_IncrementalPush] = "on";
IF [W12_TTLRepush] = "on" THEN [W1_Topology] <> "Nearline-only";
IF [R1_ClientType] <> "DaVinci" THEN [R3_DaVinciStorage] = "N_A";
IF [R1_ClientType] = "DaVinci" THEN [R3_DaVinciStorage] <> "N_A";
IF [R1_ClientType] <> "Fast" THEN [R4_FastRouting] = "N_A";
IF [R1_ClientType] = "Fast" THEN [R4_FastRouting] <> "N_A";
IF [R1_ClientType] <> "DaVinci" THEN [R6_RecordTransformer] = "N_A";
IF [R1_ClientType] = "DaVinci" THEN [R6_RecordTransformer] <> "N_A";
IF [R1_ClientType] = "DaVinci" THEN [R5_LongTailRetry] = "off";
IF [W4_WriteCompute] = "off" THEN [S3_AAWCParallel] = "off";
IF [W4_WriteCompute] = "on" THEN [W1_Topology] <> "Batch-only";
IF [W7_IncrementalPush] = "on" THEN [W1_Topology] <> "Nearline-only";
```

### Estimated Test Count

With 34 active dimensions (9 fixed to MATURE values, 8 RT excluded), 2-way (t=2) combinatorial testing produces
approximately **19 test cases** across **18 unique cluster configurations**.

See `tests/venice-feature-matrix-test/src/test/resources/feature-matrix-model.pict` for the complete PICT model
definition.
