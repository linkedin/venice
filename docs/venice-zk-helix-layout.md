# Venice ZooKeeper / Helix Data Structure Documentation

This document describes the ZooKeeper and Helix data layout used by Venice controllers, servers, and routers. It is designed to provide accurate context for understanding and querying Venice's distributed state management.

---

## Table of Contents

1. [Overview](#overview)
2. [ZooKeeper Root Layout](#zookeeper-root-layout)
3. [Global Store Configuration](#1-global-store-configuration)
4. [Helix Controller Cluster](#2-helix-controller-cluster-shared)
5. [Individual Venice Cluster Layout](#3-individual-venice-cluster-layout)
6. [Store-Specific Metadata](#4-store-specific-metadata)
7. [Data Ownership Summary](#data-ownership-summary)

---

## Overview

Venice uses Apache Helix for cluster management and ZooKeeper as the underlying coordination service. The data structure spans three logical levels:

1. **Global Level** - Cross-cluster store routing configuration
2. **Controller Cluster Level** - Venice controller leader election
3. **Storage Cluster Level** - Per-cluster resources, stores, and push status

### Cluster Types

| Environment            | Clusters |
|------------------------|----------|
| EI (Early Integration) | `mt-0`, `test-cluster`, `venice-0`, `venice-1` |
| Cert (Certification)   | `cert-0`, `cert-1` |
| Production             | `venice-0` through `venice-16` |

### Parent Controller ZooKeeper Locations

| Environment | ZK Cluster | Path |
|-------------|------------|------|
| EI          | ei-ltx1    | `/venice-parent/` |
| Production  | corp-lva1  | `/venice-parent/` |

### Helix Cluster Types in Venice

1. **Venice Storage Cluster** (e.g., `venice-0`)
   - **Purpose**: Assign store version replicas to storage nodes
   - **Participants**: Venice storage nodes
   - **Spectators**: Venice controllers and routers
   - **Resources**: Store versions (e.g., `test-store_v1`)

2. **Venice Controller Cluster** (`venice-controllers`)
   - **Purpose**: Select Venice logical leader per storage cluster (this is different form storage clusters)
   - **Participants**: Venice controllers
   - **Resources**: Venice cluster names (e.g., `venice-0`, `venice-1`)
   - **Note**: Each resource has one partition with replication for fault tolerance

---

## ZooKeeper Root Layout

```
/venice
├── storeConfigs/                    # Global store → cluster mapping
│   └── <StoreName>                  # Per-store routing config
├── venice-controllers/              # Controller leader election cluster
│   ├── CONFIGS/
│   ├── CONTROLLER/
│   ├── EXTERNALVIEW/
│   ├── IDEALSTATES/
│   ├── INSTANCES/
│   ├── LIVEINSTANCES/
│   ├── PROPERTYSTORE/
│   └── STATEMODELDEFS/
├── venice-0/                        # Individual Venice storage cluster
├── venice-1/
├── ...
└── venice-16/
```

---

## 1. Global Store Configuration

### Path

```
/venice/storeConfigs/<StoreName>
```

### Example

```
/venice/storeConfigs/PremiumAITargetedOffers
```

### Data Structure

```json
{
  "storeName": "PremiumAITargetedOffers",
  "cluster": "venice-1",
  "deleting": false,
  "migrationDestCluster": null,
  "migrationSrcCluster": null
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `storeName` | String | Unique store identifier |
| `cluster` | String | Current owning Venice cluster |
| `deleting` | Boolean | True if store is being deleted |
| `migrationSrcCluster` | String | Source cluster during migration (null otherwise) |
| `migrationDestCluster` | String | Destination cluster during migration (null otherwise) |

### Semantics

- **What it represents**: The authoritative mapping of store name to Venice cluster
- **Who writes it**: Venice controllers (parent or child)
- **When it changes**:
  - Store creation: Created with initial cluster assignment
  - Store migration: Updated with src/dest clusters during migration
  - Store deletion: `deleting` flag set to true, then node removed
- **How it is used**:
  - **Routing**: Routers use this to determine which cluster serves a store
  - **Migration**: Controllers coordinate cross-cluster migration using migration fields
  - **Deletion**: Controllers check `deleting` flag before cleanup operations

### Accessor Class

`ZkStoreConfigAccessor` (`internal/venice-common`)

---

## 2. Helix Controller Cluster (Shared)

### Path

```
/venice/venice-controllers/
```

### Structure

```
/venice/venice-controllers/
├── CONFIGS/
│   ├── CLUSTER/
│   │   └── venice-controllers         # Cluster-level Helix config
│   ├── PARTICIPANT/
│   │   └── <controller_host_port>     # Per-controller participant config
│   └── RESOURCE/
│       └── <cluster_name>             # Per-resource config (e.g., venice-0)
├── CONTROLLER/
│   ├── HISTORY                        # Controller leadership history
│   ├── LEADER                         # Current Helix controller leader
│   └── MESSAGES                       # Controller-level messages
├── EXTERNALVIEW/
│   └── <cluster_name>                 # Current state of each Venice cluster resource
├── IDEALSTATES/
│   └── <cluster_name>                 # Desired state for each Venice cluster resource
├── INSTANCES/
│   └── <controller_host_port>         # Registered controller instances
├── LIVEINSTANCES/
│   └── <controller_host_port>         # Currently connected controllers (ephemeral)
├── PROPERTYSTORE/                     # Helix generic property store (not heavily used by Venice)
└── STATEMODELDEFS/
    ├── LeaderStandby                  # State model for Venice controller leadership
    └── OnlineOffline                  # Standard Helix state model
```

### Node Descriptions

| Node | Helix Concept | Venice Usage |
|------|--------------|--------------|
| `CONFIGS/CLUSTER` | Cluster configuration | Helix cluster settings |
| `CONFIGS/PARTICIPANT` | Per-instance config | Controller-specific settings |
| `CONFIGS/RESOURCE` | Per-resource config | Per-Venice-cluster settings |
| `CONTROLLER` | Helix controller management | Helix internal leader election |
| `EXTERNALVIEW` | Current partition assignment state | Which controller leads which Venice cluster |
| `IDEALSTATES` | Desired partition assignment | Target controller-to-cluster mapping |
| `INSTANCES` | Registered participants | All known Venice controllers |
| `LIVEINSTANCES` | Connected participants | Currently active controllers (ephemeral nodes) |
| `PROPERTYSTORE` | Generic key-value store | Minimally used by Venice |
| `STATEMODELDEFS` | State machine definitions | `LeaderStandby` for controller leadership |

### Resources in Controller Cluster

Each Venice storage cluster (e.g., `venice-0`) is represented as a **resource** in the controller cluster. The resource has:
- **1 partition** (partition 0)
- **Replication factor** for fault tolerance
- **State model**: `LeaderStandby`

The controller that holds the `LEADER` state for a resource is the **Venice logical leader** for that storage cluster.

---

## 3. Individual Venice Cluster Layout

### Cluster Root

```
/venice/<ClusterName>
```

Example: `/venice/venice-0`

### Structure

```
/venice/<ClusterName>/
├── adminTopicMetadata               # Admin topic consumption offset (V1 format)
├── adminTopicMetadataV2             # Admin topic consumption metadata (V2 format with positions)
├── ClusterConfig                    # Live cluster configuration
├── DarkClusterConfig                # Dark cluster configuration (for testing)
├── CONFIGS/                         # Helix cluster configs
│   ├── CLUSTER/
│   ├── PARTICIPANT/
│   └── RESOURCE/
├── CONTROLLER/                      # Helix controller for this cluster
├── CUSTOMIZEDVIEW/                  # Helix customized view for push monitoring
├── executionids/                    # Admin operation execution IDs
│   ├── lastGeneratedExecutionId
│   ├── lastSucceedExecutionId
│   └── succeededPerStore
├── EXTERNALVIEW/                    # Current replica assignments
│   └── <store_version>              # e.g., test-store_v1
├── IDEALSTATES/                     # Desired replica assignments
│   └── <store_version>
├── INSTANCES/                       # Registered storage node instances
├── LIVEINSTANCES/                   # Active storage nodes (ephemeral)
├── OfflinePushes/                   # Push job status tracking
│   └── <store_version>/
│       ├── (push status)
│       └── <partition_id>           # Per-partition status
├── ParentOfflinePushes/             # Parent controller push tracking
├── PROPERTYSTORE/                   # Helix property store
├── routers/                         # Router cluster management
│   ├── (router cluster config)
│   └── <router_host_port>           # Individual router nodes (ephemeral)
├── STATEMODELDEFS/                  # State models
├── STATUS/                          # Instance/resource status
├── StoreGraveyard/                  # Deleted store metadata
│   └── <StoreName>
└── Stores/                          # Active store metadata
    └── <StoreName>/
        ├── (store metadata)
        ├── key-schema/
        ├── value-schema/
        ├── derived-schema/
        └── timestamp-metadata-schema/
```

### Venice-Managed Nodes

#### `adminTopicMetadata`

**Path**: `/<ClusterName>/adminTopicMetadata` (this is deprecated)

**Purpose**: Tracks the last consumed offset from the admin topic for this cluster.

**Data Format (V1)**:
```json
{
  "executionId": 1234567890,
  "offset": 500,
  "upstream": 88998324343
}
```

**Accessor**: `ZkAdminTopicMetadataAccessor`

#### `adminTopicMetadataV2`

**Path**: `/<ClusterName>/adminTopicMetadataV2`

**Purpose**: Enhanced admin topic metadata with PubSub position support.

**Data Format (V2)**:
```json
{
  "executionId": 1234567890,
  "offset": 500,  (being deprecated so avoid using id)
  "upstreamOffset": 88998324343, (being deprecated so avoid using id)
  "position": { "typeId": 1, "base64PositionBytes": "AAAAAAAB9A==" }, 
  "upstreamPosition": { "typeId": 1, "base64PositionBytes": "AAAAFB0d+nc=" }
}
```

#### `executionids/`

**Path**: `/<ClusterName>/executionids/`

**Purpose**: Tracks admin operation execution IDs for idempotency and ordering.

**Children**:
- `lastGeneratedExecutionId` - Counter for generating new execution IDs
- `lastSucceedExecutionId` - Last successfully executed operation ID
- `succeededPerStore` - Map of store → last succeeded execution ID

**Accessor**: `ZkExecutionIdAccessor`

#### `OfflinePushes/`

**Path**: `/<ClusterName>/OfflinePushes/<store_version>/`

**Purpose**: Tracks push job progress and status.

**Structure**:
```
OfflinePushes/
└── <store>_v<version>/
    ├── (OfflinePushStatus JSON at root node)
    ├── 0/                           # Partition 0 status
    ├── 1/                           # Partition 1 status
    └── ...
```

**Data Structure**:
- **Root node** (`OfflinePushStatus`): kafkaTopic, numberOfPartition, replicationFactor, strategy, currentStatus, statusHistory[],
- **Partition nodes** (`PartitionStatus`): partitionId, replicaStatuses[] with instanceId, currentStatus, statusHistory[]

**Accessor**: `VeniceOfflinePushMonitorAccessor`


#### `routers/`

**Path**: `/<ClusterName>/routers/`

**Purpose**: Router discovery and cluster configuration.

**Structure**:
- Root node contains `RoutersClusterConfig` (throttling settings, expected router count)
- Child nodes: ephemeral nodes created by each router with format `<hostname>_<port>` (e.g., `ltx1-app0767.stg.linkedin.com_19966`)

**Accessor**: `ZkRoutersClusterManager`

#### `StoreGraveyard/`

**Path**: `/<ClusterName>/StoreGraveyard/<StoreName>`

**Purpose**: Preserves metadata of deleted stores for version number continuity.

**Why it exists**: When a store is re-created, Venice must continue from the next version number to avoid topic name collisions.

**Data Structure**: `Store` (full store metadata at time of deletion)

**Accessor**: `HelixStoreGraveyard`

#### `Stores/`

**Path**: `/<ClusterName>/Stores/<StoreName>/`

**Purpose**: Active store metadata and schemas.

See [Store-Specific Metadata](#4-store-specific-metadata) for details.

### Helix-Managed Nodes

| Node | Purpose |
|------|---------|
| `CONFIGS/` | Helix configuration for cluster, participants, resources |
| `CONTROLLER/` | Helix controller leadership for this storage cluster |
| `CUSTOMIZEDVIEW/` | Aggregated customized state (used for push monitoring) |
| `EXTERNALVIEW/` | Current partition-to-instance assignments |
| `IDEALSTATES/` | Desired partition-to-instance assignments |
| `INSTANCES/` | Registered storage node participants |
| `LIVEINSTANCES/` | Currently connected storage nodes (ephemeral) |
| `PROPERTYSTORE/` | Generic Helix property store |
| `STATEMODELDEFS/` | State machine definitions (`LeaderStandby`, `OnlineOffline`) |
| `STATUS/` | Instance and resource status information |

---

## 4. Store-Specific Metadata

### Store Root

```
/venice/<ClusterName>/Stores/<StoreName>/
```

Example: `/venice/venice-1/Stores/PremiumAITargetedOffers/`

### Structure

```
/venice/<ClusterName>/Stores/<StoreName>/
├── (store metadata JSON)
├── key-schema/
│   └── 1                            # Key schema (only ID=1 supported)
├── value-schema/
│   ├── 1                            # Value schema version 1
│   ├── 2                            # Value schema version 2
│   └── ...
├── derived-schema/
│   ├── 1-1                          # Derived schema (valueSchemaId-derivedSchemaId)
│   └── ...
└── timestamp-metadata-schema/       # Replication metadata schemas
    ├── 1-1                          # (valueSchemaId-rmdSchemaId)
    └── ...
```

### Store Metadata (Root Node)

**Path**: `/<ClusterName>/Stores/<StoreName>`

**Data Structure**: `Store` / `ZKStore` (JSON serialized)

**Key Fields**:
- `name` - Store name
- `owner` - Store owner
- `createdTime` - Creation timestamp
- `currentVersion` - Currently serving version number
- `largestUsedVersionNumber` - Highest version ever created
- `versions` - List of version metadata
- `partitionCount` - Number of partitions
- `replicationFactor` - Number of replicas per partition
- `readQuotaInCU` - Read quota in capacity units
- `hybridStoreConfig` - Hybrid store settings (if enabled)
- `partitionerConfig` - Custom partitioner settings
- `accessControlled` - ACL enabled flag
- `compressionStrategy` - Compression type
- `storageQuotaInByte` - Storage limit
- `systemStores` - Map of system store configurations

**Accessor**: `CachedReadOnlyStoreRepository`, `HelixReadOnlyStoreRepository`

### Key Schema

**Path**: `/<ClusterName>/Stores/<StoreName>/key-schema/1`

**Data Structure**: `SchemaEntry` (JSON serialized Avro schema)

**Note**: Venice supports only one key schema per store (ID always = 1).

**Accessor**: `HelixSchemaAccessor`

### Value Schemas

**Path**: `/<ClusterName>/Stores/<StoreName>/value-schema/<schemaId>`

**Data Structure**: `SchemaEntry` (JSON serialized Avro schema)

**Note**: Value schemas are versioned, starting from ID 1. New schemas must be backward compatible.

**Accessor**: `HelixSchemaAccessor`

### Derived Schemas (Write Compute)

**Path**: `/<ClusterName>/Stores/<StoreName>/derived-schema/<valueSchemaId>-<derivedSchemaId>`

**Data Structure**: `DerivedSchemaEntry`

**Purpose**: Schemas for partial update operations.

**Accessor**: `HelixSchemaAccessor`

### Replication Metadata Schemas

**Path**: `/<ClusterName>/Stores/<StoreName>/timestamp-metadata-schema/<valueSchemaId>-<rmdSchemaId>`

**Data Structure**: `RmdSchemaEntry`

**Purpose**: Schemas for Active-Active replication metadata (timestamps, vector clocks).

**Accessor**: `HelixSchemaAccessor`

---

## Data Ownership Summary

| Path Pattern | Written By | Read By | Node Type |
|--------------|-----------|---------|-----------|
| `/storeConfigs/<store>` | Controller | Controller, Router | Persistent |
| `/venice-controllers/LIVEINSTANCES/*` | Controllers | Helix | Ephemeral |
| `/<cluster>/adminTopicMetadata*` | Controller (leader) | Controller | Persistent |
| `/<cluster>/ClusterConfig` | Controller | All components | Persistent |
| `/<cluster>/executionids/*` | Controller (leader) | Controller | Persistent |
| `/<cluster>/OfflinePushes/*` | Controller, Server | Controller | Persistent |
| `/<cluster>/routers/*` | Router, Controller | Router, Controller | Mixed |
| `/<cluster>/StoreGraveyard/*` | Controller | Controller | Persistent |
| `/<cluster>/Stores/*` | Controller | All components | Persistent |
| `/<cluster>/LIVEINSTANCES/*` | Server | Helix, Controller | Ephemeral |
| `/<cluster>/EXTERNALVIEW/*` | Helix Controller | Router, Controller | Persistent |
| `/<cluster>/IDEALSTATES/*` | Controller | Helix, Server | Persistent |

### Authoritative vs Cached Views

| Data | Authoritative Source | Cached In |
|------|---------------------|-----------|
| Store → Cluster mapping | `/storeConfigs/<store>` | Router memory |
| Store metadata | `/<cluster>/Stores/<store>` | Controller, Router, Server memory |
| Push status | `/<cluster>/OfflinePushes/<version>` | Controller memory |
| Partition assignment | `/<cluster>/EXTERNALVIEW/<version>` | Router memory |

---

## Key Venice-Specific Extensions to Helix

1. **Store Metadata in Helix Cluster** - Venice stores application-level metadata (stores, schemas) under the Helix cluster path rather than using Helix's property store.

2. **Custom Push Monitoring** - Venice implements its own push monitoring (`OfflinePushes/`) rather than relying solely on Helix external view convergence.

3. **Store Graveyard** - Venice maintains deleted store metadata to ensure version number continuity across store re-creation.

4. **Router Cluster Management** - Venice manages router discovery and configuration through a custom ZK-based mechanism rather than treating routers as Helix participants.

5. **Admin Topic Metadata** - Venice tracks Kafka admin topic consumption state in ZK for controller failover recovery.

---

## Clarifications / Known Ambiguities

1. **`PROPERTYSTORE` Usage**: Helix's property store is present but minimally used by Venice. Most application data uses Venice's own ZK paths.

2. **Legacy vs New Paths**: `adminTopicMetadata` (V1) is deprecated; use `adminTopicMetadataV2` (V2) for position-based consumption tracking.

3. **`OfflinePushes`**: Only exists in child clusters where storage nodes reside. Parent controller has no storage clusters, so no `OfflinePushes` path.

---

## Related Code References

| Component | Package/Class |
|-----------|---------------|
| ZK Path Constants | `com.linkedin.venice.zk.VeniceZkPaths` |
| Store Config Access | `com.linkedin.venice.helix.ZkStoreConfigAccessor` |
| Store Repository | `com.linkedin.venice.helix.CachedReadOnlyStoreRepository` |
| Schema Access | `com.linkedin.venice.helix.HelixSchemaAccessor` |
| Push Monitor | `com.linkedin.venice.helix.VeniceOfflinePushMonitorAccessor` |
| Store Graveyard | `com.linkedin.venice.helix.HelixStoreGraveyard` |
| Admin Metadata | `com.linkedin.venice.controller.ZkAdminTopicMetadataAccessor` |
| Execution IDs | `com.linkedin.venice.controller.ZkExecutionIdAccessor` |
| Router Manager | `com.linkedin.venice.helix.ZkRoutersClusterManager` |
| Cluster Config | `com.linkedin.venice.helix.HelixReadOnlyLiveClusterConfigRepository` |

---

## LLM Cheat Sheet

Quick-reference patterns for reasoning about Venice ZK/Helix state.

### Common Query Patterns

#### "Which cluster owns store X?"

```
Path: /venice/storeConfigs/<StoreName>
Field: cluster
```

**Example**: To find where `MyStore` lives, read `/venice/storeConfigs/MyStore` → `{"cluster": "venice-3", ...}`

#### "What is the current version of store X?"

```
Path: /venice/<cluster>/Stores/<StoreName>
Field: currentVersion
```

**Example**: Read `/venice/venice-3/Stores/MyStore` → look for `currentVersion` field

#### "Is a push currently running for store X?"

```
Path: /venice/<cluster>/OfflinePushes/<StoreName>_v<N>
```

If the node exists, a push is tracked. Check `executionStatus` field for state (`STARTED`, `COMPLETED`, `ERROR`).

#### "Which storage nodes are serving store X version N?"

```
Path: /venice/<cluster>/EXTERNALVIEW/<StoreName>_v<N>
```

The external view shows partition → instance mapping with states (`ONLINE`, `BOOTSTRAP`, `OFFLINE`).

#### "Which controller is leader for cluster X?"

```
Path: /venice/venice-controllers/EXTERNALVIEW/<cluster>
```

Look for the instance with `LEADER` state for partition 0.

#### "Is store X being migrated?"

```
Path: /venice/storeConfigs/<StoreName>
Fields: migrationSrcCluster, migrationDestCluster
```

If either field is non-null, migration is in progress.

#### "What schemas does store X have?"

```
Key Schema: /venice/<cluster>/Stores/<StoreName>/key-schema/1
Value Schemas: /venice/<cluster>/Stores/<StoreName>/value-schema/*
```

List children of `value-schema/` to see all version IDs.

#### "Why did store X's version N push fail?"

```
Path: /venice/<cluster>/OfflinePushes/<StoreName>_v<N>
Fields: executionStatus, statusDetails
```

Check partition-level status at `/venice/<cluster>/OfflinePushes/<StoreName>_v<N>/<partitionId>` for replica-level errors.

### State Transition Mental Model

```
Store Creation Flow:
1. /storeConfigs/<store> created (cluster assignment)
2. /<cluster>/Stores/<store> created (metadata)
3. /<cluster>/Stores/<store>/key-schema/1 created
4. /<cluster>/Stores/<store>/value-schema/1 created

Push Flow:
1. /<cluster>/IDEALSTATES/<store>_v<N> created (Helix resource)
2. /<cluster>/OfflinePushes/<store>_v<N> created (push tracking)
3. Servers bootstrap → update partition status
4. /<cluster>/EXTERNALVIEW/<store>_v<N> converges to IDEALSTATES
5. Push completes → store.currentVersion updated

Store Deletion Flow:
1. /storeConfigs/<store>.deleting = true
2. /<cluster>/Stores/<store> → /<cluster>/StoreGraveyard/<store>
3. /<cluster>/IDEALSTATES/<store>_v* deleted
4. /storeConfigs/<store> deleted
```

### Quick Path Lookup Table

| What You Need | ZK Path |
|---------------|---------|
| Store's cluster | `/venice/storeConfigs/<store>` |
| Store metadata | `/venice/<cluster>/Stores/<store>` |
| Store schemas | `/venice/<cluster>/Stores/<store>/value-schema/*` |
| Push status | `/venice/<cluster>/OfflinePushes/<store>_v<N>` |
| Partition assignment | `/venice/<cluster>/EXTERNALVIEW/<store>_v<N>` |
| Live storage nodes | `/venice/<cluster>/LIVEINSTANCES/*` |
| Live routers | `/venice/<cluster>/routers/*` |
| Controller leader | `/venice/venice-controllers/EXTERNALVIEW/<cluster>` |
| Deleted store info | `/venice/<cluster>/StoreGraveyard/<store>` |
| Cluster config | `/venice/<cluster>/ClusterConfig` |

### Common Debugging Scenarios

#### Scenario: "Push stuck in STARTED state"

1. Check partition status: `/<cluster>/OfflinePushes/<store>_v<N>/<partition>`
2. Look for replicas stuck in `BOOTSTRAP` or `OFFLINE`
3. Check if storage nodes are live: `/<cluster>/LIVEINSTANCES/`
4. Compare IDEALSTATE vs EXTERNALVIEW for missing assignments

#### Scenario: "Store not found" errors

1. Verify store exists: `/storeConfigs/<store>`
2. Check cluster assignment matches expected
3. Verify store metadata exists: `/<cluster>/Stores/<store>`
4. Check if store is being deleted: `deleting` flag

#### Scenario: "Controller not responding for cluster X"

1. Check controller cluster: `/venice-controllers/LIVEINSTANCES/`
2. Verify leader assignment: `/venice-controllers/EXTERNALVIEW/<cluster>`
3. Look for leadership changes in `/venice-controllers/CONTROLLER/HISTORY`

#### Scenario: "Version number gap after store re-creation"

1. Check graveyard: `/<cluster>/StoreGraveyard/<store>`
2. `largestUsedVersionNumber` field shows last used version
3. New store versions will continue from this number + 1

### Key Invariants

1. **One cluster per store**: A store exists in exactly one cluster at a time (except during migration)
2. **Version monotonicity**: Version numbers never decrease, even across store deletion/recreation
3. **Schema compatibility**: Value schemas must be backward compatible
4. **Single key schema**: Only one key schema per store (ID = 1)
5. **Leader singleton**: Exactly one controller is leader per storage cluster

---

*Document generated from Venice codebase analysis. Last updated: January 2026*
