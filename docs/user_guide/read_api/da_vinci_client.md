---
layout: default
title: Da Vinci Client
parent: Read APIs
grand_parent: User Guides
permalink: /docs/user_guide/read_api/da_vinci_client
---

# Da Vinci Client
This allows you to eagerly load some or all partitions of the dataset and perform queries against the resulting local 
cache. Future updates to the data continue to be streamed in and applied to the local cache.

## Record Transformer (DVRT)

The Da Vinci Record Transformer lets you hook into Venice's data ingestion process to react to every record change in real-time.

### What Does It Do?

- **React to changes**: Get notified when records are added, updated, or deleted
- **Transform data**: Modify records as they come in (add fields, filter data, etc.)
- **Forward data**: Send Venice data to other systems (databases, search indexes, analytics)

### Quick Start Guide

#### Step 1: Implement the Interface
Extend [DaVinciRecordTransformer](https://github.com/linkedin/venice/blob/main/clients/da-vinci-client/src/main/java/com/linkedin/davinci/client/DaVinciRecordTransformer.java) and implement:

- `transform(key, value, partitionId)` - Transform data before local persistence, returns [DaVinciRecordTransformerResult](https://github.com/linkedin/venice/blob/main/clients/da-vinci-client/src/main/java/com/linkedin/davinci/client/DaVinciRecordTransformerResult.java):
  - `UNCHANGED` - Keep original value
  - `TRANSFORMED` - Use new transformed value  
  - `SKIP` - Drop this record entirely
- `processPut(key, value, partitionId)` - Handle record updates
- `processDelete(key, partitionId)` - Handle deletions (optional)

#### Step 2: Configure and Register
Build a [DaVinciRecordTransformerConfig](https://github.com/linkedin/venice/blob/main/clients/da-vinci-client/src/main/java/com/linkedin/davinci/client/DaVinciRecordTransformerConfig.java) and register it:

```java
DaVinciRecordTransformerConfig config = new DaVinciRecordTransformerConfig.Builder()
    .setRecordTransformerFunction(YourTransformer::new)
    .build();

DaVinciConfig daVinciConfig = new DaVinciConfig();
daVinciConfig.setRecordTransformerConfig(config);
```

**For Custom Constructor Parameters:**
If you need to pass additional parameters to your transformer's constructor beyond the default ones provided:

```java
// Your custom parameter
String databasePath = "/my/path";

DaVinciRecordTransformerFunctionalInterface transformerFunction = (
    // Venice-provided parameters:
    storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> 
    new YourTransformer(
        // Venice-provided parameters
        storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, config,
        // Your custom parameter
        databasePath);

DaVinciRecordTransformerConfig config = new DaVinciRecordTransformerConfig.Builder()
    .setRecordTransformerFunction(transformerFunction)
    .build();
```

### Key Concepts

#### Version Management
Venice stores have versions. When new data is pushed, Venice creates a **future version** while the **current version** continues serving traffic. Once ready, Venice atomically swaps the future version to become the new current version.

**For Record Transformers:**
- Each version gets its own transformer instance
- During a push, you'll have transformers for both current and future versions running in parallel
- Use `onStartVersionIngestion(isCurrentVersion)` to initialize resources
- Use `onEndVersionIngestion(currentVersion)` to clean up when a version stops serving

```java
@Override
public void onStartVersionIngestion(boolean isCurrentVersion) {
    // Initialize resources for this version
    externalDB.createTable("store_v" + getStoreVersion());
}

@Override
public void onEndVersionIngestion(int currentVersion) {
    // Only clean up if this version is no longer serving
    if (currentVersion != getStoreVersion()) {
        externalDB.dropTable("store_v" + getStoreVersion());
    }
}
```

#### Key Behaviors
- **Lazy deserialization**: Keys/values are deserialized lazily to avoid unnecessary CPU/memory overhead if you only need
    to inspect some records or parameters
- **Startup replay**: Venice replays existing records from disk on startup to rebuild external state
- **Compatibility checks**: Implementation changes are automatically detected and local state is rebuilt to prevent stale data

### Featured Implementations
- [BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl](https://github.com/linkedin/venice/blob/main/clients/da-vinci-client/src/main/java/com/linkedin/davinci/consumer/BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.java): 
  - The new Venice Change Data Capture (CDC) client was built using the record transformer.
- [DuckDBDaVinciRecordTransformer](https://github.com/linkedin/venice/blob/main/integrations/venice-duckdb/src/main/java/com/linkedin/venice/duckdb/DuckDBDaVinciRecordTransformer.java):
  - Forwards Venice data to DuckDB, allowing you to query your Venice data via SQL.

### Configuration Options

**Required:**
- `setRecordTransformerFunction` - Function that creates your transformer instances

**Optional:**
  - `setKeyClass`: set this if you want to deserialize keys into Avro SpecificRecords.
  - `setOutputValueClass` + `setOutputValueSchema`: required together when changing value type/schema or using Avro
    SpecificRecords for values.
  - `setStoreRecordsInDaVinci` (default: true): persist into Da Vinci’s local disk.
  - `setAlwaysBootstrapFromVersionTopic` (default: false): set this to true if `storeRecordsInDaVinci` is false, and
    you're storing records in memory without being backed by disk.
  - `setSkipCompatibilityChecks` (default: false): consider true when returning `UNCHANGED` during `transform` or
    during frequent changes to the interface without modifying the transform logic.