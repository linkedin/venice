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
A Da Vinci plugin that allows you to register callbacks on per-record puts/deletes and optional record transformation
during ingestion.

### Why Use It
- Easily forward Venice data to an external system through callbacks without having to do a table scan yourself.
- Optimized for performance by being compatible with blob transfer, leading to faster bootstrapping times.
- Transform Venice data in-place without the need for a secondary storage, such as field projection or computing new fields.
- Built-in support for Avro SpecificRecord for keys and values.
- Allows you to become version aware of your Venice data.

### Quick Start Guide
1. Implement [DaVinciRecordTransformer.java](https://github.com/linkedin/venice/blob/main/clients/da-vinci-client/src/main/java/com/linkedin/davinci/client/DaVinciRecordTransformer.java) to register callbacks.

2. Build a [DaVinciRecordTransformerConfig.java](https://github.com/linkedin/venice/blob/main/clients/da-vinci-client/src/main/java/com/linkedin/davinci/client/DaVinciRecordTransformerConfig.java).

3. Register via [DaVinciConfig#setRecordTransformerConfig](https://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciConfig.html#setRecordTransformerConfig(com.linkedin.davinci.client.DaVinciRecordTransformerConfig)).

Example:
```java
DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
    .setRecordTransformerFunction(MyTransformer::new)
    .build();

DaVinciConfig config = new DaVinciConfig();
config.setRecordTransformerConfig(recordTransformerConfig);
```

If you need to pass additional parameters to your record transformer's constructor beyond the default ones provided,
you can use a functional interface to register it like so:
```java
DaVinciRecordTransformerFunctionalInterface recordTransformerFunction = (
    storeNameParam, 
    storeVersion,
    keySchema,
    inputValueSchema,
    outputValueSchema,
    config) -> new ExampleDaVinciRecordTransformer(
        storeNameParam,
        storeVersion,
        keySchema,
        inputValueSchema,
        outputValueSchema,
        config, 
        ...additional parameters);

DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
    .setRecordTransformerFunction(recordTransformerFunction)
    .build();

DaVinciConfig config = new DaVinciConfig();
config.setRecordTransformerConfig(recordTransformerConfig);
```

### Example Implementations
- [BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.java](https://github.com/linkedin/venice/blob/main/clients/da-vinci-client/src/main/java/com/linkedin/davinci/consumer/BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.java): 
  - The new Venice Change Data Capture (CDC) client was built using the record transformer.
- [DuckDBDaVinciRecordTransformer.java](https://github.com/linkedin/venice/blob/main/integrations/venice-duckdb/src/main/java/com/linkedin/venice/duckdb/DuckDBDaVinciRecordTransformer.java):
  - Forwards Venice data to DuckDB, allowing you to query your Venice data via SQL.

### Details
#### Core concepts:
- Startup playback: on startup, the record transformer replays on-disk records by invoking `processPut` to rehydrate
  external systems.
- Lazy deserialization: key and value inputs are wrapped in `Lazy` to avoid unnecessary deserialization.
- Per-version instances: one transformer instance is created per store version.
- Compatibility checks: when a change is detected in your transformer implementation, local state will be wiped
  automatically and will then bootstrap from the Version Topic. This ensures data consistency by preventing stale
  transformations. If you're not transforming your data, take a look at `setSkipCompatibilityChecks` in the config
  section below.

#### Callbacks:
- `transform(key, value, partitionId)`: decide what to do with each record. Return `SKIP` to drop, `UNCHANGED` to keep
  the original value, or `TRANSFORMED` with a new value.
- `processPut(key, value, partitionId)`: side effects for puts/updates (for example, write to an external DB or index).
- `processDelete(key, partitionId)`: side-effects for deletes.
- `onStartVersionIngestion(isCurrentVersion)`: initialize resources for `getStoreVersion()` (connections, tables, views).
- `onEndVersionIngestion(currentVersion)`: close/release resources. Invoked when a version stops serving (e.g., after a
  version swap) or when the application is shutting down. For batch stores, this may also be invoked during normal
  operation once batch ingestion completes, since batch stores don't continuously receive updates like hybrid stores.

#### Configs:
- Required:
  - `setRecordTransformerFunction`: functional interface that constructs your transformer.
- Optional:
  - `setKeyClass`: set this if you want to deserialize keys into Avro SpecificRecords.
  - `setOutputValueClass` + `setOutputValueSchema`: required together when changing value type/schema or using Avro
    SpecificRecords for values.
  - `setStoreRecordsInDaVinci` (default: true): persist into Da Vinci’s local disk.
  - `setAlwaysBootstrapFromVersionTopic` (default: false): set this to true if `storeRecordsInDaVinci` is false, and
    you're storing records in memory without being backed by disk.
  - `setSkipCompatibilityChecks` (default: false): consider true when returning `UNCHANGED` during `transform` or
    during rapid, non-functional edits.