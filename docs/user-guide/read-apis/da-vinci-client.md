# Da Vinci Client

The Da Vinci Client is a stateful, embedded caching client that eagerly loads Venice data into local RocksDB storage. It
provides ultra-low latency reads by serving all queries locally with zero network hops.

## When to Use

Choose Da Vinci Client when you need:

- **Ultra-low latency** - Local RocksDB reads with zero network hops
- **Local data access** - Direct access to the entire dataset for batch processing or analytics

**Trade-offs:**

- Higher memory/disk footprint (stores full dataset locally)
- Higher startup time (must bootstrap full dataset on first launch)
- Requires stable local disk storage

For other client options, see the [Read APIs comparison](index.md).

## Basic Usage

### Setup

Add the Venice client dependency:

```gradle
dependencies {
    implementation 'com.linkedin.venice:venice-client:$LATEST_VERSION'
}
```

### Creating a Client

```java
// Create factory (reuse across multiple clients if needed)
CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
    d2Client,                       // D2 client for service discovery
    clusterDiscoveryD2ServiceName,  // D2 service name for Venice discovery
    metricsRepository,              // Metrics repository
    backendConfig                   // VeniceProperties with server config
);

// Configure client
DaVinciConfig config = new DaVinciConfig();
config.setStorageClass(StorageClass.MEMORY_BACKED_BY_DISK);

// Get and start client
DaVinciClient<String, GenericRecord> client =
    factory.getAndStartGenericAvroClient("my-store-name", config);
```

### Reading Data

Da Vinci Client supports the same read operations as other Venice clients:

```java
// Single get
GenericRecord value = client.get("user123").get();

if (value != null) {
    String name = value.get("name").toString();
    Integer age = (Integer) value.get("age");
}

// Batch get
Set<String> keys = Set.of("user123", "user456", "user789");
Map<String, GenericRecord> results = client.batchGet(keys).get();

for (Map.Entry<String, GenericRecord> entry : results.entrySet()) {
    System.out.println("Key: " + entry.getKey());
    System.out.println("Value: " + entry.getValue());
}
```

### Shutdown

```java
client.close();
```

## Configuration Options

Key configuration options for `DaVinciConfig`:

| Option                               | Description                                                                                                                                                                                              | Default                 |
| ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------- |
| `setStorageClass`                    | Storage backend: `DISK` or `MEMORY_BACKED_BY_DISK`                                                                                                                                                       | `MEMORY_BACKED_BY_DISK` |
| `setManaged`                         | Allow Da Vinci to manage local state cleanup for unused stores                                                                                                                                           | true                    |
| `setReadMetricsEnabled`              | Enable read-path metrics (impacts performance)                                                                                                                                                           | false                   |
| `setCacheConfig`                     | In-memory cache that stores deserialized records. Avoids repeated deserialization overhead for frequently accessed keys. Configure with `ObjectCacheConfig` to set max cache size per partition and TTL. | null                    |
| `setRecordTransformerConfig`         | Configure Record Transformer for advanced use cases                                                                                                                                                      | null                    |
| `setLargeBatchRequestSplitThreshold` | When batch get request exceeds this key count, split into chunks of this size and execute concurrently for better performance                                                                            | 100                     |

## Data Lifecycle

### Initial Bootstrap

On first startup, Da Vinci Client downloads the entire dataset from Venice:

1. **Consume from Kafka** - Streams the full dataset from Venice's version topic
2. **Build local storage** - Applies all records to local RocksDB as they arrive
3. **Ready to serve** - Client becomes available for reads once caught up

### Continuous Updates

After bootstrap, the client stays up-to-date by:

- Consuming real-time updates from Kafka topics
- Applying changes to local RocksDB storage
- Automatically handling version swaps during batch pushes

### Version Swaps

When Venice pushes new batch data:

1. Client begins consuming the new version from Kafka in the background
2. Old version continues serving reads
3. Once new version is caught up, client atomically swaps to it
4. Old version data is cleaned up

## Advanced Features

### Blob Transfer (P2P Bootstrapping)

By default, Da Vinci Client bootstraps by consuming the full dataset from Kafka. For large datasets, this can be slow.
**Blob Transfer** enables peer-to-peer (P2P) bootstrapping where clients download snapshots directly from other Da Vinci
instances.

**Benefits:**

- Significantly faster bootstrap for large datasets
- Reduced load on Kafka brokers during initial ingestion

**When to enable:**

- Large stores (> 10 GB) where Kafka bootstrap is too slow
- High client churn requiring frequent bootstrap operations

Blob transfer is disabled by default and must be configured at the store and client level. See
[P2P Bootstrapping](../../operations/advanced/p2p-bootstrapping.md) for configuration details.

### Record Transformer (DVRT)

The Record Transformer (DVRT) is an advanced feature that lets you hook into Da Vinci's data ingestion process to react
to every record change in real-time.

#### What Does It Do?

- **React to changes**: Get notified when records are added, updated, or deleted
- **Transform data**: Modify records as they come in (add fields, filter data, etc.)
- **Forward data**: Send Venice data to other systems (databases, search indexes, analytics)

#### Quick Start

**Step 1: Implement the Interface**

Extend
[DaVinciRecordTransformer](https://github.com/linkedin/venice/blob/main/clients/da-vinci-client/src/main/java/com/linkedin/davinci/client/DaVinciRecordTransformer.java)
and implement:

- `transform` - Transform data before local persistence, returns
  [DaVinciRecordTransformerResult](https://github.com/linkedin/venice/blob/main/clients/da-vinci-client/src/main/java/com/linkedin/davinci/client/DaVinciRecordTransformerResult.java):
  - `UNCHANGED` - Keep original value
  - `TRANSFORMED` - Use new transformed value
  - `SKIP` - Drop this record entirely
- `processPut` - Handle record updates
- `processDelete` - Handle deletions (optional)

**Step 2: Configure and Register**

Build a
[DaVinciRecordTransformerConfig](https://github.com/linkedin/venice/blob/main/clients/da-vinci-client/src/main/java/com/linkedin/davinci/client/DaVinciRecordTransformerConfig.java)
and register it:

```java
DaVinciRecordTransformerConfig config = new DaVinciRecordTransformerConfig.Builder()
    .setRecordTransformerFunction(YourTransformer::new)
    .build();

DaVinciConfig daVinciConfig = new DaVinciConfig();
daVinciConfig.setRecordTransformerConfig(config);
```

**For Custom Constructor Parameters:** If you need to pass additional parameters to your transformer's constructor
beyond the default ones provided:

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

#### Key Concepts

**Version Management**

Venice stores have versions. When new data is pushed, Venice creates a **future version** while the **current version**
continues serving traffic. Once ready, Venice atomically swaps the future version to become the new current version.

For Record Transformers:

- Each version gets its own transformer instance
- During a push, you'll have transformers for both current and future versions running in parallel
- Use `onStartVersionIngestion(partitionId, isCurrentVersion)` to initialize resources
- Use `onEndVersionIngestion(currentVersion)` to clean up when a version stops serving

**Best Practice: Separate Data by Version**

When propagating Venice data to external systems (databases, search indexes, etc.), **always separate data from
different versions into independent storage locations**. Think of it as maintaining one database table per Venice store
version.

**Why Version Separation Matters:**

- **Prevents data races**: Multiple versions writing to the same table creates race conditions
- **Avoids record leaks**: Old version data won't pollute your current dataset
- **Enables clean transitions**: You can atomically switch to new data once ready

**Implementation Strategy:**

1. **Create version-specific storage** (e.g., `user_profiles_v1`, `user_profiles_v2`)
2. **Maintain a pointer to current version** (database views, atomic pointer, etc.)
3. **Switch pointer atomically** when Venice promotes a new current version
4. **Clean up old versions** once no longer needed

See the example below:

```java
AtomicBoolean setUpComplete = new AtomicBoolean();
String tableName = getStoreName() + "_v" + getStoreVersion();
String currentVersionViewName = getStoreName() + "_current_version";

@Override
synchronized public void onStartVersionIngestion(int partitionId, boolean isCurrentVersion) {
  if (setUpComplete.get()) {
    return;
  }

  // Initialize resources for this version
  if (!externalDB.containsTable(tableName)) {
    externalDB.createTable(tableName);
  }

  // Maintain pointer to current version
  if (isCurrentVersion) {
    externalDB.createOrReplaceView(currentViewName, "SELECT * FROM " + tableName);
  }
  setUpComplete.set(true);
}

@Override
public void onEndVersionIngestion(int currentVersion) {
  // Only clean up if this version is no longer serving
  if (currentVersion != getStoreVersion()) {
    String newCurrentTableName = getStoreName() + "_v" + currentVersion;

    // Update view to point to new current version
    externalDB.createOrReplaceView(currentViewName, "SELECT * FROM " + newCurrentTableName);

    // Delete old version
    externalDB.dropTable(tableName);
  }
}
```

**Key Behaviors**

- **Lazy deserialization**: Keys/values are deserialized lazily to avoid unnecessary CPU/memory overhead if you only
  need to inspect some records or parameters
- **Startup replay**: Venice replays existing records from disk on startup to rebuild external state
- **Compatibility checks**: Implementation changes are automatically detected and local state is rebuilt to prevent
  stale data

#### Featured Implementations

- [VeniceChangelogConsumerDaVinciRecordTransformerImpl](https://github.com/linkedin/venice/blob/main/clients/da-vinci-client/src/main/java/com/linkedin/davinci/consumer/VeniceChangelogConsumerDaVinciRecordTransformerImpl.java):
  - The new Venice Change Data Capture (CDC) client was built using the record transformer.
- [DuckDBDaVinciRecordTransformer](https://github.com/linkedin/venice/blob/main/integrations/venice-duckdb/src/main/java/com/linkedin/venice/duckdb/DuckDBDaVinciRecordTransformer.java):
  - Forwards Venice data to DuckDB, allowing you to query your Venice data via SQL.

#### Record Transformer Configuration Options

Configuration options for `DaVinciRecordTransformerConfig`:

| Option                               | Description                                                                                                        | Required | Default |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------ | -------- | ------- |
| `setRecordTransformerFunction`       | Function that creates your transformer instances                                                                   | Yes      | -       |
| `setKeyClass`                        | Deserialize keys into Avro SpecificRecords                                                                         | No       | null    |
| `setOutputValueClass`                | Change output value type (must use with `setOutputValueSchema`)                                                    | No       | null    |
| `setOutputValueSchema`               | Output value schema (must use with `setOutputValueClass`)                                                          | No       | null    |
| `setStoreRecordsInDaVinci`           | Persist records into Da Vinci's local disk                                                                         | No       | true    |
| `setAlwaysBootstrapFromVersionTopic` | Set to true if `storeRecordsInDaVinci` is false, and you're storing records in memory without being backed by disk | No       | false   |
| `setRecordTransformationEnabled`     | Set false if always returning `UNCHANGED` during `transform`                                                       | No       | true    |
| `setRecordMetadataEnabled`           | Enable if you need the record metadata in `DaVinciRecordTransformerRecordMetadata`                                 | No       | false   |
