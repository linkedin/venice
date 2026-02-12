# Change Data Capture (CDC)

Venice's Change Data Capture (CDC) API allows applications to consume a stream of changes from Venice stores. Unlike the
point-in-time read clients (Thin, Fast, Da Vinci), CDC provides a continuous feed of all mutations, making it ideal for
building derived systems, maintaining caches, triggering workflows, or replicating data.

## Characteristics

- **Stream-based**: Consumes change events as they occur, not point-in-time snapshots
- **After images**: Access current value for each change
- **Checkpointing**: Save and restore consumption position for restarts
- **Partition-aware**: Subscribe to specific partitions or all partitions
- **Hybrid-aware**: Automatically handles version swaps during batch pushes; restart to consume batch data

## When to Use

Choose the CDC client when:

- You need to react to data changes in real-time
- You're building derived data systems or materialized views
- You need to maintain external caches synchronized with Venice
- You're implementing event-driven workflows triggered by data changes

For point-in-time reads, consider the [Thin Client](thin-client.md), [Fast Client](fast-client.md), or
[Da Vinci Client](da-vinci-client.md).

## Usage

### Dependency

Add the Da Vinci client dependency to your project (CDC is part of this module):

```groovy
dependencies {
  implementation 'com.linkedin.venice:venice-client:<version>'
}
```

### Creating a Consumer

Use `VeniceChangelogConsumerClientFactory` to create CDC consumers:

```java
// Configure the changelog client
ChangelogClientConfig config = new ChangelogClientConfig<>()
    .setStoreName("my-store")
    .setD2Client(d2Client)
    .setControllerD2ServiceName("VeniceController")
    .setConsumerProperties(consumerProperties);

// Create the factory
VeniceChangelogConsumerClientFactory factory =
    new VeniceChangelogConsumerClientFactory(config, metricsRepository);

// Get a changelog consumer for a specific store
VeniceChangelogConsumer<String, MyValue> consumer =
    factory.getChangelogConsumer("my-store");
```

### Subscribing to Changes

Subscribe to specific partitions or all partitions:

```java
// Subscribe to specific partitions
CompletableFuture<Void> future = consumer.subscribe(Set.of(0, 1, 2));
future.get(); // Wait for subscription to complete

// Or subscribe to all partitions
consumer.subscribeAll().get();

// Get the total partition count if needed
int partitionCount = consumer.getPartitionCount();
```

### Polling for Changes

Poll for change events in a loop:

```java
while (true) {
    // Poll with timeout in milliseconds
    Collection<PubSubMessage<String, ChangeEvent<MyValue>, VeniceChangeCoordinate>> messages =
        consumer.poll(1000);

    for (PubSubMessage<String, ChangeEvent<MyValue>, VeniceChangeCoordinate> message : messages) {
        String key = message.getKey();
        ChangeEvent<MyValue> changeEvent = message.getValue();

        // Access the current value (after the change)
        MyValue currentValue = changeEvent.getCurrentValue();

        // Get the checkpoint for this message (for restarts)
        VeniceChangeCoordinate checkpoint = message.getPosition();

        // Process the change...
        processChange(key, currentValue);
    }
}
```

### Seek Operations

Stateless CDC supports multiple seek operations for controlling where consumption starts. **Note:** Stateful CDC does
not support seek operations - it always resumes from its persisted state.

#### Seek to Beginning of Push

Start from the beginning of the current serving version (includes batch push data):

```java
// Seek specific partitions
consumer.seekToBeginningOfPush(Set.of(0, 1, 2)).get();

// Or seek all subscribed partitions
consumer.seekToBeginningOfPush().get();
```

#### Seek to Tail

Start from the current end of the stream (only consume new events):

```java
// Seek specific partitions to tail
consumer.seekToTail(Set.of(0, 1, 2)).get();

// Or seek all subscribed partitions
consumer.seekToTail().get();
```

#### Seek to Timestamp

Resume from a specific wall-clock timestamp. Typically used for restarts when you've checkpointed the timestamp of the
last consumed message:

```java
// During normal consumption, save timestamps from messages
long lastTimestamp = message.getPubSubMessageTime();
// ... persist lastTimestamp for later restart

// On restart, seek to the last consumed timestamp
consumer.seekToTimestamp(lastTimestamp).get();

// Or seek specific partitions to different timestamps
Map<Integer, Long> timestamps = Map.of(
    0, partition0LastTimestamp,
    1, partition1LastTimestamp
);
consumer.seekToTimestamps(timestamps).get();
```

#### Seek to Checkpoint

Resume from a previously saved checkpoint:

```java
// Save checkpoints during consumption
Set<VeniceChangeCoordinate> savedCheckpoints = // ... saved from previous run

// Resume from checkpoints
consumer.seekToCheckpoint(savedCheckpoints).get();
```

### Checkpointing for Restarts

Save checkpoints to resume consumption after application restarts:

```java
Set<VeniceChangeCoordinate> checkpoints = new HashSet<>();

while (true) {
    Collection<PubSubMessage<String, ChangeEvent<MyValue>, VeniceChangeCoordinate>> messages =
        consumer.poll(1000);

    for (PubSubMessage<String, ChangeEvent<MyValue>, VeniceChangeCoordinate> message : messages) {
        // Process the message...

        // Save the checkpoint position
        checkpoints.add(message.getPosition());
    }

    // Periodically persist checkpoints to durable storage for restart
    if (shouldPersistCheckpoints()) {
        persistCheckpoints(checkpoints);
        checkpoints.clear();
    }
}
```

Checkpoints can be serialized to strings for storage:

```java
// Serialize checkpoint to string
String encoded = VeniceChangeCoordinate
    .convertVeniceChangeCoordinateToStringAndEncode(checkpoint);

// Deserialize checkpoint from string
VeniceChangeCoordinate restored = VeniceChangeCoordinate
    .decodeStringAndConvertToVeniceChangeCoordinate(deserializer, encoded);
```

### Monitoring Consumption Progress

Check if the consumer has caught up to the current data:

```java
// Check if all partitions are caught up
boolean caughtUp = consumer.isCaughtUp();

// Get last heartbeat timestamps per partition
Map<Integer, Long> heartbeats = consumer.getLastHeartbeatPerPartition();
```

### Unsubscribing

Stop consuming from partitions:

```java
// Unsubscribe from specific partitions
consumer.unsubscribe(Set.of(0, 1));

// Or unsubscribe from all
consumer.unsubscribeAll();

// Close when done
consumer.close();
```

## Choosing a CDC Client: Stateless vs Stateful

Venice offers two CDC client variants. Choose based on your use case:

| Feature               | Stateless CDC                | Stateful CDC                       |
| --------------------- | ---------------------------- | ---------------------------------- |
| Persistence           | None (streams from Kafka)    | Local RocksDB storage              |
| Checkpoint management | Manual                       | Automatic                          |
| Bootstrap time        | Slower (consumes from Kafka) | Faster (serves from disk)          |
| Seek operations       | Full support                 | Not supported                      |
| Storage requirement   | None                         | SSD recommended                    |
| Best for              | Fine-grained seek control    | Full dataset access, fast restarts |

### Stateless CDC

Use the Stateless CDC client when you need fine-grained control over where to seek in the change stream, or when
operating in a stateless environment:

```java
VeniceChangelogConsumer<String, MyValue> consumer =
    factory.getChangelogConsumer("my-store");
```

**When to use:**

- You only need recent changes, not the full dataset
- You need precise control over seek positions
- Your application is stateless
- You manage checkpoints externally

### Stateful CDC

Use the Stateful CDC client when you need access to the entire Venice dataset with fast bootstrap times:

```java
ChangelogClientConfig config = new ChangelogClientConfig<>()
    .setStoreName("my-store")
    .setD2Client(d2Client)
    .setControllerD2ServiceName("VeniceController")
    .setConsumerProperties(kafkaConsumerProperties)
    .setBootstrapFileSystemPath("/path/to/local/storage");

VeniceChangelogConsumerClientFactory factory =
    new VeniceChangelogConsumerClientFactory(config, metricsRepository);

StatefulVeniceChangelogConsumer<String, MyValue> consumer =
    factory.getStatefulChangelogConsumer("my-store");
```

**When to use:**

- You need the entire Venice dataset
- Fast restart/bootstrap time is critical
- You want automatic checkpoint management

**Key behaviors:**

- Persists consumed records to local RocksDB storage
- Automatically manages checkpoints
- On restart, serves all on-disk records first via `poll()`, then resumes from Kafka at the last processed position
- Provides a compacted view of your data (deduplicates by key)
- Supports [Blob Transfer](../../operations/advanced/p2p-bootstrapping.md) for downloading snapshots from other nodes
  during cold start
- **Does not support seek operations** - consumption always starts from the persisted state

**Storage recommendations:**

- Use SSD storage for optimal performance
- Allocate at least 2.5x the expected dataset size to accommodate both current and future versions during batch pushes

## Configuration Options

Key configuration options for `ChangelogClientConfig`:

| Option                                               | Description                              | Default |
| ---------------------------------------------------- | ---------------------------------------- | ------- |
| `setStoreName(String)`                               | Store name (required)                    | -       |
| `setD2Client(D2Client)`                              | D2 client for service discovery          | -       |
| `setControllerD2ServiceName(String)`                 | D2 service name for controller           | -       |
| `setConsumerProperties(Properties)`                  | Kafka consumer properties                | -       |
| `setViewName(String)`                                | View name for materialized views         | -       |
| `setBootstrapFileSystemPath(String)`                 | Local storage path for stateful consumer | -       |
| `setShouldCompactMessages(boolean)`                  | Enable message compaction                | false   |
| `setMaxBufferSize(int)`                              | Max records buffered before pausing      | 1000    |
| `setVersionSwapDetectionIntervalTimeInSeconds(long)` | Version swap check interval              | 60      |

## Checkpoint Properties

When working with `VeniceChangeCoordinate` checkpoints for application restarts, keep in mind:

- Checkpoints are **partition-specific** and not comparable across partitions
- Checkpoints are **region-specific** and not valid across regions
- Checkpoints are **version-specific** and may become invalid after version swaps
- Checkpoints may expire based on topic retention settings

## Important Considerations

### Threading Model

**Do not use multiple threads to poll on the same CDC instance.** This will cause ordering issues. If your
post-processing logic is slow, use multiple threads for processing the messages returned by `poll()`, not for calling
`poll()` itself.

### Single Instance Pattern

**Do not create multiple CDC client instances for the same store.** The CDC client includes a built-in consumer pool for
parallel consumption. Creating multiple instances will cause ordering issues. A single instance is sufficient.

### Event Consistency

- **Across regions**: Event counts may differ between regions in Active/Active deployments. An intermediate state in one
  region might be the final state, causing different event counts.
- **Within a region**: Event counts are consistent within a single region.

### Timestamp Semantics

The `seekToTimestamps` method uses the time when the **Venice server processed** each message, not the producer
timestamp provided when writing to Venice. Use `getPubSubMessageTime()` from the returned messages to get this
timestamp.

Be aware that during version pushes, timestamps are relative to the push time. If you seek to a timestamp older than the
current version's push time, you will consume all events in that version.

## Best Practices

1. **Handle version swaps**: The CDC client automatically handles version swaps during batch pushes. It consumes the
   future version in the background and swaps at roughly the same position. This is best-effort, so your application
   should tolerate temporary duplicate records.

2. **Checkpoint regularly**: For Stateless CDC, persist checkpoints or timestamps periodically to minimize reprocessing
   after restarts. Stateful CDC manages checkpoints automatically.

3. **Monitor lag**: Set up alerting on lag metrics to detect when the CDC client isn't making progress.

4. **Handle null current values**: Deletions will have null `getCurrentValue()`.

5. **Restart after batch push if you need batch data**: During a version swap, the CDC client attempts to continue from
   roughly the same position. If you need to consume the batch push data, restart the client after the push completes.

## Frequently Asked Questions

### What happens during a batch push?

The CDC client starts consuming the future version in the background. Once it catches up, it attempts to swap to the
future version at roughly the same position you were consuming on the current version. This swap is best effort, so you
may temporarily see duplicate records and your application should tolerate them.

If you need the batch data, restart the client after the batch push completes.

### Does it handle chunking/compression?

Yes, but only records that are part of a batch push will be chunked/compressed. Records from nearline/real-time updates
are not chunked/compressed.

### Does it support Protobuf?

No, the CDC client supports deserializing records into Avro Generic or Specific Records only.

### Why is my CDC client lagging?

Common reasons for CDC client lag:

1. Large increase in nearline/real-time updates that the client can't keep up with
2. For Stateful CDC: disk may be full and the client cannot write to disk
3. A background thread may have died (restart can mitigate)

Monitor the heartbeat delay metric to detect lag issues early.
