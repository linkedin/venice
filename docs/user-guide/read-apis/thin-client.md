# Thin Client

The Thin Client is Venice's simplest read client. It's stateless, routes requests through the Venice Router, and
requires minimal configuration.

## Characteristics

- **Stateless**: No local data storage, minimal memory footprint
- **Network hops**: 2 (client → router → server)
- **Typical latency**: < 10ms
- **Best for**: Applications with moderate latency requirements or limited local resources

## When to Use

Choose the Thin Client when:

- You need a simple, low-maintenance client
- Your application can tolerate ~10ms read latency
- You don't want to manage local storage or metadata
- You're getting started with Venice and want the easiest integration

For lower latency requirements, consider the [Fast Client](fast-client.md) (1 hop, < 2ms) or
[Da Vinci Client](da-vinci-client.md) (0 hops, < 1ms).

## Usage

### Dependency

Add the Venice client dependency to your project:

```groovy
dependencies {
  implementation 'com.linkedin.venice:venice-client:<version>'
}
```

### Creating a Client

Use `ClientFactory` to create a Thin Client:

```java
// Create client configuration
ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig("my-store")
    .setVeniceURL("http://venice-router.example.com:7777");

// Create and start the client
AvroGenericStoreClient<String, MyValue> client = ClientFactory.getAndStartGenericStoreClient(clientConfig);
```

### Reading Data

#### Single Get

```java
// Asynchronous single-key lookup
CompletableFuture<MyValue> future = client.get("my-key");
MyValue value = future.get(); // Blocks until complete

// Returns null if key doesn't exist
if (value == null) {
    System.out.println("Key not found");
}
```

#### Batch Get

```java
Set<String> keys = Set.of("key1", "key2", "key3");

// Asynchronous batch lookup
CompletableFuture<Map<String, MyValue>> future = client.batchGet(keys);
Map<String, MyValue> results = future.get();

// Only contains entries for keys that exist
for (Map.Entry<String, MyValue> entry : results.entrySet()) {
    System.out.println(entry.getKey() + " = " + entry.getValue());
}
```

#### Streaming Batch Get

For large batch requests, use the streaming API to receive results as they arrive:

```java
Set<String> keys = Set.of("key1", "key2", "key3");

// Streaming callback for real-time results
client.streamingBatchGet(keys, new StreamingCallback<String, MyValue>() {
    @Override
    public void onRecordReceived(String key, MyValue value) {
        // Called for each key-value pair as it arrives
        System.out.println("Received: " + key);
    }

    @Override
    public void onCompletion(Optional<Exception> exception) {
        if (exception.isPresent()) {
            System.err.println("Error: " + exception.get());
        } else {
            System.out.println("All records received");
        }
    }
});
```

### Using Specific Records

If you have generated Avro SpecificRecord classes:

```java
ClientConfig clientConfig = ClientConfig.defaultSpecificClientConfig("my-store", MyValueRecord.class)
    .setVeniceURL("http://venice-router.example.com:7777");

AvroSpecificStoreClient<String, MyValueRecord> client =
    ClientFactory.getAndStartSpecificStoreClient(clientConfig);

MyValueRecord value = client.get("my-key").get();
```

### Closing the Client

Always close the client when done to release resources:

```java
client.close();
```

## Read Compute

The Thin Client supports Read Compute for server-side operations:

```java
Set<String> keys = Set.of("key1", "key2", "key3");

// Project specific fields and compute dot product
client.compute()
    .project("field1", "field2")
    .dotProduct("embedding", queryVector, "similarity")
    .execute(keys)
    .whenComplete((results, error) -> {
        if (error != null) {
            System.err.println("Compute failed: " + error);
        } else {
            results.forEach((key, record) -> {
                System.out.println(key + " similarity: " + record.get("similarity"));
            });
        }
    });
```

## Configuration Options

Key configuration options for `ClientConfig`:

| Option                           | Description                     | Default |
| -------------------------------- | ------------------------------- | ------- |
| `setVeniceURL(String)`           | Router URL (required)           | -       |
| `setRequestTimeoutMs(long)`      | Request timeout in milliseconds | 5000    |
| `setMaxConnectionsPerRoute(int)` | Max HTTP connections per route  | 10      |
| `setMaxConnectionsTotal(int)`    | Total max HTTP connections      | 100     |

## Error Handling

The Thin Client throws `VeniceClientException` for errors:

```java
try {
    MyValue value = client.get("my-key").get();
} catch (ExecutionException e) {
    if (e.getCause() instanceof VeniceClientHttpException) {
        VeniceClientHttpException httpEx = (VeniceClientHttpException) e.getCause();
        System.err.println("HTTP error: " + httpEx.getHttpStatus());
    } else if (e.getCause() instanceof VeniceClientRateExceededException) {
        System.err.println("Rate limit exceeded, implement backoff");
    }
}
```

## Best Practices

1. **Reuse clients**: Create one client per store and reuse it across requests
2. **Use batch gets**: For multiple keys, batch get is more efficient than multiple single gets
3. **Handle timeouts**: Configure appropriate timeouts for your latency requirements
4. **Close clients**: Always close clients in a finally block or use try-with-resources
