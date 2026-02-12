# Fast Client

The Fast Client is Venice's high-performance read client. It's partition-aware, routes requests directly to servers
(bypassing the Router), and provides lower latency than the Thin Client.

## Characteristics

- **Partition-aware**: Maintains metadata to route directly to the correct server
- **Network hops**: 1 (client → server)
- **Typical latency**: < 2ms
- **Best for**: Applications requiring low latency with moderate resource overhead

## When to Use

Choose the Fast Client when:

- You need lower latency than the Thin Client (< 2ms vs < 10ms)
- You can accept some additional memory overhead for metadata caching
- Your application makes frequent reads and benefits from direct server routing
- You want built-in long-tail retry and load control features

For the lowest latency, consider the [Da Vinci Client](da-vinci-client.md) (0 hops, < 1ms). For simplicity, consider the
[Thin Client](thin-client.md).

## Usage

### Dependency

Add the Venice client dependency to your project:

```groovy
dependencies {
  implementation 'com.linkedin.venice:venice-client:<version>'
}
```

### Creating a Client

Use `ClientFactory` from the fastclient package:

```java
// D2 client for service discovery (required)
D2Client d2Client = // ... your D2 client setup

// Create client configuration
ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    .setStoreName("my-store")
    .setR2Client(d2Client)
    .setD2Client(d2Client)
    .setClusterDiscoveryD2Service("VeniceController")
    .build();

// Create and start the client
AvroGenericStoreClient<String, MyValue> client =
    ClientFactory.getAndStartGenericStoreClient(clientConfig);
```

### Reading Data

The Fast Client implements the same `AvroGenericStoreClient` interface as the Thin Client:

#### Single Get

```java
// Asynchronous single-key lookup
CompletableFuture<MyValue> future = client.get("my-key");
MyValue value = future.get();
```

#### Batch Get

```java
Set<String> keys = Set.of("key1", "key2", "key3");
Map<String, MyValue> results = client.batchGet(keys).get();
```

### Using Specific Records

```java
ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<String, MyValueRecord, MyValueRecord>()
    .setStoreName("my-store")
    .setR2Client(d2Client)
    .setD2Client(d2Client)
    .setClusterDiscoveryD2Service("VeniceController")
    .setSpecificValueClass(MyValueRecord.class)
    .build();

AvroSpecificStoreClient<String, MyValueRecord> client =
    ClientFactory.getAndStartSpecificStoreClient(clientConfig);
```

## Key Features

### Long-Tail Retry

The Fast Client supports automatic retry for slow requests (long-tail latency):

```java
ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    .setStoreName("my-store")
    .setR2Client(d2Client)
    .setD2Client(d2Client)
    .setClusterDiscoveryD2Service("VeniceController")
    // Enable long-tail retry for single get
    .setLongTailRetryEnabledForSingleGet(true)
    .setLongTailRetryThresholdForSingleGetInMicroSeconds(5000) // 5ms
    // Enable for batch get with range-based thresholds
    .setLongTailRetryEnabledForBatchGet(true)
    .build();
```

### Retry Budget

Control retry rate to prevent overloading servers:

```java
ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    // ... other config ...
    .setRetryBudgetEnabled(true)
    .setRetryBudgetPercentage(10.0) // Allow 10% of requests to retry
    .build();
```

### Load Control

Automatic request rejection when servers are overloaded:

```java
ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    // ... other config ...
    .setStoreLoadControllerEnabled(true)
    .setStoreLoadControllerMaxRejectionRatio(0.5) // Max 50% rejection
    .build();
```

### Dual Read Mode

Run Fast Client alongside Thin Client for gradual migration:

```java
// Create a Thin Client first
AvroGenericStoreClient<String, MyValue> thinClient = // ... create thin client

ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    // ... other config ...
    .setDualReadEnabled(true)
    .setGenericThinClient(thinClient)
    .build();

// Dual read: Fast Client handles reads, Thin Client used for comparison/fallback
AvroGenericStoreClient<String, MyValue> client =
    ClientFactory.getAndStartGenericStoreClient(clientConfig);
```

## Configuration Options

Key configuration options for `ClientConfig`:

| Option                                         | Description                                     | Default |
| ---------------------------------------------- | ----------------------------------------------- | ------- |
| `setStoreName(String)`                         | Store name (required)                           | -       |
| `setR2Client(Client)`                          | R2 client for HTTP (required unless using gRPC) | -       |
| `setD2Client(D2Client)`                        | D2 client for service discovery                 | -       |
| `setClusterDiscoveryD2Service(String)`         | D2 service name for controller                  | -       |
| `setMetadataRefreshIntervalInSeconds(long)`    | Metadata refresh interval                       | 60      |
| `setLongTailRetryEnabledForSingleGet(boolean)` | Enable retry for single get                     | false   |
| `setLongTailRetryEnabledForBatchGet(boolean)`  | Enable retry for batch get                      | false   |
| `setRetryBudgetEnabled(boolean)`               | Enable retry budget                             | false   |
| `setRetryBudgetPercentage(double)`             | Max retry rate percentage                       | 10.0    |
| `setStoreLoadControllerEnabled(boolean)`       | Enable load control                             | false   |

### Routing Strategies

The Fast Client supports different routing strategies:

```java
ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    // ... other config ...
    .setClientRoutingStrategyType(ClientRoutingStrategyType.LEAST_LOADED)
    .build();
```

Available strategies:

- `LEAST_LOADED`: Route to the server with the least pending requests
- `HELIX_ASSISTED`: Use Helix routing information

## gRPC Support

The Fast Client supports gRPC transport for potentially lower latency:

```java
GrpcClientConfig grpcConfig = new GrpcClientConfig.Builder()
    .setNettyServerToGrpcAddressMap(serverToGrpcMap)
    .build();

ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    .setStoreName("my-store")
    .setUseGrpc(true)
    .setGrpcClientConfig(grpcConfig)
    .build();
```

## Health Monitoring

The Fast Client includes instance health monitoring to avoid unhealthy servers:

```java
InstanceHealthMonitorConfig healthConfig = new InstanceHealthMonitorConfig.Builder()
    .setBlockedInstanceMaxBackoffMs(30000)
    .setBlockedInstanceMinBackoffMs(1000)
    .build();

ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    // ... other config ...
    .setInstanceHealthMonitorConfig(healthConfig)
    .build();
```

## Best Practices

1. **Configure D2 properly**: The Fast Client relies on D2 for metadata discovery
2. **Enable retry for critical paths**: Long-tail retry improves P99 latency
3. **Use retry budget**: Prevents retry storms under load
4. **Monitor health metrics**: The Fast Client exposes metrics for monitoring
5. **Start with conservative settings**: Tune retry thresholds based on observed latency

## Metrics

The Fast Client exposes metrics through `FastClientStats`:

- Request latency (P50, P95, P99)
- Request count by type (single get, batch get, compute)
- Retry count and success rate
- Instance health status
