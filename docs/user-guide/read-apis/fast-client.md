# Fast Client

The Fast Client is Venice's high-performance read client that routes requests directly to servers (bypassing the
Router), and provides lower latency than the Thin Client.

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
// Create R2 client for HTTP communication
HttpClientFactory httpClientFactory = new HttpClientFactory.Builder()
    .setUsePipelineV2(true) // Enable HTTP/2
    .build();
Client r2Client = new TransportClientAdapter(
    httpClientFactory.getClient(Collections.emptyMap())
);

// Create D2 client for service discovery
D2Client d2Client = new D2ClientBuilder()
    .setZkHosts("zookeeper.example.com:2181") // Your ZooKeeper hosts
    .build();
D2ClientUtils.startClient(d2Client); // Start the D2 client

// Create Fast Client configuration
ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    .setStoreName("my-store")
    .setR2Client(r2Client)
    .setD2Client(d2Client)
    .setClusterDiscoveryD2Service("VeniceController")
    .build();

// Create and start the Fast Client
AvroGenericStoreClient<String, MyValue> client =
    ClientFactory.getAndStartGenericStoreClient(clientConfig);
```

**With SSL**, add SSL properties to the R2 and D2 clients:

```java
// R2 client: Add SSL properties to httpClientFactory.getClient()
Map<String, Object> sslProperties = new HashMap<>();
sslProperties.put(HttpClientFactory.HTTP_SSL_CONTEXT, sslFactory.getSSLContext());
sslProperties.put(HttpClientFactory.HTTP_SSL_PARAMS, sslFactory.getSSLParameters());
Client r2Client = new TransportClientAdapter(
    httpClientFactory.getClient(sslProperties) // Use sslProperties instead of Collections.emptyMap()
);

// D2 client: Add SSL configuration to builder
D2Client d2Client = new D2ClientBuilder()
    .setZkHosts("zookeeper.example.com:2181")
    .setIsSSLEnabled(true)
    .setSSLContext(sslFactory.getSSLContext())
    .setSSLParameters(sslFactory.getSSLParameters())
    .build();
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

For type-safe access with Avro-generated classes:

```java
ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<String, MyValueRecord, MyValueRecord>()
    // ... basic config ...
    .setSpecificValueClass(MyValueRecord.class)
    .build();

AvroSpecificStoreClient<String, MyValueRecord> client =
    ClientFactory.getAndStartSpecificStoreClient(clientConfig);
```

## Key Features

### Long-Tail Retry

Automatically retry slow requests to improve P99 latency:

```java
ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    // ... basic config ...
    .setLongTailRetryEnabledForSingleGet(true)
    .setLongTailRetryThresholdForSingleGetInMicroSeconds(5000) // 5ms
    .setLongTailRetryEnabledForBatchGet(true)
    .build();
```

### Advanced Configuration

Configure retry budget, load control, and dual-read mode:

```java
ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    // ... basic config ...
    // Retry budget (enabled by default) - prevents retry storms
    .setRetryBudgetEnabled(true)
    .setRetryBudgetPercentage(0.1) // Allow 10% of requests to retry
    // Load control - automatic request rejection when overloaded
    .setStoreLoadControllerEnabled(true)
    .setStoreLoadControllerMaxRejectionRatio(0.5) // Max 50% rejection
    // Dual read - run alongside Thin Client for gradual migration
    .setDualReadEnabled(true)
    .setGenericThinClient(thinClient) // Your existing Thin Client instance
    .build();
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
| `setRetryBudgetEnabled(boolean)`               | Enable retry budget                             | true    |
| `setRetryBudgetPercentage(double)`             | Max retry rate percentage (0.0-1.0)             | 0.1     |
| `setStoreLoadControllerEnabled(boolean)`       | Enable load control                             | false   |

### Routing Strategies

Configure different routing strategies:

```java
ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    // ... basic config ...
    .setClientRoutingStrategyType(ClientRoutingStrategyType.LEAST_LOADED)
    .build();
```

Available strategies:

- `LEAST_LOADED`: Route to the server with the least pending requests
- `HELIX_ASSISTED`: Use Helix routing information

## gRPC Support

Enable gRPC transport for potentially lower latency:

```java
GrpcClientConfig grpcConfig = new GrpcClientConfig.Builder()
    .setR2Client(r2Client) // Required for non-storage requests
    .setNettyServerToGrpcAddress(serverToGrpcMap)
    .build();

ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    // ... basic config ...
    .setUseGrpc(true)
    .setGrpcClientConfig(grpcConfig)
    .build();
```

## Health Monitoring

Configure instance health monitoring to avoid unhealthy servers:

```java
InstanceHealthMonitorConfig healthConfig = new InstanceHealthMonitorConfig.Builder()
    .setClient(r2Client)
    .setRoutingRequestDefaultTimeoutMS(1000)
    .setRoutingPendingRequestCounterInstanceBlockThreshold(50)
    .setHeartBeatIntervalSeconds(30)
    .setHeartBeatRequestTimeoutMS(10000)
    .build();

ClientConfig clientConfig = new ClientConfig.ClientConfigBuilder<>()
    // ... basic config ...
    .setInstanceHealthMonitor(new InstanceHealthMonitor(healthConfig))
    .build();
```

## Client Lifecycle

### Closing the Client

Always close clients properly to release resources:

```java
// Close the Venice client
client.close();

// Shutdown D2 client
D2ClientUtils.shutdownClient(d2Client);

// Shutdown R2 client
r2Client.shutdown(null);
```

## Best Practices

1. **Configure D2 properly**: The Fast Client relies on D2 for metadata discovery
2. **Enable retry for critical paths**: Long-tail retry improves P99 latency
3. **Use retry budget**: Prevents retry storms under load
4. **Monitor health metrics**: The Fast Client exposes metrics for monitoring
5. **Start with conservative settings**: Tune retry thresholds based on observed latency
6. **Always close clients**: Properly shutdown all clients to prevent resource leaks
