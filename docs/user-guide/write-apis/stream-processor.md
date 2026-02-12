# Stream Processor

Stream processors enable nearline data ingestion into Venice with automatic checkpointing and exactly-once semantics.
The best supported stream processor is Apache Samza, with integration provided through the `venice-samza` module.

## When to Use

Choose stream processors when you need:

- **Exactly-once processing guarantees** - Samza's checkpointing ensures no duplicate writes after restarts
- **Stateful stream processing** - Transform and enrich data before writing to Venice
- **Hybrid stores with nearline updates** - Combine batch push with real-time streaming updates
- **Automatic checkpointing** - Built-in progress tracking relative to upstream data sources

For simpler use cases without stream processing logic, consider the [Online Producer](online-producer.md).

## Prerequisites

To write to Venice from a stream processor, the store must be configured as a **hybrid store** with:

1. Hybrid store enabled with a rewind time
2. Current version capable of receiving nearline writes
3. Either `ACTIVE_ACTIVE` or `NON_AGGREGATE` replication policy

## Apache Samza Integration

Venice integrates with Apache Samza through
[VeniceSystemProducer](https://github.com/linkedin/venice/blob/main/integrations/venice-samza/src/main/java/com/linkedin/venice/samza/VeniceSystemProducer.java)
and
[VeniceSystemFactory](https://github.com/linkedin/venice/blob/main/integrations/venice-samza/src/main/java/com/linkedin/venice/samza/VeniceSystemFactory.java).

### Configuration

Configure Venice as an output system in your Samza job properties file:

```properties
# Define Venice as an output system
systems.venice.samza.factory=com.linkedin.venice.samza.VeniceSystemFactory

# Required: Store name to write to
systems.venice.store=my-store-name

# Required: Push type
systems.venice.push.type=STREAM

# Required: Venice controller discovery URL
systems.venice.venice.controller.discovery.url=http://controller.host:1234
```

### Writing Data

Once configured, write to Venice using Samza's `MessageCollector`:

```java
public class MyStreamTask implements StreamTask {

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    // Process your data
    String key = processKey(envelope);
    GenericRecord value = processValue(envelope);

    // Send to Venice
    OutgoingMessageEnvelope out = new OutgoingMessageEnvelope(new SystemStream("venice", "my-store-name"), key, value);

    collector.send(out);
  }
}

```

## Best Practices

1. **Monitor Samza lag metrics** - Ensure your stream processor keeps up with upstream data
2. **Configure appropriate buffer sizes** - Balance memory usage and throughput via Kafka producer configs
3. **Handle schema evolution** - Ensure your Samza job can handle multiple value schema versions
4. **Set appropriate rewind time** - Configure hybrid store rewind time based on expected downtime and reprocessing
   needs
