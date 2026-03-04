# Stream Processor

The stream processor enable nearline data ingestion into Venice with automatic checkpointing and at-least-once delivery
semantics. The best supported stream processor is Apache Samza, with integration provided through the `venice-samza`
module.

## When to Use

Choose the stream processor when you need:

- **At-least-once delivery with idempotent writes** - Samza's checkpointing ensures all records are processed, and
  Venice's timestamp-based conflict resolution provides idempotency when using deterministic logical timestamps
- **Stateful stream processing** - Transform and enrich data before writing to Venice
- **Hybrid stores with nearline updates** - Combine batch push with nearline streaming updates
- **Automatic checkpointing** - Built-in progress tracking relative to upstream data sources

**Note:** Use stream processors when you want to leverage Samza's framework capabilities (stateful processing,
windowing, joins, automatic checkpointing). If you prefer to write Venice integration code directly in your application
without a stream processing framework, use the [Online Producer](online-producer.md) instead.

## Prerequisites

To write to Venice from a stream processor, the store must be configured as a **hybrid store** with:

1. **Hybrid store enabled with a rewind time** - The rewind time controls how far back nearline writes are replayed on
   top of a new batch version during a version push. For example, a 24-hour rewind time means the last 24 hours of
   nearline data is replayed when a new batch version is pushed.
2. **Current version capable of receiving nearline writes** - The active version must be hybrid-enabled

## Apache Samza Integration

### Dependency

Add the Venice Samza integration dependency to your Samza job:

```groovy
dependencies {
  implementation 'com.linkedin.venice:venice-samza:<version>'
}
```

### Integration Overview

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
import com.linkedin.venice.samza.VeniceObjectWithTimestamp;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.MessageCollector;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

public class MyStreamTask implements StreamTask {

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    // Process your data
    String key = processKey(envelope);
    GenericRecord value = processValue(envelope);

    // Send to Venice (without explicit timestamp - Venice will use producer timestamp)
    OutgoingMessageEnvelope out = new OutgoingMessageEnvelope(new SystemStream("venice", "my-store-name"), key, value);
    collector.send(out);
  }
}

```

### Using Logical Timestamps for Idempotent Writes

For idempotent writes across restarts, wrap your values with a deterministic logical timestamp:

```java
import com.linkedin.venice.samza.VeniceObjectWithTimestamp;

public class MyStreamTask implements StreamTask {

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    String key = processKey(envelope);
    GenericRecord value = processValue(envelope);

    // Use a deterministic timestamp (e.g., from the source event)
    // DO NOT use System.currentTimeMillis() as it will differ on replay
    long logicalTimestamp = envelope.getEventTime();

    // Wrap value with timestamp for idempotent writes
    VeniceObjectWithTimestamp valueWithTimestamp = new VeniceObjectWithTimestamp(value, logicalTimestamp);

    OutgoingMessageEnvelope out = new OutgoingMessageEnvelope(
      new SystemStream("venice", "my-store-name"),
      key,
      valueWithTimestamp // Use wrapper instead of raw value
    );
    collector.send(out);
  }
}

```

**Important:** The logical timestamp must be deterministic. If you replay a record, it must produce the same timestamp,
otherwise you'll lose idempotency and get duplicate entries in Venice.

## Delivery Semantics

### At-Least-Once Delivery

The Samza-Venice integration provides **at-least-once delivery semantics**:

- After a Samza job restart, records that were written to Venice between the last checkpoint and the failure will be
  replayed
- This can result in duplicate writes to the Venice topic (Kafka)
- Venice handles these duplicates through **timestamp-based conflict resolution** (last-write-wins)

### Achieving Idempotent Writes

To ensure replayed writes are idempotent:

1. **Use deterministic logical timestamps** - Wrap values with `VeniceObjectWithTimestamp` using timestamps derived from
   the input event (see example above), NOT `System.currentTimeMillis()`
2. **Deterministic transformations** - Ensure your Samza processing logic produces the same output for the same input.
   If your code reads from Venice to calculate a new value (e.g., read counter, increment, write back), replays will
   read different values and produce non-idempotent results.
