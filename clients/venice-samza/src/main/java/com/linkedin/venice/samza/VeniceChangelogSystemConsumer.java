package com.linkedin.venice.samza;

import com.linkedin.davinci.consumer.*;
import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;


public class VeniceChangelogSystemConsumer<K, V> implements SystemConsumer, Closeable {
  private static final Logger logger = LogManager.getLogger(VeniceChangelogSystemConsumer.class);
  private final VeniceChangelogConsumer<K, V> consumer;
  // Reusable byte array buffers for serialization/deserialization of offset objects to reduce GC pressure
  private static final ThreadLocal<ByteArrayOutputStream> _byteArrayOutputStream =
      ThreadLocal.withInitial(ByteArrayOutputStream::new);

  public VeniceChangelogSystemConsumer(VeniceChangelogConsumer consumer) {
    // TODO: Determine if this is kosher
    this.consumer = consumer;
  }

  private static final ThreadLocal<ObjectOutputStream> _objectOutputStream = ThreadLocal.withInitial(() -> {
    try {
      return new ObjectOutputStream(_byteArrayOutputStream.get());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  });

  protected static String convertVeniceChangeCoordinateToStringAndEncode(VeniceChangeCoordinate veniceChangeCoordinate)
      throws IOException {
    veniceChangeCoordinate.writeExternal(_objectOutputStream.get());
    _objectOutputStream.get().flush();
    _objectOutputStream.get().close();
    byte[] data = _byteArrayOutputStream.get().toByteArray();
    return Base64.getEncoder().encodeToString(data);
  }

  protected static VeniceChangeCoordinate decodeStringAndConvertToVeniceChangeCoordinate(String offsetString)
      throws IOException, ClassNotFoundException {
    byte[] newData = Base64.getDecoder().decode(offsetString);
    ByteArrayInputStream inMemoryInputStream = new ByteArrayInputStream(newData);
    ObjectInputStream objectInputStream = new ObjectInputStream(inMemoryInputStream);
    VeniceChangeCoordinate restoredCoordinate = new VeniceChangeCoordinate();
    restoredCoordinate.readExternal(objectInputStream);
    return restoredCoordinate;
  }

  @Override
  public void close() throws IOException {
    consumer.close();
  }

  @Override
  public void start() {
    // NoOp
  }

  @Override
  public void stop() {
    // NoOp
  }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String offset) {
    try {
      consumer.subscribe(Collections.singleton(systemStreamPartition.getPartition().getPartitionId())).get();
      // Only seek if a checkpoint is provided
      if (offset != null && !offset.isEmpty()) {
        consumer.seekToCheckpoint(Collections.singleton(decodeStringAndConvertToVeniceChangeCoordinate(offset))).get();
      }
    } catch (InterruptedException | ExecutionException | ClassNotFoundException | IOException e) {
      throw new VeniceException("Failed to subscribe to partition" + systemStreamPartition.getPartition(), e);
    }
  }

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
      Set<SystemStreamPartition> systemStreamPartitions,
      long timeout) throws InterruptedException {

    // consumer.pause();
    HashMap<Integer, SystemStreamPartition> partitionsToPoll = new HashMap<>();
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> transformedResult = new HashMap<>();
    for (SystemStreamPartition systemStreamPartition: systemStreamPartitions) {
      partitionsToPoll.put(systemStreamPartition.getPartition().getPartitionId(), systemStreamPartition);
      transformedResult.put(systemStreamPartition, new ArrayList<>());
    }

    // consumer.resume(partitionsToPoll.keySet());

    try {
      Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> results = consumer.poll(timeout);
      for (PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> message: results) {

        IncomingMessageEnvelope incomingMessageEnvelope = new IncomingMessageEnvelope(
            partitionsToPoll.get(message.getPartition()),
            convertVeniceChangeCoordinateToStringAndEncode(message.getOffset()),
            message.getKey(),
            message.getValue(),
            message.getPayloadSize(),
            message.getPubSubMessageTime(),
            Instant.now().toEpochMilli());

        transformedResult.get(partitionsToPoll.get(message.getPartition())).add(incomingMessageEnvelope);
      }
      return transformedResult;
    } catch (Exception e) {
      // This is stolen from brooklin's System Consumer, but it has the smell of post-mortem action item,
      // so we're gonna put it here too.
      logger.error("Received error on poll!", e);
      try {
        // Samza poll interval is pretty small. Add some extra sleep to avoid overwhelming Kafka and logs.
        Thread.sleep(10000L);
      } catch (InterruptedException p) {
        logger.warn("Interrupted while Sleeping on Exception: ", p);
      }
    }
    return Collections.emptyMap();
  }
}
