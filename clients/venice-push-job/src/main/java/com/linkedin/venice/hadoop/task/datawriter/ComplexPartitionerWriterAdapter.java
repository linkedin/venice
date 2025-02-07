package com.linkedin.venice.hadoop.task.datawriter;

import com.linkedin.venice.partitioner.VeniceComplexPartitioner;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;


public class ComplexPartitionerWriterAdapter<K, V, U> extends AbstractVeniceWriter<K, V, U> {
  private final VeniceWriter<K, V, U> internalVeniceWriter;
  private final VeniceComplexPartitioner complexPartitioner;
  private final int numPartitions;
  private final Function<V, GenericRecord> deserializer;

  public ComplexPartitionerWriterAdapter(
      String topicName,
      VeniceWriter<K, V, U> veniceWriter,
      VeniceComplexPartitioner complexPartitioner,
      int numPartitions,
      Function<V, GenericRecord> deserializer) {
    super(topicName);
    this.internalVeniceWriter = veniceWriter;
    this.complexPartitioner = complexPartitioner;
    this.deserializer = deserializer;
    this.numPartitions = numPartitions;
  }

  @Override
  public void close(boolean gracefulClose) throws IOException {
    // no op, internal writer is initialized outside the adapter and should be closed elsewhere.
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback) {
    return put(key, value, valueSchemaId, callback, null);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    return internalVeniceWriter.writeWithComplexPartitioner(
        key,
        value,
        valueSchemaId,
        Lazy.of(() -> deserializer.apply(value)),
        complexPartitioner,
        numPartitions);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      DeleteMetadata deleteMetadata) {
    // No-op, delete by key is undefined for complex partitioner.
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public Future<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      PubSubProducerCallback callback) {
    throw new UnsupportedOperationException(this.getClass().getSimpleName() + " does not support update function");
  }

  @Override
  public void flush() {
    internalVeniceWriter.flush();
  }

  @Override
  public void close() throws IOException {
    close(true);
  }
}
