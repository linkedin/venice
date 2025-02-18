package com.linkedin.venice.hadoop.task.datawriter;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.ComplexVeniceWriter;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.PutMetadata;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;


/**
 * Adapter class for {@link ComplexVeniceWriter} to support public APIs defined in {@link AbstractVeniceWriter} in the
 * context of being called in a {@link com.linkedin.venice.writer.CompositeVeniceWriter} from VPJ. This class will
 * provide capabilities to deserialize the value in order to provide {@link ComplexVeniceWriter} a value provider, and
 * decompression capabilities in case of a re-push (Kafka input).
 */
public class ComplexVeniceWriterAdapter<K, V, U> extends AbstractVeniceWriter<K, V, U> {
  private final ComplexVeniceWriter<K, V, U> internalVeniceWriter;
  private final BiFunction<V, Integer, GenericRecord> deserializeFunction;
  private final Function<V, V> decompressFunction;

  public ComplexVeniceWriterAdapter(
      String topicName,
      ComplexVeniceWriter<K, V, U> veniceWriter,
      BiFunction<V, Integer, GenericRecord> deserializeFunction,
      Function<V, V> decompressFunction) {
    super(topicName);
    this.internalVeniceWriter = veniceWriter;
    this.deserializeFunction = deserializeFunction;
    this.decompressFunction = decompressFunction;
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

  /**
   * The {@link PubSubProduceResult} will always be null and should not be used. This is acceptable because:
   *   1. {@link ComplexVeniceWriter#complexPut(Object, Object, int, Lazy)} returns a CompletableFuture with Void
   *   since it could potentially write to multiple partitions resulting in multiple PubSubProduceResult.
   *   2. Only the PubSubProduceResult of the main writer in {@link com.linkedin.venice.writer.CompositeVeniceWriter} is
   *   used for reporting purpose in VPJ.
   */
  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    CompletableFuture<PubSubProduceResult> wraper = new CompletableFuture<>();
    Lazy<GenericRecord> valueProvider =
        Lazy.of(() -> deserializeFunction.apply(decompressFunction.apply(value), valueSchemaId));
    internalVeniceWriter.complexPut(key, value, valueSchemaId, valueProvider)
        .whenCompleteAsync((ignored, writeException) -> {
          if (writeException == null) {
            wraper.complete(null);
          } else {
            wraper.completeExceptionally(writeException);
          }
        });
    return wraper;
  }

  /**
   * In VPJ, only re-push can trigger this function. During re-push the deletion to view topics are useless and should
   * be ignored.
   */
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
