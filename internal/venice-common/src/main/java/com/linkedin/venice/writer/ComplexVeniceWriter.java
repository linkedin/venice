package com.linkedin.venice.writer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.partitioner.ComplexVenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;


/**
 * Provide more complex and sophisticated writer APIs for writing to {@link com.linkedin.venice.views.MaterializedView}.
 * Specifically when a {@link ComplexVenicePartitioner} is involved. Otherwise, use the
 * {@link VeniceWriter} APIs.
 */
public class ComplexVeniceWriter<K, V, U> extends VeniceWriter<K, V, U> {
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  private static final String SKIP_LARGE_RECORD = "SkipLargeRecord";
  private final ComplexVenicePartitioner complexPartitioner;
  private final AtomicLong skippedLargeRecords = new AtomicLong(0);

  public ComplexVeniceWriter(
      VeniceWriterOptions params,
      VeniceProperties props,
      PubSubProducerAdapter producerAdapter) {
    super(params, props, producerAdapter);
    if (partitioner instanceof ComplexVenicePartitioner) {
      complexPartitioner = (ComplexVenicePartitioner) partitioner;
    } else {
      complexPartitioner = null;
    }
  }

  /**
   * {@link ComplexVenicePartitioner} offers a more sophisticated getPartitionId API. It also takes value as a
   * parameter, and could return a single, multiple or no partition(s).
   */
  public CompletableFuture<Void> complexPut(K key, V value, int valueSchemaId, Lazy<GenericRecord> valueProvider) {
    CompletableFuture<Void> finalCompletableFuture = new CompletableFuture<>();
    if (value == null) {
      // Ignore null value
      throw new VeniceException("Put value should not be null");
    } else {
      // Write updated/put record to materialized view topic partition(s)
      if (complexPartitioner == null) {
        // No VeniceComplexPartitioner involved, perform simple put.
        byte[] serializedKey = keySerializer.serialize(topicName, key);
        byte[] serializedValue = valueSerializer.serialize(topicName, value);
        int partition = getPartition(serializedKey);
        propagateVeniceWriterFuture(
            put(serializedKey, serializedValue, valueSchemaId, partition),
            finalCompletableFuture);
      } else {
        byte[] serializedKey = keySerializer.serialize(topicName, key);
        int[] partitions = complexPartitioner.getPartitionId(serializedKey, valueProvider.get(), numberOfPartitions);
        if (partitions.length == 0) {
          finalCompletableFuture.complete(null);
        } else {
          byte[] serializedValue = valueSerializer.serialize(topicName, value);
          performMultiPartitionAction(
              partitions,
              finalCompletableFuture,
              (partition) -> this.put(serializedKey, serializedValue, valueSchemaId, partition));
        }
      }
    }
    return finalCompletableFuture;
  }

  /**
   * Perform "delete" on the given key. If a {@link ComplexVenicePartitioner} is involved then it will be a best effort
   * attempt to delete the record using the valueProvider. It's best effort because:
   *   1. Nothing we can do if value is null or not provided via valueProvider.
   *   2. Previous writes
   */
  public CompletableFuture<Void> complexDelete(K key, Lazy<GenericRecord> valueProvider) {
    CompletableFuture<Void> finalCompletableFuture = new CompletableFuture<>();
    if (complexPartitioner == null) {
      // No VeniceComplexPartitioner involved, perform simple delete.
      byte[] serializedKey = keySerializer.serialize(topicName, key);
      int partition = getPartition(serializedKey);
      propagateVeniceWriterFuture(delete(serializedKey, null, partition), finalCompletableFuture);
    } else {
      GenericRecord value = valueProvider.get();
      if (value == null) {
        // Ignore the delete since we cannot perform delete with VeniceComplexPartitioner without the value
        finalCompletableFuture.complete(null);
      } else {
        byte[] serializedKey = keySerializer.serialize(topicName, key);
        int[] partitions = complexPartitioner.getPartitionId(serializedKey, value, numberOfPartitions);
        if (partitions.length == 0) {
          finalCompletableFuture.complete(null);
        } else {
          performMultiPartitionAction(
              partitions,
              finalCompletableFuture,
              (partition) -> this.delete(serializedKey, null, partition));
        }
      }
    }
    return finalCompletableFuture;
  }

  /**
   * Prevent the {@link ComplexVeniceWriter} from writing any actual chunks for large values. This is because we are
   * only using ComplexVeniceWriter for writing to materialized view. The consumers of materialized view do not fully
   * support assembling the chunks correctly yet. The behavior is the same for both large values from VPJ and leader
   * replicas.
   */
  @Override
  protected CompletableFuture<PubSubProduceResult> putLargeValue(
      byte[] serializedKey,
      byte[] serializedValue,
      int valueSchemaId,
      PubSubProducerCallback callback,
      int partition,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs,
      PutMetadata putMetadata,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest) {
    skippedLargeRecords.incrementAndGet();
    if (!REDUNDANT_LOGGING_FILTER.isRedundantException(topicName, SKIP_LARGE_RECORD)) {
      logger.warn("Skipped writing {} large record(s) to topic: {}", skippedLargeRecords.getAndSet(0), topicName);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs,
      PutMetadata putMetadata,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest) {
    throw new UnsupportedOperationException("ComplexVeniceWriter should use complexPut instead of put");
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs,
      DeleteMetadata deleteMetadata,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest) {
    throw new UnsupportedOperationException("ComplexVeniceWriter should use complexDelete instead of delete");
  }

  @Override
  public Future<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      PubSubProducerCallback callback,
      long logicalTs) {
    throw new UnsupportedOperationException("ComplexVeniceWriter does not support update");
  }

  /**
   * Execute a "delete" on the key for a predetermined partition.
   */
  private CompletableFuture<PubSubProduceResult> delete(
      byte[] serializedKey,
      PubSubProducerCallback callback,
      int partition) {
    return delete(
        serializedKey,
        callback,
        DEFAULT_LEADER_METADATA_WRAPPER,
        APP_DEFAULT_LOGICAL_TS,
        null,
        null,
        null,
        partition);
  }

  /**
   * Write records with new DIV to a predetermined partition.
   */
  private CompletableFuture<PubSubProduceResult> put(
      byte[] serializedKey,
      byte[] serializedValue,
      int valueSchemaId,
      int partition) {
    return put(
        serializedKey,
        serializedValue,
        partition,
        valueSchemaId,
        null,
        DEFAULT_LEADER_METADATA_WRAPPER,
        APP_DEFAULT_LOGICAL_TS,
        null,
        null,
        null);
  }

  /**
   * Helper function to perform multi-partition action and configure the finalCompletableFuture to complete when the
   * action is completed on all partitions. Caller is expected to check for empty partitions case to minimize work
   * needed to provide the action function.
   */
  private void performMultiPartitionAction(
      int[] partitions,
      CompletableFuture<Void> finalCompletableFuture,
      Function<Integer, CompletableFuture<PubSubProduceResult>> action) {
    CompletableFuture<PubSubProduceResult>[] partitionFutures = new CompletableFuture[partitions.length];
    int index = 0;
    for (int p: partitions) {
      partitionFutures[index++] = action.apply(p);
    }
    CompletableFuture.allOf(partitionFutures).whenCompleteAsync((ignored, writeException) -> {
      if (writeException == null) {
        finalCompletableFuture.complete(null);
      } else {
        finalCompletableFuture.completeExceptionally(writeException);
      }
    });
  }

  private void propagateVeniceWriterFuture(
      CompletableFuture<PubSubProduceResult> veniceWriterFuture,
      CompletableFuture<Void> finalCompletableFuture) {
    veniceWriterFuture.whenCompleteAsync((ignored, writeException) -> {
      if (writeException == null) {
        finalCompletableFuture.complete(null);
      } else {
        finalCompletableFuture.completeExceptionally(writeException);
      }
    });
  }
}
