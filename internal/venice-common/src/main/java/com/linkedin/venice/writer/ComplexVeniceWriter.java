package com.linkedin.venice.writer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.ComplexVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.VeniceView;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;


/**
 * Provide more complex and sophisticated writer APIs for writing to {@link com.linkedin.venice.views.MaterializedView}.
 * Specifically when a {@link ComplexVenicePartitioner} is involved. Otherwise, use the
 * {@link VeniceWriter} APIs.
 */
public class ComplexVeniceWriter<K, V, U> extends VeniceWriter<K, V, U> {
  private final ComplexVenicePartitioner complexPartitioner;
  private final String viewName;

  public ComplexVeniceWriter(
      VeniceWriterOptions params,
      VeniceProperties props,
      PubSubProducerAdapter producerAdapter) {
    super(params, props, producerAdapter);
    if (partitioner.getPartitionerType() == VenicePartitioner.VenicePartitionerType.COMPLEX) {
      complexPartitioner = (ComplexVenicePartitioner) partitioner;
    } else {
      complexPartitioner = null;
    }
    // For now, we expect ComplexVeniceWriter to be used only for writing to MaterializedView
    viewName =
        VeniceView.getViewNameFromViewStoreName(VeniceView.parseStoreAndViewFromViewTopic(params.getTopicName()));
  }

  public CompletableFuture<Void> complexPut(K key, V value, int valueSchemaId, Lazy<GenericRecord> valueProvider) {
    return complexPut(key, value, valueSchemaId, valueProvider, null, null, null);
  }

  /**
   * {@link ComplexVenicePartitioner} offers a more sophisticated getPartitionId API. It also takes value as a
   * parameter, and could return a single, multiple or no partition(s). The API also accepts a partition consumer to
   * offer the resulting partition(s) of this complexPut.
   */
  public CompletableFuture<Void> complexPut(
      K key,
      V value,
      int valueSchemaId,
      Lazy<GenericRecord> valueProvider,
      Consumer<int[]> partitionConsumer,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    CompletableFuture<Void> finalCompletableFuture = new CompletableFuture<>();
    if (value == null) {
      // Ignore null value
      throw new VeniceException("Put value should not be null");
    } else {
      // Write updated/put record to materialized view topic partition(s)
      byte[] serializedKey = keySerializer.serialize(topicName, key);
      if (complexPartitioner == null) {
        // No VeniceComplexPartitioner involved, perform simple put.
        byte[] serializedValue = valueSerializer.serialize(topicName, value);
        int partition = getPartition(serializedKey);
        propagateVeniceWriterFuture(
            put(serializedKey, serializedValue, partition, valueSchemaId, callback, putMetadata),
            finalCompletableFuture);
        if (partitionConsumer != null) {
          partitionConsumer.accept(new int[] { partition });
        }
      } else {
        int[] partitions = complexPartitioner.getPartitionId(serializedKey, valueProvider.get(), numberOfPartitions);
        if (partitions.length == 0) {
          finalCompletableFuture.complete(null);
        } else {
          byte[] serializedValue = valueSerializer.serialize(topicName, value);
          performMultiPartitionAction(
              partitions,
              finalCompletableFuture,
              (partition) -> put(serializedKey, serializedValue, partition, valueSchemaId, callback, putMetadata));
        }
        if (partitionConsumer != null) {
          partitionConsumer.accept(partitions);
        }
      }
    }
    return finalCompletableFuture;
  }

  /**
   * Used during NR pass-through in remote region to forward records or chunks of records to corresponding view
   * partition based on provided view partition map. This way the producing leader don't need to worry about large
   * record assembly or chunking for view topic(s) when ingesting from source VT during NR pass-through. It's also
   * expected to receive an empty partition set and in which case it's a no-op and we simply return a completed future.
   * This is a valid use case since certain complex partitioner implementation could filter out records based on value
   * fields and return an empty partition.
   */
  public CompletableFuture<Void> forwardPut(K key, V value, int valueSchemaId, Set<Integer> partitions) {
    if (partitions.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    byte[] serializedKey = keySerializer.serialize(topicName, key);
    byte[] serializedValue = valueSerializer.serialize(topicName, value);
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, serializedKey);
    Put putPayload = buildPutPayload(serializedValue, valueSchemaId, null);
    int[] partitionArray = partitions.stream().mapToInt(i -> i).toArray();
    CompletableFuture<Void> finalCompletableFuture = new CompletableFuture<>();
    performMultiPartitionAction(
        partitionArray,
        finalCompletableFuture,
        (partition) -> sendMessage(
            producerMetadata -> kafkaKey,
            MessageType.PUT,
            putPayload,
            partition,
            null,
            DEFAULT_LEADER_METADATA_WRAPPER,
            APP_DEFAULT_LOGICAL_TS));
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
              (partition) -> delete(serializedKey, null, partition));
        }
      }
    }
    return finalCompletableFuture;
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
      int partition,
      int valueSchemaId,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    return put(
        serializedKey,
        serializedValue,
        partition,
        valueSchemaId,
        callback,
        DEFAULT_LEADER_METADATA_WRAPPER,
        APP_DEFAULT_LOGICAL_TS,
        putMetadata,
        null,
        null);
  }

  public String getViewName() {
    return viewName;
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
