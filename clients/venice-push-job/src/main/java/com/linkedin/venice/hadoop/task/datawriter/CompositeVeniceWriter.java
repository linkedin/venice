package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_TERM_ID;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_UPSTREAM_KAFKA_CLUSTER_ID;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.ComplexVeniceWriter;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;


/**
 * The composite writer contains a main writer and multiple child writers. The main writer will only perform the write
 * once all of its child writers are complete. Child writers are {@link com.linkedin.venice.writer.ComplexVeniceWriter}.
 * This is to provide chunking support during NR pass-through. All records produced by the {@link ComplexVeniceWriter}'s
 * main writer is expected to carry the
 * {@link com.linkedin.venice.pubsub.api.PubSubMessageHeaders#VENICE_VIEW_PARTITIONS_MAP_HEADER}.
 */
@NotThreadsafe
public class CompositeVeniceWriter<K, V, U> extends AbstractVeniceWriter<K, V, U> {
  private final VeniceWriter<K, V, U> mainWriter;
  private final ComplexVeniceWriter<K, V, U>[] childWriters;
  private final PubSubProducerCallback childCallback;

  // the extractor should be capable of extracting the value from bytes even if it's compressed.
  private final BiFunction<V, Integer, GenericRecord> valueExtractor;

  public CompositeVeniceWriter(
      String topicName,
      VeniceWriter<K, V, U> mainWriter,
      ComplexVeniceWriter<K, V, U>[] childWriters,
      PubSubProducerCallback childCallback,
      BiFunction<V, Integer, GenericRecord> valueExtractor) {
    super(topicName);
    if (childWriters.length < 1) {
      throw new IllegalArgumentException("A composite writer is not needed if there are no child writers");
    }
    this.mainWriter = mainWriter;
    this.childWriters = childWriters;
    this.childCallback = childCallback;
    this.valueExtractor = valueExtractor;
  }

  @Override
  public void close(boolean gracefulClose) throws IOException {
    // no op, child writers and the main writer are initialized outside the class and should be closed elsewhere.
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback) {
    return compositePut(key, value, APP_DEFAULT_LOGICAL_TS, valueSchemaId, callback, null);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      long logicalTimestamp,
      PubSubProducerCallback callback) {
    return compositePut(key, value, logicalTimestamp, valueSchemaId, callback, null);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    return compositePut(key, value, APP_DEFAULT_LOGICAL_TS, valueSchemaId, callback, putMetadata);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      long logicalTimestamp,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    return compositePut(key, value, logicalTimestamp, valueSchemaId, callback, putMetadata);
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
    return mainWriter.delete(key, callback, deleteMetadata);
  }

  /**
   * The main use of the {@link CompositeVeniceWriter} for now is to write batch portion of a store version to VT and
   * materialized view topic in the NR fabric. Updates should never go through the {@link CompositeVeniceWriter} because
   * it should be written to RT (hybrid writes or incremental push) and handled by view writers in L/F or A/A SIT.
   */
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
  public CompletableFuture<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      long logicalTimestamp,
      PubSubProducerCallback callback) {
    throw new UnsupportedOperationException(this.getClass().getSimpleName() + " does not support update function");
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(K key, PubSubProducerCallback callback) {
    throw new UnsupportedOperationException(
        this.getClass().getSimpleName() + " does not support delete function without delete metadata");
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(K key, long logicalTimestamp, PubSubProducerCallback callback) {
    throw new UnsupportedOperationException(
        this.getClass().getSimpleName() + " does not support delete function without delete metadata");
  }

  @Override
  public void flush() {
    for (AbstractVeniceWriter writer: childWriters) {
      writer.flush();
    }
    mainWriter.flush();
  }

  @Override
  public void close() throws IOException {
    close(true);
  }

  /**
   * Helper function to perform a composite put where put is first executed for all childWriters and then put is
   * executed for mainWriter. The returned completable future is completed when the mainWriter completes the put.
   */
  private CompletableFuture<PubSubProduceResult> compositePut(
      K key,
      V value,
      long logicalTimestamp,
      int valueSchemaId,
      PubSubProducerCallback mainWriterCallback,
      PutMetadata putMetadata) {
    CompletableFuture<PubSubProduceResult> finalFuture = new CompletableFuture<>();
    CompletableFuture<Void>[] childFutures = new CompletableFuture[childWriters.length];
    Lazy<GenericRecord> valueProvider = Lazy.of(() -> valueExtractor.apply(value, valueSchemaId));
    Map<String, Set<Integer>> viewPartitionMap = new HashMap<>();
    int index = 0;
    for (ComplexVeniceWriter<K, V, U> writer: childWriters) {
      // There should be an entry for every materialized view, even if the partition set is empty. This way we can
      // differentiate between skipped view write and missing view partition info unexpectedly.
      childFutures[index++] = writer.complexPut(
          key,
          value,
          valueSchemaId,
          valueProvider,
          (partitionArray) -> viewPartitionMap.put(
              writer.getViewName(),
              Arrays.stream(partitionArray).boxed().collect(Collectors.toCollection(HashSet::new))),
          childCallback,
          putMetadata);
    }
    LeaderMetadataWrapper leaderMetadataWrapper = new LeaderMetadataWrapper(
        PubSubSymbolicPosition.EARLIEST,
        DEFAULT_UPSTREAM_KAFKA_CLUSTER_ID,
        DEFAULT_TERM_ID,
        viewPartitionMap);
    // We only need to pass the logical timestamp to the main writer as it's only used for write conflict resolution
    // in the venice server or TTL repush. So we don't need to pass it to view topics.
    long passedTimestamp = logicalTimestamp > 0 ? logicalTimestamp : APP_DEFAULT_LOGICAL_TS;
    CompletableFuture<PubSubProduceResult> mainFuture = mainWriter
        .put(key, value, valueSchemaId, mainWriterCallback, leaderMetadataWrapper, passedTimestamp, putMetadata);
    CompletableFuture.allOf(childFutures).whenCompleteAsync((ignored, writeException) -> {
      if (writeException == null) {
        try {
          finalFuture.complete(mainFuture.get());
        } catch (Exception e) {
          finalFuture.completeExceptionally(
              new VeniceException("compositePut's main writer throwing exception unexpectedly", e));
        }
      } else {
        finalFuture.completeExceptionally(new VeniceException(writeException));
      }
    });
    return finalFuture;
  }
}
