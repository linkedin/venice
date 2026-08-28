package com.linkedin.venice.writer;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;


/**
 * A base class which users of {@link VeniceWriter} can leverage in order to
 * make unit tests easier.
 *
 * @see VeniceWriter
 * // @see MockVeniceWriter in the VPJ tests (commented because this module does not depend on VPJ)
 */
public abstract class AbstractVeniceWriter<K, V, U> implements Closeable {
  protected final String topicName;

  public AbstractVeniceWriter(String topicName) {
    this.topicName = topicName;
  }

  public String getTopicName() {
    return this.topicName;
  }

  public CompletableFuture<PubSubProduceResult> put(K key, V value, int valueSchemaId) {
    return put(key, value, valueSchemaId, null);
  }

  public abstract void close(boolean gracefulClose) throws IOException;

  public abstract CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback);

  public abstract CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      long logicalTimestamp,
      PubSubProducerCallback callback);

  public abstract Future<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      PubSubProducerCallback callback);

  public abstract CompletableFuture<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      long logicalTimestamp,
      PubSubProducerCallback callback);

  public abstract CompletableFuture<PubSubProduceResult> delete(K key, PubSubProducerCallback callback);

  public abstract CompletableFuture<PubSubProduceResult> delete(
      K key,
      long logicalTimestamp,
      PubSubProducerCallback callback);

  public abstract CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      PutMetadata putMetadata);

  public abstract CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      long logicalTimestamp,
      PubSubProducerCallback callback,
      PutMetadata putMetadata);

  public abstract CompletableFuture<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      DeleteMetadata deleteMetadata);

  public abstract void flush();

  /**
   * Returns the partition id that {@code key} maps to under this writer's partitioner, or a conservative
   * default of {@code 0} for writers that cannot compute it. Additive and binary-compatible: callers that
   * need partition-stable routing (for example striped dispatch) can use it, while legacy writers fall back
   * to a single stripe. Overridden by {@link VeniceWriter} and delegated by {@link BatchingVeniceWriter}.
   *
   * @param key the (typed) key to route
   * @return a non-negative partition id, or 0 when routing is unavailable
   */
  public int getPartitionId(K key) {
    return 0;
  }
}
