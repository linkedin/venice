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

  /**
   * Return the exact Venice partition that this writer will use for a serialized key.
   *
   * <p>This is a non-abstract compatibility extension so existing external subclasses continue to link. Writers that
   * can route serialized byte keys should override it; callers receive a clear failure from unsupported wrappers rather
   * than guessing from controller metadata.</p>
   *
   * @param serializedKey serialized Venice key
   * @return the partition selected by this writer
   */
  public int getPartitionId(byte[] serializedKey) {
    throw new UnsupportedOperationException(
        "Partition routing is not supported by writer class " + getClass().getName());
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
}
