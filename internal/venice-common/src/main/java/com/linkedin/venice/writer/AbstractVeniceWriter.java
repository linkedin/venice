package com.linkedin.venice.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;


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

  public Future<RecordMetadata> put(K key, V value, int valueSchemaId) {
    return put(key, value, valueSchemaId, null);
  }

  public abstract void close(boolean gracefulClose) throws IOException;

  public abstract Future<RecordMetadata> put(K key, V value, int valueSchemaId, Callback callback);

  public abstract Future<RecordMetadata> put(
      K key,
      V value,
      int valueSchemaId,
      Callback callback,
      PutMetadata putMetadata);

  public abstract Future<RecordMetadata> delete(K key, Callback callback, DeleteMetadata deleteMetadata);

  public abstract Future<RecordMetadata> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      Callback callback);

  public abstract void flush();
}
