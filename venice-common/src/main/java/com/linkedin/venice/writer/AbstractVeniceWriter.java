package com.linkedin.venice.writer;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * A base class which users of {@link VeniceWriter} can leverage in order to
 * make unit tests easier.
 *
 * @see VeniceWriter
 * // @see MockVeniceWriter in the H2V tests (commented because this module does not depend on H2V)
 */
public abstract class AbstractVeniceWriter <K, V, U> implements Closeable {
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

  public abstract void close(boolean shouldEndAllSegments) throws IOException;

  public abstract Future<RecordMetadata> put(K key, V value, int valueSchemaId, Callback callback);

  public abstract Future<RecordMetadata> put(K key, V value, int valueSchemaId, Callback callback, PutMetadata putMetadata);

  public abstract Future<RecordMetadata> update(K key, U update, int valueSchemaId, int derivedSchemaId, Callback callback);

  public abstract void flush();

  public Map<String, Double> getMeasurableProducerMetrics() {
    return Collections.emptyMap();
  }

  public String getBrokerLeaderHostname(String topic, int partition) {
    throw new VeniceException("Not implemented");
  };
}
