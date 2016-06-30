package com.linkedin.venice.client;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

/**
 * A base class which users of {@link VeniceWriter} can leverage in order to
 * make unit tests easier.
 *
 * @see VeniceWriter
 * // @see MockVeniceWriter in the H2V tests (commented because this module does not depend on H2V)
 */
public abstract class AbstractVeniceWriter <K, V> implements Closeable {
  protected final String topicName;

  public AbstractVeniceWriter(String topicName) {
    this.topicName = topicName;
  }

  public String getTopicName() {
    return this.topicName;
  }

  public abstract Future<RecordMetadata> put(K key, V value, int valueSchemaId);
}
