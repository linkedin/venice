package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import java.io.Closeable;


/**
 * Generic factory interface for creating PubSub producers.
 * <p>
 * Concrete implementations should create and configure producers for a specific PubSub system (e.g., Kafka, Pulsar).
 * <p>
 * Implementations must provide a public no-arg constructor to support reflective instantiation.
 */
public abstract class PubSubProducerAdapterFactory<ADAPTER extends PubSubProducerAdapter> implements Closeable {
  /**
   * Constructor for PubSubProducerAdapterFactory used mainly for reflective instantiation.
   */
  public PubSubProducerAdapterFactory() {
    // no-op
  }

  /**
   * Creates a producer adapter using the provided context.
   * @param context A context object that contains all the necessary information to create a producer adapter.
   * @return Returns an instance of a producer adapter
   */
  public abstract ADAPTER create(PubSubProducerAdapterContext context);

  public abstract String getName();
}
