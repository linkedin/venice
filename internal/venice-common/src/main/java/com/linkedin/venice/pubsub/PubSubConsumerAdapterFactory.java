package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import java.io.Closeable;


/**
 * Generic factory interface for creating PubSub consumers.
 * <p>
 * Concrete implementations should create and configure consumers for a specific PubSub system (e.g., Kafka, Pulsar).
 * <p>
 * Implementations must provide a public no-arg constructor to support reflective instantiation.
 */
public abstract class PubSubConsumerAdapterFactory<ADAPTER extends PubSubConsumerAdapter> implements Closeable {
  /**
   * Constructor for PubSubConsumerAdapterFactory used mainly for reflective instantiation.
   */
  public PubSubConsumerAdapterFactory() {
    // no-op
  }

  /**
   * Creates a PubSub consumer adapter.
   *
   * @param context The context containing all dependencies and configurations required to create a consumer.
   * @return An instance of the PubSub consumer adapter.
   */
  public abstract ADAPTER create(PubSubConsumerAdapterContext context);

  public abstract String getName();
}
