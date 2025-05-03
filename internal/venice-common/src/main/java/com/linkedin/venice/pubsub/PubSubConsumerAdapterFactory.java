package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.utils.VeniceProperties;
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
   * @param context                     Context for creating the consumer adapter.
   * @return                            Returns an instance of a consumer adapter
   */
  public abstract ADAPTER create(PubSubConsumerAdapterContext context);

  public VeniceProperties getDefaultProperties() {
    return VeniceProperties.empty();
  }

  public abstract String getName();
}
