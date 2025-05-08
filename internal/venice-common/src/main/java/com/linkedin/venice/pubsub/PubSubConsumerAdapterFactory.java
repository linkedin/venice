package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
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
   *
   * @param veniceProperties            A copy of venice properties. Relevant consumer configs will be extracted from
   *                                    veniceProperties using prefix matching. For example, to construct kafka consumer
   *                                    configs that start with "kafka." prefix will be used.
   * @param isOffsetCollectionEnabled   A flag to enable collection of offset or not.
   * @param pubSubMessageDeserializer   To deserialize the raw byte records into {@link PubSubMessage}s to process.
   * @param consumerName                Name of the consumer. If not null, it will be used to set the context
   *                                    for consumer thread.
   * @return                            Returns an instance of a consumer adapter
   */
  public abstract ADAPTER create(
      VeniceProperties veniceProperties,
      boolean isOffsetCollectionEnabled,
      PubSubMessageDeserializer pubSubMessageDeserializer,
      String consumerName);

  public abstract String getName();
}
