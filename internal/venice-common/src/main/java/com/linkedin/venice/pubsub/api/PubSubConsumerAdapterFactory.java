package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;


/**
 * Generic consumer factory interface.
 *
 * A pus-sub specific concrete implementation of this interface should be provided to be able to create
 * and instantiate consumers for that system.
 */
public interface PubSubConsumerAdapterFactory<ADAPTER extends PubSubConsumerAdapter> extends Closeable {
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
  ADAPTER create(
      VeniceProperties veniceProperties,
      boolean isOffsetCollectionEnabled,
      PubSubMessageDeserializer pubSubMessageDeserializer,
      String consumerName);

  String getName();
}
