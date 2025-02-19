package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterContext;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;


/**
 * Generic producer factory interface.
 *
 * A pus-sub specific concrete implementation of this interface should be provided to be able to create
 * and instantiate producers for that system.
 */
public interface PubSubProducerAdapterFactory<ADAPTER extends PubSubProducerAdapter> extends Closeable {
  /**
   *
   * @param veniceProperties     A copy of venice properties. Relevant producer configs will be extracted from
   *                             veniceProperties using prefix matching. For example, to construct kafka producer
   *                             configs that start with "kafka." prefix will be used.
   * @param producerName         Name of the producer. If not null, it will be used to set the context
   *                             for producer thread.
   * @param targetBrokerAddress  Broker address to use when creating a producer.
   *                             If this value is null, local broker address present in veniceProperties will be used.
   * @return                     Returns an instance of a producer adapter
   */
  @Deprecated
  ADAPTER create(VeniceProperties veniceProperties, String producerName, String targetBrokerAddress);

  default ADAPTER create(PubSubProducerAdapterContext context) {
    return create(context.getVeniceProperties(), context.getProducerName(), context.getBrokerAddress());
  }

  String getName();
}
