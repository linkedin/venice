package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterContext;
import com.linkedin.venice.utils.VeniceProperties;


/**
 * Implementation of {@link PubSubProducerAdapterFactory} used to create Apache Kafka producers.
 *
 * A producer created using this factory is usually used to send data to a single pub-sub topic.
 */
public class ApacheKafkaProducerAdapterFactory implements PubSubProducerAdapterFactory<ApacheKafkaProducerAdapter> {
  private static final String NAME = "ApacheKafkaProducer";

  @Override
  @Deprecated
  public ApacheKafkaProducerAdapter create(
      VeniceProperties veniceProperties,
      String producerName,
      String brokerAddressToOverride) {
    return create(
        new PubSubProducerAdapterContext.Builder().setVeniceProperties(veniceProperties)
            .setBrokerAddress(brokerAddressToOverride)
            .setShouldValidateProducerConfigStrictly(true)
            .setProducerName(producerName)
            .build());
  }

  @Override
  public ApacheKafkaProducerAdapter create(PubSubProducerAdapterContext context) {
    return new ApacheKafkaProducerAdapter(new ApacheKafkaProducerConfig(context));
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void close() {
  }
}
