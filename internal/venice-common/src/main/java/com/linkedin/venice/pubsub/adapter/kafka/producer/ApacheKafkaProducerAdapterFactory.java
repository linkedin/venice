package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.pubsub.PubSubProducerAdapterContext;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;


/**
 * Implementation of {@link PubSubProducerAdapterFactory} used to create Apache Kafka producers.
 *
 * A producer created using this factory is usually used to send data to a single pub-sub topic.
 */
public class ApacheKafkaProducerAdapterFactory extends PubSubProducerAdapterFactory<ApacheKafkaProducerAdapter> {
  private static final String NAME = "ApacheKafkaProducer";

  /**
   * Constructor for ApacheKafkaProducerAdapterFactory used for reflective instantiation.
   */
  public ApacheKafkaProducerAdapterFactory() {
    // no-op
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
