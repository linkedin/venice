package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.kafka.KafkaClientFactory;


/**
 * A wrapper around pub-sub producer, consumer, and admin adapter factories
 *
 * This will be passed as one of the arguments to the component which depends on the pub-sub APIs.
 *
 * TODO: Replace KafkaClientFactory with PubSubAdminAdapterFactory & PubSubConsumerAdapterFactory once they
 * are added.
 */
public class PubSubClientsFactory {
  private final PubSubProducerAdapterFactory producerAdapterFactory;
  // todo: replace KafkaClientFactory with consumer and admin factory once it is available
  private final KafkaClientFactory kafkaClientFactory;

  public PubSubClientsFactory(
      PubSubProducerAdapterFactory producerAdapterFactory,
      KafkaClientFactory kafkaClientFactory) {
    this.producerAdapterFactory = producerAdapterFactory;
    this.kafkaClientFactory = kafkaClientFactory;
  }

  PubSubProducerAdapterFactory<PubSubProducerAdapter> getProducerAdapterFactory() {
    return producerAdapterFactory;
  }

  KafkaClientFactory getKafkaClientFactory() {
    return kafkaClientFactory;
  }
}
