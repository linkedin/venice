package com.linkedin.venice.pubsub.api;

/**
 * A wrapper around pub-sub producer, consumer, and admin adapter factories
 *
 * This will be passed as one of the arguments to the component which depends on the pub-sub APIs.
 */
public class PubSubClientsFactory {
  private final PubSubProducerAdapterFactory producerAdapterFactory;
  // todo: Add PubSubAdminAdapterFactory and PubSubConsumerAdapterFactory once it is available

  public PubSubClientsFactory(PubSubProducerAdapterFactory producerAdapterFactory) {
    this.producerAdapterFactory = producerAdapterFactory;
  }

  public PubSubProducerAdapterFactory getProducerAdapterFactory() {
    return producerAdapterFactory;
  }
}
