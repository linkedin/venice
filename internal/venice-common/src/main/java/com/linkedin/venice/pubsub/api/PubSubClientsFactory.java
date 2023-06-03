package com.linkedin.venice.pubsub.api;

/**
 * A wrapper around pub-sub producer, consumer, and admin adapter factories
 *
 * This will be passed as one of the arguments to the component which depends on the pub-sub APIs.
 */
public class PubSubClientsFactory {
  private final PubSubProducerAdapterFactory producerAdapterFactory;
  private final PubSubConsumerAdapterFactory consumerAdapterFactory;
  private final PubSubAdminAdapterFactory adminAdapterFactory;

  public PubSubClientsFactory(
      PubSubProducerAdapterFactory producerAdapterFactory,
      PubSubConsumerAdapterFactory consumerAdapterFactory,
      PubSubAdminAdapterFactory adminAdapterFactory) {
    this.producerAdapterFactory = producerAdapterFactory;
    this.consumerAdapterFactory = consumerAdapterFactory;
    this.adminAdapterFactory = adminAdapterFactory;
  }

  public PubSubProducerAdapterFactory getProducerAdapterFactory() {
    return producerAdapterFactory;
  }

  public PubSubConsumerAdapterFactory getConsumerAdapterFactory() {
    return consumerAdapterFactory;
  }

  public PubSubAdminAdapterFactory getAdminAdapterFactory() {
    return adminAdapterFactory;
  }
}
