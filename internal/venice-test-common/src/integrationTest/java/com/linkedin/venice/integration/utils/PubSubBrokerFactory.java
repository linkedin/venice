package com.linkedin.venice.integration.utils;

import com.linkedin.venice.pubsub.PubSubClientsFactory;


public interface PubSubBrokerFactory<BROKER extends PubSubBrokerWrapper> {
  StatefulServiceProvider<BROKER> generateService(PubSubBrokerConfigs configs);

  String getServiceName();

  /**
   * Anchor method for creating clients for this broker
   *
   * @return the {@link PubSubClientsFactory} for this broker factory
   */
  PubSubClientsFactory getClientsFactory();
}
