package com.linkedin.venice.integration.utils;

import com.linkedin.venice.pubsub.api.PubSubClientsFactory;


public interface PubSubBrokerFactory {
  StatefulServiceProvider<PubSubBrokerWrapper> generateService(PubSubBrokerConfigs configs);

  String getServiceName();

  /**
   * Anchor method for creating clients for this broker
   *
   * @return the {@link PubSubClientsFactory} for this broker factory
   */
  PubSubClientsFactory getClientsFactory();
}
