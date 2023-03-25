package com.linkedin.venice.integration.utils;

public interface PubSubBrokerFactory {
  StatefulServiceProvider<PubSubBrokerWrapper> generateService(PubSubBrokerConfigs configs);

  String getServiceName();
}
