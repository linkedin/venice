package com.linkedin.venice.producer.online;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.VeniceProperties;


public class OnlineProducerFactory {
  public static <K, V> OnlineVeniceProducer<K, V> createProducer(
      ClientConfig storeClientConfig,
      VeniceProperties producerConfigs,
      ICProvider icProvider) {
    return new OnlineVeniceProducer<>(
        storeClientConfig,
        producerConfigs,
        storeClientConfig.getMetricsRepository(),
        icProvider);
  }
}
