package com.linkedin.venice.producer.online;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterHook;


public class OnlineProducerFactory {
  public static <K, V> OnlineVeniceProducer<K, V> createProducer(
      ClientConfig storeClientConfig,
      VeniceProperties producerConfigs,
      ICProvider icProvider) {
    return createProducer(storeClientConfig, producerConfigs, icProvider, null);
  }

  public static <K, V> OnlineVeniceProducer<K, V> createProducer(
      ClientConfig storeClientConfig,
      VeniceProperties producerConfigs,
      ICProvider icProvider,
      VeniceWriterHook writerHook) {
    return new OnlineVeniceProducer<>(
        storeClientConfig,
        producerConfigs,
        storeClientConfig.getMetricsRepository(),
        icProvider,
        writerHook);
  }
}
