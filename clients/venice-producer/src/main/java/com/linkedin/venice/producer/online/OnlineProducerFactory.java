package com.linkedin.venice.producer.online;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterHook;


public class OnlineProducerFactory {
  /**
   * Creates an online producer with an optional hook installed before writer construction.
   *
   * <p>The hook must be thread-safe. It runs on producer workers, or on the caller's thread when
   * {@code client.producer.worker.count=0}. Blocking delays submission; hook exceptions fail the write future.
   * Completing the hook does not complete durability, which still requires a broker acknowledgment.</p>
   *
   * @param writerHook the writer hook, or {@code null} to preserve hook-free behavior
   */
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
