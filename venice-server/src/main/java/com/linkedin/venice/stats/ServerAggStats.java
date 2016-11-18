package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerPerStoreService;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class ServerAggStats {
  private static ServerAggStats instance;
  private static MetricsRepository metricsRepository;
  private static KafkaConsumerPerStoreService kafkaConsumerPerStoreService;

  //per store metrics
  final Map<String, ServerStats> storeMetrics;
  final ServerStats totalStats;

  public static synchronized void init(MetricsRepository metricsRepository,
                                       KafkaConsumerPerStoreService kafkaConsumerPerStoreService) {
    if (metricsRepository == null) {
      throw new IllegalArgumentException("metricsRepository is null");
    }

    if (kafkaConsumerPerStoreService == null) {
      throw new IllegalArgumentException("KafkaConsumerPerStoreService is null");
    }

    if (instance == null) {
      ServerAggStats.metricsRepository = metricsRepository;
      ServerAggStats.kafkaConsumerPerStoreService = kafkaConsumerPerStoreService;
      instance = new ServerAggStats(kafkaConsumerPerStoreService);
    }
  }

  public static ServerAggStats getInstance() {
    if (instance == null) {
      throw new VeniceException("ServerStats has not been initialized yet.");
    }
    return instance;
  }

  private ServerStats getStoreStats(String storeName) {
    ServerStats storeStats =
      storeMetrics.computeIfAbsent(storeName, k -> new ServerStats(metricsRepository, storeName,
                                                                   kafkaConsumerPerStoreService));
    return storeStats;
  }

  private ServerAggStats(KafkaConsumerPerStoreService kafkaConsumerPerStoreService) {
    this.storeMetrics = new ConcurrentHashMap<>();
    this.totalStats = new ServerStats(metricsRepository, "total", kafkaConsumerPerStoreService);
  }

  public void recordBytesConsumed(String storeName, long bytes) {
    totalStats.recordBytesConsumed(bytes);
    getStoreStats(storeName).recordBytesConsumed(bytes);
  }

  public void recordRecordsConsumed(String storeName, int count) {
    totalStats.recordRecordsConsumed(count);
    getStoreStats(storeName).recordRecordsConsumed(count);
  }

  public void recordSuccessRequest(String storeName) {
    totalStats.recordSuccessRequest();
    getStoreStats(storeName).recordSuccessRequest();
  }

  public void recordErrorRequest() {
    totalStats.recordErrorRequest();
  }

  public void recordErrorRequest(String storeName) {
    totalStats.recordErrorRequest();
    getStoreStats(storeName).recordErrorRequest();
  }

  public void recordSuccessRequestLatency(String storeName, double latency) {
    totalStats.recordSuccessRequestLatency(latency);
    getStoreStats(storeName).recordSuccessRequestLatency(latency);
  }

  public void recordErrorRequestLatency(double latency) {
    totalStats.recordErrorRequestLatency(latency);
  }

  public void recordErrorRequestLatency(String storeName, double latency) {
    totalStats.recordErrorRequestLatency(latency);
    getStoreStats(storeName).recordErrorRequestLatency(latency);
  }

  public void recordBdbQueryLatency(String storeName, double latency) {
    totalStats.recordBdbQueryLatency(latency);
    getStoreStats(storeName).recordBdbQueryLatency(latency);
  }

  public void close() {
    for (ServerStats storeStats : storeMetrics.values()) {
      storeStats.close();
    }

    totalStats.close();
  }
}
