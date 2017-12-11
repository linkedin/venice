package com.linkedin.venice.stats;

import com.linkedin.venice.read.RequestType;
import io.tehuti.metrics.MetricsRepository;


public class AggServerHttpRequestStats extends AbstractVeniceAggStats<ServerHttpRequestStats> {
  public AggServerHttpRequestStats(MetricsRepository metricsRepository, RequestType requestType) {
    super(metricsRepository,
          (metricsRepo, storeName) -> new ServerHttpRequestStats(metricsRepo, storeName, requestType));
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

  public void recordErrorRequestLatency (double latency) {
    totalStats.recordErrorRequestLatency(latency);
  }

  public void recordErrorRequestLatency(String storeName, double latency) {
    totalStats.recordErrorRequestLatency(latency);
    getStoreStats(storeName).recordErrorRequestLatency(latency);
  }

  public void recordBdbQueryLatency(String storeName, double latency, boolean assembledMultiChunkLargeValue) {
    totalStats.recordBdbQueryLatency(latency, assembledMultiChunkLargeValue);
    getStoreStats(storeName).recordBdbQueryLatency(latency, assembledMultiChunkLargeValue);
  }

  public void recordRequestKeyCount(String storeName, int keyNum) {
    totalStats.recordRequestKeyCount(keyNum);
    getStoreStats(storeName).recordRequestKeyCount(keyNum);
  }

  public void recordSuccessRequestKeyCount(String storeName, int keyNum) {
    totalStats.recordSuccessRequestKeyCount(keyNum);
    getStoreStats(storeName).recordSuccessRequestKeyCount(keyNum);
  }

  public void recordMultiChunkLargeValueCount(String storeName, int multiChunkLargeValueCount) {
    totalStats.recordMultiChunkLargeValueCount(multiChunkLargeValueCount);
    getStoreStats(storeName).recordMultiChunkLargeValueCount(multiChunkLargeValueCount);
  }
}
