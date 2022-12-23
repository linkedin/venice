package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import io.tehuti.metrics.MetricsRepository;


/**
 * {@code AggServerHttpRequestStats} is the aggregate statistics for {@code ServerHttpRequestStats} corresponding to
 * the type of requests defined in {@link RequestType}.
 */
public class AggServerHttpRequestStats extends AbstractVeniceAggStoreStats<ServerHttpRequestStats> {
  public AggServerHttpRequestStats(
      MetricsRepository metricsRepository,
      RequestType requestType,
      boolean isKeyValueProfilingEnabled,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled) {
    super(
        metricsRepository,
        new ServerHttpRequestStatsSupplier(requestType, isKeyValueProfilingEnabled),
        metadataRepository,
        unregisterMetricForDeletedStoreEnabled);
  }

  static class ServerHttpRequestStatsSupplier implements StatsSupplier<ServerHttpRequestStats> {
    private final RequestType requestType;
    private final boolean isKeyValueProfilingEnabled;

    ServerHttpRequestStatsSupplier(RequestType requestType, boolean isKeyValueProfilingEnabled) {
      this.requestType = requestType;
      this.isKeyValueProfilingEnabled = isKeyValueProfilingEnabled;
    }

    @Override
    public ServerHttpRequestStats get(MetricsRepository metricsRepository, String storeName) {
      throw new VeniceException("Should not be called.");
    }

    @Override
    public ServerHttpRequestStats get(
        MetricsRepository metricsRepository,
        String storeName,
        ServerHttpRequestStats totalStats) {
      return new ServerHttpRequestStats(
          metricsRepository,
          storeName,
          requestType,
          isKeyValueProfilingEnabled,
          totalStats);
    }
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

  public void recordMultiChunkLargeValueCount(String storeName, int multiChunkLargeValueCount) {
    totalStats.recordMultiChunkLargeValueCount(multiChunkLargeValueCount);
    getStoreStats(storeName).recordMultiChunkLargeValueCount(multiChunkLargeValueCount);
  }

  public void recordStorageExecutionHandlerSubmissionWaitTime(double submissionWaitTime) {
    totalStats.recordStorageExecutionHandlerSubmissionWaitTime(submissionWaitTime);
  }

  public void recordStorageExecutionQueueLen(int len) {
    totalStats.recordStorageExecutionQueueLen(len);
  }
}
