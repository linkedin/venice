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
      boolean unregisterMetricForDeletedStoreEnabled,
      boolean isDaVinciClient) {
    super(
        metricsRepository,
        new ServerHttpRequestStatsSupplier(requestType, isKeyValueProfilingEnabled, isDaVinciClient),
        metadataRepository,
        unregisterMetricForDeletedStoreEnabled);
  }

  static class ServerHttpRequestStatsSupplier implements StatsSupplier<ServerHttpRequestStats> {
    private final RequestType requestType;
    private final boolean isKeyValueProfilingEnabled;

    private boolean isDaVinciClient;

    ServerHttpRequestStatsSupplier(
        RequestType requestType,
        boolean isKeyValueProfilingEnabled,
        boolean isDaVinciClient) {
      this.requestType = requestType;
      this.isKeyValueProfilingEnabled = isKeyValueProfilingEnabled;
      this.isDaVinciClient = isDaVinciClient;
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
          totalStats,
          isDaVinciClient);
    }
  }

  public void recordErrorRequest() {
    totalStats.recordErrorRequest();
  }

  public void recordErrorRequestLatency(double latency) {
    totalStats.recordErrorRequestLatency(latency);
  }

  public void recordMisroutedStoreVersionRequest() {
    totalStats.recordMisroutedStoreVersionRequest();
  }
}
