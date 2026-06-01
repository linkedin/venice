package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.concurrent.LatencyPercentileProvider;
import io.tehuti.metrics.MetricsRepository;


/**
 * {@code AggServerHttpRequestStats} is the aggregate statistics for {@code ServerHttpRequestStats} corresponding to
 * the type of requests defined in {@link RequestType}.
 */
public class AggServerHttpRequestStats extends AbstractVeniceAggStoreStats<ServerHttpRequestStats> {
  public AggServerHttpRequestStats(
      String clusterName,
      MetricsRepository metricsRepository,
      RequestType requestType,
      boolean isKeyValueProfilingEnabled,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled,
      boolean isDaVinciClient,
      boolean readOtelStatsEnabled) {
    this(
        clusterName,
        metricsRepository,
        requestType,
        isKeyValueProfilingEnabled,
        metadataRepository,
        unregisterMetricForDeletedStoreEnabled,
        isDaVinciClient,
        readOtelStatsEnabled,
        null);
  }

  public AggServerHttpRequestStats(
      String clusterName,
      MetricsRepository metricsRepository,
      RequestType requestType,
      boolean isKeyValueProfilingEnabled,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled,
      boolean isDaVinciClient,
      boolean readOtelStatsEnabled,
      LatencyPercentileProvider latencyPercentileProvider) {
    super(
        clusterName,
        metricsRepository,
        new ServerHttpRequestStatsSupplier(
            requestType,
            isKeyValueProfilingEnabled,
            isDaVinciClient,
            readOtelStatsEnabled,
            latencyPercentileProvider),
        metadataRepository,
        unregisterMetricForDeletedStoreEnabled,
        false);
  }

  static class ServerHttpRequestStatsSupplier implements StatsSupplier<ServerHttpRequestStats> {
    private final RequestType requestType;
    private final boolean isKeyValueProfilingEnabled;
    private final boolean isDaVinciClient;
    private final boolean readOtelStatsEnabled;
    private final LatencyPercentileProvider latencyPercentileProvider;

    ServerHttpRequestStatsSupplier(
        RequestType requestType,
        boolean isKeyValueProfilingEnabled,
        boolean isDaVinciClient,
        boolean readOtelStatsEnabled,
        LatencyPercentileProvider latencyPercentileProvider) {
      this.requestType = requestType;
      this.isKeyValueProfilingEnabled = isKeyValueProfilingEnabled;
      this.isDaVinciClient = isDaVinciClient;
      this.readOtelStatsEnabled = readOtelStatsEnabled;
      this.latencyPercentileProvider = latencyPercentileProvider;
    }

    @Override
    public ServerHttpRequestStats get(MetricsRepository metricsRepository, String storeName, String clusterName) {
      throw new VeniceException("Should not be called.");
    }

    @Override
    public ServerHttpRequestStats get(
        MetricsRepository metricsRepository,
        String storeName,
        String clusterName,
        ServerHttpRequestStats totalStats) {
      return new ServerHttpRequestStats(
          metricsRepository,
          storeName,
          clusterName,
          requestType,
          isKeyValueProfilingEnabled,
          totalStats,
          isDaVinciClient,
          readOtelStatsEnabled,
          latencyPercentileProvider);
    }
  }

  public void recordErrorRequest(
      HttpResponseStatusEnum statusEnum,
      HttpResponseStatusCodeCategory statusCategory,
      VeniceResponseStatusCategory veniceCategory) {
    totalStats.recordErrorRequest(statusEnum, statusCategory, veniceCategory);
  }

  public void recordMisroutedStoreVersionRequest() {
    totalStats.recordMisroutedStoreVersionRequest();
  }

  public void recordKeyNotFoundCount(int count) {
    totalStats.recordKeyNotFoundCount(count);
  }
}
