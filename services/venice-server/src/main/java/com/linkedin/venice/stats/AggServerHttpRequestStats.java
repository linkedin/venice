package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
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
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled,
      boolean isDaVinciClient) {
    super(
        clusterName,
        metricsRepository,
        new ServerHttpRequestStatsSupplier(requestType, isDaVinciClient),
        metadataRepository,
        unregisterMetricForDeletedStoreEnabled,
        false);
  }

  static class ServerHttpRequestStatsSupplier implements StatsSupplier<ServerHttpRequestStats> {
    private final RequestType requestType;
    private final boolean isDaVinciClient;

    ServerHttpRequestStatsSupplier(RequestType requestType, boolean isDaVinciClient) {
      this.requestType = requestType;
      this.isDaVinciClient = isDaVinciClient;
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
          totalStats,
          isDaVinciClient);
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
