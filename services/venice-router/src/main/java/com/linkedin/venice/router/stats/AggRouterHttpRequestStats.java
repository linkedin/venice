package com.linkedin.venice.router.stats;

import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.stats.AbstractVeniceAggStoreStats;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.function.Function;


public class AggRouterHttpRequestStats extends AbstractVeniceAggStoreStats<RouterHttpRequestStats> {
  private final Map<String, ScatterGatherStats> scatterGatherStatsMap = new VeniceConcurrentHashMap<>();

  public AggRouterHttpRequestStats(
      String clusterName,
      MetricsRepository metricsRepository,
      RequestType requestType,
      ReadOnlyStoreRepository metadataRepository,
      boolean isUnregisterMetricForDeletedStoreEnabled) {
    this(
        clusterName,
        metricsRepository,
        requestType,
        false,
        metadataRepository,
        isUnregisterMetricForDeletedStoreEnabled);
  }

  public AggRouterHttpRequestStats(
      String cluster,
      MetricsRepository metricsRepository,
      RequestType requestType,
      boolean isKeyValueProfilingEnabled,
      ReadOnlyStoreRepository metadataRepository,
      boolean isUnregisterMetricForDeletedStoreEnabled) {
    super(cluster, metricsRepository, metadataRepository, isUnregisterMetricForDeletedStoreEnabled);
    /**
     * Use a setter function to bypass the restriction that the supertype constructor could not
     * touch member fields of current object.
     */
    setStatsSupplier((metricsRepo, storeName, clusterName) -> {
      ScatterGatherStats stats;
      if (storeName.equals(AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT)) {
        stats = new AggScatterGatherStats();
      } else {
        stats = scatterGatherStatsMap.computeIfAbsent(storeName, k -> new ScatterGatherStats());
      }

      return new RouterHttpRequestStats(
          metricsRepo,
          storeName,
          clusterName,
          requestType,
          stats,
          isKeyValueProfilingEnabled);
    });
  }

  public ScatterGatherStats getScatterGatherStatsForStore(String storeName) {
    return scatterGatherStatsMap.computeIfAbsent(storeName, k -> new ScatterGatherStats());
  }

  public void recordRequest(String storeName) {
    totalStats.recordIncomingRequest();
    getStoreStats(storeName).recordIncomingRequest();
  }

  public void recordHealthyRequest(String storeName, double latency, HttpResponseStatus responseStatus) {
    totalStats.recordHealthyRequest(latency, responseStatus);
    getStoreStats(storeName).recordHealthyRequest(latency, responseStatus);
  }

  public void recordUnhealthyRequest(String storeName, HttpResponseStatus responseStatus) {
    totalStats.recordUnhealthyRequest(responseStatus);
    if (storeName != null) {
      getStoreStats(storeName).recordUnhealthyRequest(responseStatus);
    }
  }

  public void recordUnavailableReplicaStreamingRequest(String storeName) {
    totalStats.recordUnavailableReplicaStreamingRequest();
    getStoreStats(storeName).recordUnavailableReplicaStreamingRequest();
  }

  public void recordUnhealthyRequest(String storeName, double latency, HttpResponseStatus responseStatus) {
    totalStats.recordUnhealthyRequest(latency, responseStatus);
    if (storeName != null) {
      getStoreStats(storeName).recordUnhealthyRequest(latency, responseStatus);
    }
  }

  /**
   * Calculate read quota usage based on how many key/value pairs
   * are successfully returned from server.
   * @param storeName
   * @param quotaUsage
   */
  public void recordReadQuotaUsage(String storeName, int quotaUsage) {
    totalStats.recordReadQuotaUsage(quotaUsage);
    getStoreStats(storeName).recordReadQuotaUsage(quotaUsage);
  }

  public void recordTardyRequest(String storeName, double latency, HttpResponseStatus responseStatus) {
    totalStats.recordTardyRequest(latency, responseStatus);
    getStoreStats(storeName).recordTardyRequest(latency, responseStatus);
  }

  /**
   * Once we stop reporting throttled requests in {@link com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils},
   * and we only report them in {@link com.linkedin.venice.router.api.VeniceResponseAggregator} then we will always have
   * a latency and we'll be able to remove this overload.
   *
   * TODO: Remove this overload after fixing the above.
   */
  public void recordThrottledRequest(String storeName, HttpResponseStatus httpResponseStatus) {
    totalStats.recordThrottledRequest(httpResponseStatus);
    getStoreStats(storeName).recordThrottledRequest(httpResponseStatus);
  }

  public void recordThrottledRequest(String storeName, double latency, HttpResponseStatus httpResponseStatus) {
    totalStats.recordThrottledRequest(latency, httpResponseStatus);
    getStoreStats(storeName).recordThrottledRequest(latency, httpResponseStatus);
  }

  public void recordBadRequest(String storeName, HttpResponseStatus responseStatus) {
    totalStats.recordBadRequest(responseStatus);
    if (storeName != null) {
      getStoreStats(storeName).recordBadRequest(responseStatus);
    }
  }

  public void recordBadRequestKeyCount(String storeName, int keyCount) {
    totalStats.recordBadRequestKeyCount(keyCount);
    if (storeName != null) {
      getStoreStats(storeName).recordBadRequestKeyCount(keyCount);
    }
  }

  public void recordRequestThrottledByRouterCapacity(String storeName) {
    totalStats.recordRequestThrottledByRouterCapacity();
    if (storeName != null) {
      getStoreStats(storeName).recordRequestThrottledByRouterCapacity();
    }
  }

  public void recordErrorRetryCount(String storeName) {
    totalStats.recordErrorRetryCount();
    if (storeName != null) {
      getStoreStats(storeName).recordErrorRetryCount();
    }
  }

  public void recordFanoutRequestCount(String storeName, int count) {
    totalStats.recordFanoutRequestCount(count);
    getStoreStats(storeName).recordFanoutRequestCount(count);
  }

  public void recordLatency(String storeName, double latency) {
    totalStats.recordLatency(latency);
    if (storeName != null) {
      getStoreStats(storeName).recordLatency(latency);
    }
  }

  public void recordResponseWaitingTime(String storeName, double waitingTime) {
    totalStats.recordResponseWaitingTime(waitingTime);
    getStoreStats(storeName).recordResponseWaitingTime(waitingTime);
  }

  public void recordRequestSize(String storeName, double keySize) {
    totalStats.recordRequestSize(keySize);
    getStoreStats(storeName).recordRequestSize(keySize);
  }

  public void recordCompressedResponseSize(String storeName, double compressedResponseSize) {
    totalStats.recordCompressedResponseSize(compressedResponseSize);
    getStoreStats(storeName).recordCompressedResponseSize(compressedResponseSize);
  }

  public void recordResponseSize(String storeName, double valueSize) {
    totalStats.recordResponseSize(valueSize);
    getStoreStats(storeName).recordResponseSize(valueSize);
  }

  public void recordDecompressionTime(String storeName, double decompressionTime) {
    totalStats.recordDecompressionTime(decompressionTime);
    getStoreStats(storeName).recordDecompressionTime(decompressionTime);
  }

  public void recordQuota(String storeName, double quota) {
    getStoreStats(storeName).recordQuota(quota);
  }

  public void recordTotalQuota(double totalQuota) {
    totalStats.recordQuota(totalQuota);
  }

  public void recordFindUnhealthyHostRequest(String storeName) {
    totalStats.recordFindUnhealthyHostRequest();
    getStoreStats(storeName).recordFindUnhealthyHostRequest();
  }

  public void recordResponse(String storeName) {
    totalStats.recordResponse();
    getStoreStats(storeName).recordResponse();
  }

  public void recordMetaStoreShadowRead(String storeName) {
    totalStats.recordMetaStoreShadowRead();
    getStoreStats(storeName).recordMetaStoreShadowRead();
  }

  private class AggScatterGatherStats extends ScatterGatherStats {
    private long getAggStats(Function<ScatterGatherStats, Long> func) {
      long total = 0;
      for (ScatterGatherStats stats: scatterGatherStatsMap.values()) {
        total += func.apply(stats);
      }
      return total;
    }

    @Override
    public long getTotalRetries() {
      return getAggStats(stats -> stats.getTotalRetries());
    }

    @Override
    public long getTotalRetriedKeys() {
      return getAggStats(stats -> stats.getTotalRetriedKeys());
    }

    @Override
    public long getTotalRetriesDiscarded() {
      return getAggStats(stats -> stats.getTotalRetriesDiscarded());
    }

    @Override
    public long getTotalRetriesWinner() {
      return getAggStats(stats -> stats.getTotalRetriesWinner());
    }

    @Override
    public long getTotalRetriesError() {
      return getAggStats(stats -> stats.getTotalRetriesError());
    }
  }

  public void recordKeyNum(String storeName, int keyNum) {
    totalStats.recordKeyNum(keyNum);
    getStoreStats(storeName).recordKeyNum(keyNum);
  }

  public void recordRequestUsage(String storeName, int usage) {
    totalStats.recordRequestUsage(usage);
    getStoreStats(storeName).recordRequestUsage(usage);
  }

  public void recordMultiGetFallback(String storeName, int keyCount) {
    totalStats.recordMultiGetFallback(keyCount);
    getStoreStats(storeName).recordMultiGetFallback(keyCount);
  }

  public void recordRequestParsingLatency(String storeName, double latency) {
    totalStats.recordRequestParsingLatency(latency);
    getStoreStats(storeName).recordRequestParsingLatency(latency);
  }

  public void recordRequestRoutingLatency(String storeName, double latency) {
    totalStats.recordRequestRoutingLatency(latency);
    getStoreStats(storeName).recordRequestRoutingLatency(latency);
  }

  public void recordUnavailableRequest(String storeName) {
    totalStats.recordUnavailableRequest();
    getStoreStats(storeName).recordUnavailableRequest();
  }

  public void recordDelayConstraintAbortedRetryRequest(String storeName) {
    totalStats.recordDelayConstraintAbortedRetryRequest();
    getStoreStats(storeName).recordDelayConstraintAbortedRetryRequest();
  }

  public void recordSlowRouteAbortedRetryRequest(String storeName) {
    totalStats.recordSlowRouteAbortedRetryRequest();
    getStoreStats(storeName).recordSlowRouteAbortedRetryRequest();
  }

  public void recordRetryRouteLimitAbortedRetryRequest(String storeName) {
    totalStats.recordRetryRouteLimitAbortedRetryRequest();
    getStoreStats(storeName).recordRetryRouteLimitAbortedRetryRequest();
  }

  public void recordKeySize(String storeName, long keySize) {
    totalStats.recordKeySizeInByte(keySize);
  }

  public void recordAllowedRetryRequest(String storeName) {
    totalStats.recordAllowedRetryRequest();
    getStoreStats(storeName).recordAllowedRetryRequest();
  }

  public void recordDisallowedRetryRequest(String storeName) {
    totalStats.recordDisallowedRetryRequest();
    getStoreStats(storeName).recordDisallowedRetryRequest();
  }

  public void recordNoAvailableReplicaAbortedRetryRequest(String storeName) {
    totalStats.recordNoAvailableReplicaAbortedRetryRequest();
    getStoreStats(storeName).recordRetryRouteLimitAbortedRetryRequest();
  }

  public void recordErrorRetryAttemptTriggeredByPendingRequestCheck(String storeName) {
    totalStats.recordErrorRetryAttemptTriggeredByPendingRequestCheck();
    getStoreStats(storeName).recordErrorRetryAttemptTriggeredByPendingRequestCheck();
  }

  public void recordRetryDelay(String storeName, double delay) {
    totalStats.recordRetryDelay(delay);
    getStoreStats(storeName).recordRetryDelay(delay);
  }
}
