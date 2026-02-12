package com.linkedin.venice.router.stats;

import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.stats.AbstractVeniceAggStoreStats;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;


public class AggRouterHttpRequestStats extends AbstractVeniceAggStoreStats<RouterHttpRequestStats> {
  private final Map<String, ScatterGatherStats> scatterGatherStatsMap = new VeniceConcurrentHashMap<>();
  private final boolean isStoreStatsEnabled;

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
        isUnregisterMetricForDeletedStoreEnabled,
        null);
  }

  public AggRouterHttpRequestStats(
      String cluster,
      MetricsRepository metricsRepository,
      RequestType requestType,
      boolean isKeyValueProfilingEnabled,
      ReadOnlyStoreRepository metadataRepository,
      boolean isUnregisterMetricForDeletedStoreEnabled,
      Sensor totalInFlightRequestSensor) {
    super(cluster, metricsRepository, metadataRepository, isUnregisterMetricForDeletedStoreEnabled);
    // Disable store level non-streaming multi get stats reporting because it's no longer used in clients. We still
    // report to the total stats for visibility of potential old clients.
    isStoreStatsEnabled = !RequestType.MULTI_GET.equals(requestType);
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
          isKeyValueProfilingEnabled,
          totalInFlightRequestSensor);
    });
  }

  public ScatterGatherStats getScatterGatherStatsForStore(String storeName) {
    return scatterGatherStatsMap.computeIfAbsent(storeName, k -> new ScatterGatherStats());
  }

  private void recordStoreStats(String storeName, Consumer<RouterHttpRequestStats> statsConsumer) {
    if (isStoreStatsEnabled) {
      statsConsumer.accept(getStoreStats(storeName));
    }
  }

  public void recordRequest(String storeName) {
    totalStats.recordIncomingRequest();
    recordStoreStats(storeName, RouterHttpRequestStats::recordIncomingRequest);
  }

  public void recordHealthyRequest(String storeName, double latency, HttpResponseStatus responseStatus, int keyNum) {
    totalStats.recordHealthyRequest(latency, responseStatus, keyNum);
    recordStoreStats(storeName, stats -> stats.recordHealthyRequest(latency, responseStatus, keyNum));
  }

  public void recordUnhealthyRequest(String storeName, HttpResponseStatus responseStatus) {
    totalStats.recordUnhealthyRequest(responseStatus);
    if (storeName != null) {
      recordStoreStats(storeName, stats -> stats.recordUnhealthyRequest(responseStatus));
    }
  }

  public void recordUnavailableReplicaStreamingRequest(String storeName) {
    totalStats.recordUnavailableReplicaStreamingRequest();
    recordStoreStats(storeName, RouterHttpRequestStats::recordUnavailableReplicaStreamingRequest);
  }

  public void recordUnhealthyRequest(String storeName, double latency, HttpResponseStatus responseStatus, int keyNum) {
    totalStats.recordUnhealthyRequest(latency, responseStatus, keyNum);
    if (storeName != null) {
      recordStoreStats(storeName, stats -> stats.recordUnhealthyRequest(latency, responseStatus, keyNum));
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
    recordStoreStats(storeName, stats -> stats.recordReadQuotaUsage(quotaUsage));
  }

  public void recordTardyRequest(String storeName, double latency, HttpResponseStatus responseStatus, int keyNum) {
    totalStats.recordTardyRequest(latency, responseStatus, keyNum);
    recordStoreStats(storeName, stats -> stats.recordTardyRequest(latency, responseStatus, keyNum));
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
    recordStoreStats(storeName, stats -> stats.recordThrottledRequest(httpResponseStatus));
  }

  public void recordThrottledRequest(
      String storeName,
      double latency,
      HttpResponseStatus httpResponseStatus,
      int keyNum) {
    totalStats.recordThrottledRequest(latency, httpResponseStatus, keyNum);
    recordStoreStats(storeName, stats -> stats.recordThrottledRequest(latency, httpResponseStatus, keyNum));
  }

  public void recordBadRequest(String storeName, HttpResponseStatus responseStatus) {
    totalStats.recordBadRequest(responseStatus);
    if (storeName != null) {
      recordStoreStats(storeName, stats -> stats.recordBadRequest(responseStatus));
    }
  }

  public void recordBadRequestKeyCount(String storeName, HttpResponseStatus responseStatus, int keyNum) {
    totalStats.recordIncomingBadRequestKeyCountMetric(responseStatus, keyNum);
    if (storeName != null) {
      recordStoreStats(storeName, stats -> stats.recordIncomingBadRequestKeyCountMetric(responseStatus, keyNum));
    }
  }

  public void recordRequestThrottledByRouterCapacity(String storeName) {
    totalStats.recordRequestThrottledByRouterCapacity();
    if (storeName != null) {
      recordStoreStats(storeName, RouterHttpRequestStats::recordRequestThrottledByRouterCapacity);
    }
  }

  public void recordErrorRetryCount(String storeName) {
    totalStats.recordErrorRetryCount();
    if (storeName != null) {
      recordStoreStats(storeName, RouterHttpRequestStats::recordErrorRetryCount);
    }
  }

  public void recordFanoutRequestCount(String storeName, int count) {
    totalStats.recordFanoutRequestCount(count);
    recordStoreStats(storeName, stats -> stats.recordFanoutRequestCount(count));
  }

  public void recordLatency(String storeName, double latency) {
    totalStats.recordLatency(latency);
    if (storeName != null) {
      recordStoreStats(storeName, stats -> stats.recordLatency(latency));
    }
  }

  public void recordResponseWaitingTime(String storeName, double waitingTime) {
    totalStats.recordResponseWaitingTime(waitingTime);
    recordStoreStats(storeName, stats -> stats.recordResponseWaitingTime(waitingTime));
  }

  public void recordRequestSize(String storeName, double keySize) {
    totalStats.recordRequestSize(keySize);
    recordStoreStats(storeName, stats -> stats.recordRequestSize(keySize));
  }

  public void recordCompressedResponseSize(String storeName, double compressedResponseSize) {
    totalStats.recordCompressedResponseSize(compressedResponseSize);
    recordStoreStats(storeName, stats -> stats.recordCompressedResponseSize(compressedResponseSize));
  }

  public void recordDecompressedResponseSize(String storeName, double decompressedResponseSize) {
    totalStats.recordDecompressedResponseSize(decompressedResponseSize);
    recordStoreStats(storeName, stats -> stats.recordDecompressedResponseSize(decompressedResponseSize));
  }

  public void recordResponseSize(String storeName, double valueSize) {
    totalStats.recordResponseSize(valueSize);
    recordStoreStats(storeName, stats -> stats.recordResponseSize(valueSize));
  }

  public void recordDecompressionTime(String storeName, double decompressionTime) {
    totalStats.recordDecompressionTime(decompressionTime);
    recordStoreStats(storeName, stats -> stats.recordDecompressionTime(decompressionTime));
  }

  public void recordQuota(String storeName, double quota) {
    recordStoreStats(storeName, stats -> stats.recordQuota(quota));
  }

  public void recordTotalQuota(double totalQuota) {
    totalStats.recordQuota(totalQuota);
  }

  public void recordFindUnhealthyHostRequest(String storeName) {
    totalStats.recordFindUnhealthyHostRequest();
    recordStoreStats(storeName, RouterHttpRequestStats::recordFindUnhealthyHostRequest);
  }

  public void recordResponse(String storeName) {
    totalStats.recordResponse();
    recordStoreStats(storeName, RouterHttpRequestStats::recordResponse);
  }

  public void recordMetaStoreShadowRead(String storeName) {
    totalStats.recordMetaStoreShadowRead();
    recordStoreStats(storeName, RouterHttpRequestStats::recordMetaStoreShadowRead);
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
    totalStats.recordIncomingKeyCountMetric(keyNum);
    recordStoreStats(storeName, stats -> stats.recordIncomingKeyCountMetric(keyNum));
  }

  public void recordRequestUsage(String storeName, int usage) {
    totalStats.recordRequestUsage(usage);
    recordStoreStats(storeName, stats -> stats.recordRequestUsage(usage));
  }

  public void recordMultiGetFallback(String storeName, int keyCount) {
    totalStats.recordMultiGetFallback(keyCount);
    recordStoreStats(storeName, stats -> stats.recordMultiGetFallback(keyCount));
  }

  public void recordRequestParsingLatency(String storeName, double latency) {
    totalStats.recordRequestParsingLatency(latency);
    recordStoreStats(storeName, stats -> stats.recordRequestParsingLatency(latency));
  }

  public void recordRequestRoutingLatency(String storeName, double latency) {
    totalStats.recordRequestRoutingLatency(latency);
    recordStoreStats(storeName, stats -> stats.recordRequestRoutingLatency(latency));
  }

  public void recordPipelineLatency(String storeName, double latency) {
    totalStats.recordPipelineLatency(latency);
    recordStoreStats(storeName, stats -> stats.recordPipelineLatency(latency));
  }

  public void recordScatterLatency(String storeName, double latency) {
    totalStats.recordScatterLatency(latency);
    recordStoreStats(storeName, stats -> stats.recordScatterLatency(latency));
  }

  public void recordQueueLatency(String storeName, double latency) {
    totalStats.recordQueueLatency(latency);
    recordStoreStats(storeName, stats -> stats.recordQueueLatency(latency));
  }

  public void recordDispatchLatency(String storeName, double latency) {
    totalStats.recordDispatchLatency(latency);
    recordStoreStats(storeName, stats -> stats.recordDispatchLatency(latency));
  }

  public void recordUnavailableRequest(String storeName) {
    totalStats.recordUnavailableRequest();
    recordStoreStats(storeName, RouterHttpRequestStats::recordUnavailableRequest);
  }

  public void recordDelayConstraintAbortedRetryRequest(String storeName) {
    totalStats.recordDelayConstraintAbortedRetryCountMetric();
    recordStoreStats(storeName, RouterHttpRequestStats::recordDelayConstraintAbortedRetryCountMetric);
  }

  public void recordSlowRouteAbortedRetryRequest(String storeName) {
    totalStats.recordSlowRouteAbortedRetryCountMetric();
    recordStoreStats(storeName, RouterHttpRequestStats::recordSlowRouteAbortedRetryCountMetric);
  }

  public void recordRetryRouteLimitAbortedRetryRequest(String storeName) {
    totalStats.recordRetryRouteLimitAbortedRetryCountMetric();
    recordStoreStats(storeName, RouterHttpRequestStats::recordRetryRouteLimitAbortedRetryCountMetric);
  }

  public void recordKeySize(long keySize) {
    totalStats.recordKeySizeInByte(keySize);
  }

  public void recordAllowedRetryRequest(String storeName) {
    totalStats.recordAllowedRetryRequest();
    recordStoreStats(storeName, RouterHttpRequestStats::recordAllowedRetryRequest);
  }

  public void recordDisallowedRetryRequest(String storeName) {
    totalStats.recordDisallowedRetryRequest();
    recordStoreStats(storeName, RouterHttpRequestStats::recordDisallowedRetryRequest);
  }

  public void recordNoAvailableReplicaAbortedRetryRequest(String storeName) {
    totalStats.recordNoAvailableReplicaAbortedRetryCountMetric();
    recordStoreStats(storeName, RouterHttpRequestStats::recordNoAvailableReplicaAbortedRetryCountMetric);
  }

  public void recordErrorRetryAttemptTriggeredByPendingRequestCheck(String storeName) {
    totalStats.recordErrorRetryAttemptTriggeredByPendingRequestCheck();
    recordStoreStats(storeName, RouterHttpRequestStats::recordErrorRetryAttemptTriggeredByPendingRequestCheck);
  }

  public void recordRetryDelay(String storeName, double delay) {
    totalStats.recordRetryDelay(delay);
    recordStoreStats(storeName, stats -> stats.recordRetryDelay(delay));
  }
}
