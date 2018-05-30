package com.linkedin.venice.router.stats;

import com.linkedin.ddsstorage.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


public class AggRouterHttpRequestStats extends AbstractVeniceAggStats<RouterHttpRequestStats> {
  private final Map<String, ScatterGatherStats> scatterGatherStatsMap = new ConcurrentHashMap<>();

  public AggRouterHttpRequestStats(MetricsRepository metricsRepository, RequestType requestType) {
    super(metricsRepository);
    /**
     * Use a setter function to bypass the restriction that the supertype constructor could not
     * touch member fields of current object.
     */
    setStatsSupplier((metricsRepo, storeName) -> {
      ScatterGatherStats stats;
      if (storeName.equals(AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT)) {
        stats = new AggScatterGatherStats();
      } else {
        stats = scatterGatherStatsMap.computeIfAbsent(storeName, k -> new ScatterGatherStats());
      }

      return new RouterHttpRequestStats(metricsRepo, storeName, requestType, stats);
    });
  }

  public ScatterGatherStats getScatterGatherStatsForStore(String storeName) {
    scatterGatherStatsMap.computeIfAbsent(storeName, k -> new ScatterGatherStats());
    return scatterGatherStatsMap.get(storeName);
  }

  public void recordRequest(String storeName) {
    totalStats.recordRequest();
    getStoreStats(storeName).recordRequest();
  }

  public void recordHealthyRequest(String storeName) {
    totalStats.recordHealthyRequest();
    getStoreStats(storeName).recordHealthyRequest();
  }

  public void recordUnhealthyRequest(String storeName) {
    totalStats.recordUnhealthyRequest();
    getStoreStats(storeName).recordUnhealthyRequest();
  }
  public void recordUnhealthyRequest() {
    totalStats.recordUnhealthyRequest();
  }

  public void recordThrottledRequest(String storeName){
    totalStats.recordThrottledRequest();
    getStoreStats(storeName).recordThrottledRequest();
  }

  public void recordBadRequest(String storeName) {
    totalStats.recordBadRequest();
    getStoreStats(storeName).recordBadRequest();
  }

  public void recordBadRequest() {
    totalStats.recordBadRequest();
  }

  public void recordFanoutRequestCount(String storeName, int count) {
    totalStats.recordFanoutRequestCount(count);
    getStoreStats(storeName).recordFanoutRequestCount(count);
  }

  public void recordLatency(String storeName, double latency) {
    totalStats.recordLatency(latency);
    getStoreStats(storeName).recordLatency(latency);
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

  private class AggScatterGatherStats extends ScatterGatherStats {

    private long getAggStats(Function<ScatterGatherStats, Long> func) {
      long total = 0;
      for (ScatterGatherStats stats : scatterGatherStatsMap.values()) {
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

  public void recordCacheLookupRequest(String storeName) {
    totalStats.recordCacheLookupRequest();
    getStoreStats(storeName).recordCacheLookupRequest();
  }

  public void recordCacheHitRequest(String storeName) {
    totalStats.recordCacheHitRequest();
    getStoreStats(storeName).recordCacheHitRequest();
  }

  public void recordCacheLookupLatency(String storeName, double latency) {
    totalStats.recordCacheLookupLatency(latency);
    getStoreStats(storeName).recordCacheLookupLatency(latency);
  }

  public void recordCacheLookupLatencyForEachKeyInMultiget(String storeName, double latency) {
    totalStats.recordCacheLookupLatencyForEachKeyInMultiGet(latency);
    getStoreStats(storeName).recordCacheLookupLatencyForEachKeyInMultiGet(latency);
  }

  public void recordCacheResultSerializationLatency(String storeName, double latency) {
    totalStats.recordCacheResultSerializationLatency(latency);
    getStoreStats(storeName).recordCacheResultSerializationLatency(latency);
  }

  public void recordResponseResultsDeserializationLatency(String storeName, double latency) {
    totalStats.recordResponseResultsDeserializationLatency(latency);
    getStoreStats(storeName).recordResponseResultsDeserializationLatency(latency);
  }

  public void recordCacheUpdateLatencyForMultiGet(String storeName, double latency) {
    totalStats.recordCacheUpdateLatencyForMultiGet(latency);
    getStoreStats(storeName).recordCacheUpdateLatencyForMultiGet(latency);
  }

  public void recordCachePutRequest(String storeName) {
    totalStats.recordCachePutRequest();
    getStoreStats(storeName).recordCachePutRequest();
  }

  public void recordCachePutLatency(String storeName, double latency) {
    totalStats.recordCachePutLatency(latency);
    getStoreStats(storeName).recordCachePutLatency(latency);
  }

  public void recordKeyNum(String storeName, int keyNum) {
    totalStats.recordKeyNum(keyNum);
    getStoreStats(storeName).recordKeyNum(keyNum);
  }

  public void recordRequestUsage(String storeName, int usage) {
    totalStats.recordRequestUsage(usage);
    getStoreStats(storeName).recordRequestUsage(usage);
  }
}
