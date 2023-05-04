package com.linkedin.davinci.client;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.BasicClientStats;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.LatencyUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;


/**
 * Currently, we only expose metrics for single-get and batch-get requests, and if there
 * is a need to have metrics for other request types, we can add them later.
 *
 * So far, it only offers very basic metrics:
 * 1. Healthy request rate.
 * 2. Unhealthy request rate.
 * 3. Healthy request latency.
 * 4. Key count for batch-get request.
 * 5. Success request/key count ratio.
 */
public class StatsAvroGenericDaVinciClient<K, V> extends DelegatingAvroGenericDaVinciClient<K, V> {
  private final BasicClientStats clientStatsForSingleGet;
  private final BasicClientStats clientStatsForBatchGet;

  public StatsAvroGenericDaVinciClient(AvroGenericDaVinciClient<K, V> delegate, ClientConfig clientConfig) {
    super(delegate);
    MetricsRepository metricsRepository = clientConfig.getMetricsRepository();
    if (metricsRepository == null) {
      throw new VeniceClientException("MetricsRepository shouldn't be null");
    }
    this.clientStatsForSingleGet = BasicClientStats
        .getClientStats(metricsRepository, clientConfig.getStoreName(), RequestType.SINGLE_GET, clientConfig);
    this.clientStatsForBatchGet = BasicClientStats
        .getClientStats(metricsRepository, clientConfig.getStoreName(), RequestType.MULTI_GET, clientConfig);
  }

  private static <T> CompletableFuture<T> trackRequest(
      BasicClientStats stats,
      Supplier<CompletableFuture<T>> futureSupplier) {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<T> statFuture = new CompletableFuture<>();
    try {
      return futureSupplier.get().whenComplete((v, throwable) -> {
        if (throwable != null) {
          stats.recordUnhealthyRequest();
          statFuture.completeExceptionally(throwable);
        } else {
          stats.recordHealthyRequest();
          stats.recordHealthyLatency(LatencyUtils.getLatencyInMS(startTimeInNS));
          statFuture.complete(v);
        }
      });
    } catch (Exception e) {
      stats.recordUnhealthyRequest();
      throw e;
    }
  }

  @Override
  public CompletableFuture<V> get(K key) {
    return get(key, null);
  }

  @Override
  public CompletableFuture<V> get(K key, V reusableValue) {
    clientStatsForSingleGet.recordRequestKeyCount(1);
    return trackRequest(clientStatsForSingleGet, () -> super.get(key, reusableValue)).whenComplete((v, throwable) -> {
      if (throwable == null && v != null) {
        clientStatsForSingleGet.recordSuccessRequestKeyCount(1);
      }
    });
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) {
    clientStatsForBatchGet.recordRequestKeyCount(keys.size());
    return trackRequest(clientStatsForBatchGet, () -> super.batchGet(keys)).whenComplete((v, throwable) -> {
      if (throwable == null && v != null) {
        clientStatsForBatchGet.recordSuccessRequestKeyCount(v.size());
      }
    });
  }

}
