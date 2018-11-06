package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.LatencyUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.utils.Time;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * This class is used to handle all the metric related logic.
 * @param <V>
 */
public class StatTrackingStoreClient<K, V> extends DelegatingStoreClient<K, V> {
  public static String STAT_VENICE_CLIENT_NAME = "venice_client";
  public static String STAT_SCHEMA_READER = "schema_reader";

  //TODO: do we want it to be configurable?
  //TODO: we should use a different timeout for multi-get
  public static final int TIMEOUT_IN_SECOND = 5;

  private final ClientStats singleGetStats;
  private final ClientStats multiGetStats;
  private final ClientStats schemaReaderStats;

  public StatTrackingStoreClient(InternalAvroStoreClient<K, V> innerStoreClient) {
    this(innerStoreClient, TehutiUtils.getMetricsRepository(STAT_VENICE_CLIENT_NAME));
  }

  public StatTrackingStoreClient(InternalAvroStoreClient<K, V> innerStoreClient, MetricsRepository metricsRepository) {
    super(innerStoreClient);
    this.singleGetStats = new ClientStats(metricsRepository, getStoreName(), RequestType.SINGLE_GET);
    this.multiGetStats = new ClientStats(metricsRepository, getStoreName(), RequestType.MULTI_GET);
    this.schemaReaderStats =
        new ClientStats(metricsRepository, getStoreName() + "_" + STAT_SCHEMA_READER, RequestType.SINGLE_GET);
  }

  @Override
  public CompletableFuture<V> get(K key) {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<V> innerFuture = super.get(key, Optional.of(singleGetStats), startTimeInNS);
    singleGetStats.recordRequestKeyCount(1);
    CompletableFuture<V> statFuture = innerFuture.handle(
        (BiFunction<? super V, Throwable, ? extends V>) getStatCallback(singleGetStats, startTimeInNS));
    return statFuture;
  }

  @Override
  public CompletableFuture<byte[]> getRaw(String requestPath) {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<byte[]> innerFuture = super.getRaw(requestPath, Optional.of(schemaReaderStats), startTimeInNS);
    schemaReaderStats.recordRequestKeyCount(1);
    CompletableFuture<byte[]> statFuture = innerFuture.handle(
        (BiFunction<? super byte[], Throwable, ? extends byte[]>) getStatCallback(schemaReaderStats, startTimeInNS));
    return statFuture;
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) throws VeniceClientException {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<Map<K, V>> innerFuture = super.batchGet(keys, Optional.of(multiGetStats), startTimeInNS);
    multiGetStats.recordRequestKeyCount(keys.size());
    CompletableFuture<Map<K, V>> statFuture = innerFuture.handle(
        (BiFunction<? super Map<K, V>, Throwable, ? extends Map<K, V>>) getStatCallback(multiGetStats, startTimeInNS));
    return statFuture;
  }

  private <T> BiFunction<? super T, Throwable, ? extends T> getStatCallback(
      ClientStats clientStats, long startTimeInNS) {
    return (T value, Throwable throwable) -> {
      double latency = LatencyUtils.getLatencyInMS(startTimeInNS);
      if (null != throwable) {
        clientStats.recordUnhealthyRequest();
        clientStats.recordUnhealthyLatency(latency);
        if (throwable.getCause() instanceof VeniceClientHttpException) {
          VeniceClientHttpException httpException = (VeniceClientHttpException)throwable.getCause();
          clientStats.recordHttpRequest(httpException.getHttpStatus());
        }
        handleStoreExceptionInternally(throwable);
      }
      if (latency > TIMEOUT_IN_SECOND * Time.MS_PER_SECOND) {
        clientStats.recordUnhealthyRequest();
        clientStats.recordUnhealthyLatency(latency);
      } else {
        clientStats.recordHealthyRequest();
        clientStats.recordHealthyLatency(latency);
      }

      if (value == null) {
        clientStats.recordSuccessRequestKeyCount(0);
      } else if (value instanceof Map) {
        clientStats.recordSuccessRequestKeyCount(((Map)value).size());
      } else {
        clientStats.recordSuccessRequestKeyCount(1);
      }
      return value;
    };
  }
}
