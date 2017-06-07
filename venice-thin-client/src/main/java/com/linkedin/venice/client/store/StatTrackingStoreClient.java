package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.utils.Time;

import java.util.Map;
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

  private final MetricsRepository metricsRepository;
  private final ClientStats singleGetStats;
  private final ClientStats multiGetStats;
  private final ClientStats schemaReaderStats;

  public StatTrackingStoreClient(InternalAvroStoreClient<K, V> innerStoreClient) {
    this(innerStoreClient, TehutiUtils.getMetricsRepository(STAT_VENICE_CLIENT_NAME));
  }

  public StatTrackingStoreClient(InternalAvroStoreClient<K, V> innerStoreClient, MetricsRepository metricsRepository) {
    super(innerStoreClient);
    this.metricsRepository = metricsRepository;
    this.singleGetStats = new ClientStats(metricsRepository, STAT_VENICE_CLIENT_NAME, RequestType.SINGLE_GET);
    this.multiGetStats = new ClientStats(metricsRepository, STAT_VENICE_CLIENT_NAME, RequestType.MULTI_GET);
    this.schemaReaderStats = new ClientStats(metricsRepository, STAT_SCHEMA_READER, RequestType.SINGLE_GET);
  }

  @Override
  public CompletableFuture<V> get(K key) {
    long startTime = System.currentTimeMillis();
    CompletableFuture<V> innerFuture = super.get(key);
    CompletableFuture<V> statFuture = innerFuture.handle(
        (BiFunction<? super V, Throwable, ? extends V>) getStatCallback(
            singleGetStats, startTime
        )
    );

    return statFuture;
  }

  private <T> BiFunction<? super T, Throwable, ? extends T> getStatCallback(
      ClientStats clientStats, long startTime) {
    return (value, throwable) -> {
      long latency = System.currentTimeMillis() - startTime;
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
      return value;
    };
  }

  @Override
  public CompletableFuture<byte[]> getRaw(String requestPath) {
    long startTime = System.currentTimeMillis();
    CompletableFuture<byte[]> innerFuture = super.getRaw(requestPath);
    CompletableFuture<byte[]> statFuture = innerFuture.handle(
        (BiFunction<? super byte[], Throwable, ? extends byte[]>) getStatCallback(
            schemaReaderStats, startTime
        )
    );

    return statFuture;
  }

  @Override
  public CompletableFuture<Map<K, V>> multiGet(Set<K> keys) throws VeniceClientException
  {
    long startTime = System.currentTimeMillis();
    CompletableFuture<Map<K, V>> innerFuture = super.multiGet(keys);
    CompletableFuture<Map<K, V>> statFuture = innerFuture.handle(
        (BiFunction<? super Map<K, V>, Throwable, ? extends Map<K, V>>) getStatCallback(
            multiGetStats, startTime
        )
    );
    return statFuture;
  }

}
