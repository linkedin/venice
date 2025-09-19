package com.linkedin.venice.client.store;

import static com.linkedin.venice.client.stats.BasicClientStats.getSuccessfulKeyCount;
import static com.linkedin.venice.client.stats.BasicClientStats.getUnhealthyRequestHttpStatus;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.streaming.DelegatingTrackingCallback;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.StreamingResponseTracker;
import com.linkedin.venice.client.store.streaming.VeniceResponseCompletableFuture;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.client.store.streaming.VeniceResponseMapImpl;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.ClientType;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiFunction;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to handle all the metric related logic.
 * @param <V>
 */
public class StatTrackingStoreClient<K, V> extends DelegatingStoreClient<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(StatTrackingStoreClient.class);

  private static final String STAT_VENICE_CLIENT_NAME = "venice_client";
  private static final String STAT_SCHEMA_READER = "schema_reader";

  private final ClientStats singleGetStats;
  private final ClientStats multiGetStats;
  private final ClientStats multiGetStreamingStats;
  private final ClientStats schemaReaderStats;
  private final ClientStats computeStats;
  private final ClientStats computeStreamingStats;

  public StatTrackingStoreClient(InternalAvroStoreClient<K, V> innerStoreClient, ClientConfig clientConfig) {
    super(innerStoreClient);
    MetricsRepository metricsRepository = Optional.ofNullable(clientConfig.getMetricsRepository())
        .orElse(TehutiUtils.getMetricsRepository(STAT_VENICE_CLIENT_NAME));
    this.singleGetStats = ClientStats.getClientStats(
        metricsRepository,
        innerStoreClient.getStoreName(),
        RequestType.SINGLE_GET,
        clientConfig,
        ClientType.THIN_CLIENT);
    this.multiGetStats = ClientStats.getClientStats(
        metricsRepository,
        innerStoreClient.getStoreName(),
        RequestType.MULTI_GET,
        clientConfig,
        ClientType.THIN_CLIENT);
    this.multiGetStreamingStats = ClientStats.getClientStats(
        metricsRepository,
        innerStoreClient.getStoreName(),
        RequestType.MULTI_GET_STREAMING,
        clientConfig,
        ClientType.THIN_CLIENT);
    this.schemaReaderStats = ClientStats.getClientStats(
        metricsRepository,
        innerStoreClient.getStoreName() + "_" + STAT_SCHEMA_READER,
        RequestType.SINGLE_GET,
        clientConfig,
        ClientType.THIN_CLIENT);
    this.computeStats = ClientStats.getClientStats(
        metricsRepository,
        innerStoreClient.getStoreName(),
        RequestType.COMPUTE,
        clientConfig,
        ClientType.THIN_CLIENT);
    this.computeStreamingStats = ClientStats.getClientStats(
        metricsRepository,
        innerStoreClient.getStoreName(),
        RequestType.COMPUTE_STREAMING,
        clientConfig,
        ClientType.THIN_CLIENT);
  }

  @Override
  public CompletableFuture<V> get(K key) {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<V> innerFuture = super.get(key, Optional.of(singleGetStats), startTimeInNS);
    singleGetStats.recordRequestKeyCount(1);
    CompletableFuture<V> statFuture = innerFuture
        .handle((BiFunction<? super V, Throwable, ? extends V>) getStatCallback(singleGetStats, startTimeInNS));
    return AppTimeOutTrackingCompletableFuture.track(statFuture, singleGetStats);
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
    return AppTimeOutTrackingCompletableFuture.track(internalBatchGet(keys), multiGetStreamingStats);
  }

  @Override
  public CompletableFuture<VeniceResponseMap<K, V>> streamingBatchGet(Set<K> keys) throws VeniceClientException {
    Map<K, V> resultMap = new VeniceConcurrentHashMap<>(keys.size());
    Queue<K> nonExistingKeyList = new ConcurrentLinkedQueue<>();

    VeniceResponseCompletableFuture<VeniceResponseMap<K, V>> resultFuture = new VeniceResponseCompletableFuture<>(
        () -> new VeniceResponseMapImpl(resultMap, nonExistingKeyList, false),
        keys.size(),
        Optional.of(multiGetStreamingStats));
    streamingBatchGet(keys, super.getStreamingCallback(keys, resultMap, nonExistingKeyList, resultFuture));
    return resultFuture;
  }

  public void recordRetryCount(RequestType requestType) {
    if (requestType == RequestType.SINGLE_GET) {
      singleGetStats.recordErrorRetryRequest();
    } else if (requestType == RequestType.MULTI_GET) {
      multiGetStats.recordErrorRetryRequest();
    } else if (requestType == RequestType.COMPUTE) {
      computeStats.recordErrorRetryRequest();
    }
  }

  private static class StatTrackingStreamingCallback<K, V> extends DelegatingTrackingCallback<K, V> {
    private final ClientStats stats;
    private final Optional<ClientStats> statsOptional;
    private final long preRequestTimeInNS;
    private final StreamingResponseTracker streamingResponseTracker;

    public StatTrackingStreamingCallback(
        StreamingCallback<K, V> callback,
        ClientStats stats,
        int keyCnt,
        long preRequestTimeInNS) {
      super(callback);
      this.stats = stats;
      this.statsOptional = Optional.of(stats);
      this.preRequestTimeInNS = preRequestTimeInNS;
      streamingResponseTracker = new StreamingResponseTracker(stats, keyCnt, preRequestTimeInNS);
    }

    @Override
    public Optional<ClientStats> getStats() {
      return statsOptional;
    }

    @Override
    public void onRecordDeserialized() {
      streamingResponseTracker.recordReceived();
    }

    @Override
    public void onDeserializationCompletion(
        Optional<Exception> exception,
        int successKeyCount,
        int duplicateEntryCount) {
      handleMetricTrackingForStreamingCallback(
          stats,
          preRequestTimeInNS,
          exception,
          successKeyCount,
          duplicateEntryCount);
    }
  }

  @Override
  public void streamingBatchGet(Set<K> keys, StreamingCallback<K, V> callback) throws VeniceClientException {
    long preRequestTimeInNS = System.nanoTime();
    multiGetStreamingStats.recordRequestKeyCount(keys.size());
    super.streamingBatchGet(
        keys,
        new StatTrackingStreamingCallback<>(callback, multiGetStreamingStats, keys.size(), preRequestTimeInNS));
  }

  @Override
  public void compute(
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    computeStreamingStats.recordRequestKeyCount(keys.size());
    super.compute(
        computeRequestWrapper,
        keys,
        resultSchema,
        new StatTrackingStreamingCallback<>(callback, computeStreamingStats, keys.size(), preRequestTimeInNS),
        preRequestTimeInNS);
  }

  private static void handleUnhealthyRequest(ClientStats clientStats, Throwable throwable, double latency) {
    int httpStatus = getUnhealthyRequestHttpStatus(throwable);
    clientStats.emitUnhealthyRequestMetrics(latency, httpStatus);
    if (throwable instanceof VeniceClientHttpException) {
      clientStats.recordHttpRequest(httpStatus);
    }
  }

  private static void handleMetricTrackingForStreamingCallback(
      ClientStats clientStats,
      long startTimeInNS,
      Optional<Exception> exception,
      int successKeyCnt,
      int duplicateEntryCnt) {
    double latency = LatencyUtils.getElapsedTimeFromNSToMS(startTimeInNS);
    if (exception.isPresent()) {
      handleUnhealthyRequest(clientStats, exception.get(), latency);
    } else {
      clientStats.emitHealthyRequestMetrics(latency, successKeyCnt);
    }
    clientStats.recordResponseKeyCount(successKeyCnt);
    clientStats.recordSuccessDuplicateRequestKeyCount(duplicateEntryCnt);
  }

  @Override
  public ComputeRequestBuilder<K> compute() throws VeniceClientException {
    /**
     * Here, we have to use {@link #compute(Optional, InternalAvroStoreClient, long)}
     * to pass {@link StatTrackingStoreClient}, so that {@link #compute(ComputeRequestWrapper, Set, Schema, Optional, long)}
     * will be invoked when serving 'compute' request.
     */
    return super.compute(Optional.of(computeStreamingStats), this);
  }

  public static <T> BiFunction<? super T, Throwable, ? extends T> getStatCallback(
      ClientStats clientStats,
      long startTimeInNS) {
    return (T value, Throwable throwable) -> {
      double latency = LatencyUtils.getElapsedTimeFromNSToMS(startTimeInNS);
      if (throwable != null) {
        handleUnhealthyRequest(clientStats, throwable, latency);
        handleStoreExceptionInternally(throwable);
      }

      clientStats.emitHealthyRequestMetrics(latency, value);
      clientStats.recordResponseKeyCount(getSuccessfulKeyCount(value));
      return value;
    };
  }

  public static void handleStoreExceptionInternally(Throwable throwable) {
    if (throwable == null) {
      return;
    }
    /**
     * {@link CompletionException} could be thrown by {@link CompletableFuture#handle(BiFunction)}
     *
     * Eventually, {@link CompletableFuture#get()} will throw {@link ExecutionException}, which will replace
     * {@link CompletionException}, and its cause is the real root cause instead of {@link CompletionException}
     */
    if (throwable instanceof CompletionException) {
      throw (CompletionException) throwable;
    }
    /**
     * {@link VeniceClientException} could be thrown by {@link CompletableFuture#completeExceptionally(Throwable)}
     */
    if (throwable instanceof VeniceClientException) {
      throw (VeniceClientException) throwable;
    }
    throw new VeniceClientException(throwable);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "(storeName: " + getStoreName() + ")";
  }
}
