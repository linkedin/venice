package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.TrackingStreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseCompletableFuture;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.client.store.streaming.VeniceResponseMapImpl;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.read.RequestType;
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
import java.util.concurrent.atomic.AtomicInteger;
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

  private static String STAT_VENICE_CLIENT_NAME = "venice_client";
  private static String STAT_SCHEMA_READER = "schema_reader";

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
    this.singleGetStats = ClientStats
        .getClientStats(metricsRepository, innerStoreClient.getStoreName(), RequestType.SINGLE_GET, clientConfig);
    this.multiGetStats = ClientStats
        .getClientStats(metricsRepository, innerStoreClient.getStoreName(), RequestType.MULTI_GET, clientConfig);
    this.multiGetStreamingStats = ClientStats.getClientStats(
        metricsRepository,
        innerStoreClient.getStoreName(),
        RequestType.MULTI_GET_STREAMING,
        clientConfig);
    this.schemaReaderStats = ClientStats.getClientStats(
        metricsRepository,
        innerStoreClient.getStoreName() + "_" + STAT_SCHEMA_READER,
        RequestType.SINGLE_GET,
        clientConfig);
    this.computeStats = ClientStats
        .getClientStats(metricsRepository, innerStoreClient.getStoreName(), RequestType.COMPUTE, clientConfig);
    this.computeStreamingStats = ClientStats.getClientStats(
        metricsRepository,
        innerStoreClient.getStoreName(),
        RequestType.COMPUTE_STREAMING,
        clientConfig);
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
    CompletableFuture<Map<K, V>> resultFuture = new CompletableFuture<>();
    CompletableFuture<VeniceResponseMap<K, V>> streamingResultFuture = streamingBatchGet(keys);

    streamingResultFuture.whenComplete((response, throwable) -> {
      if (throwable != null) {
        resultFuture.completeExceptionally(throwable);
      } else if (!response.isFullResponse()) {
        resultFuture.completeExceptionally(
            new VeniceClientException(
                "Received partial response, returned entry count: " + response.getTotalEntryCount()
                    + ", and key count: " + keys.size()));
      } else {
        resultFuture.complete(response);
      }
    });
    // We intentionally use stats for batch-get streaming since blocking impl of batch-get is deprecated.
    return AppTimeOutTrackingCompletableFuture.track(resultFuture, multiGetStreamingStats);
  }

  @Override
  public CompletableFuture<VeniceResponseMap<K, V>> streamingBatchGet(Set<K> keys) throws VeniceClientException {
    Map<K, V> resultMap = new VeniceConcurrentHashMap<>(keys.size());
    Queue<K> nonExistingKeyList = new ConcurrentLinkedQueue<>();

    VeniceResponseCompletableFuture<VeniceResponseMap<K, V>> resultFuture = new VeniceResponseCompletableFuture<>(
        () -> new VeniceResponseMapImpl(resultMap, nonExistingKeyList, false),
        keys.size(),
        Optional.of(multiGetStreamingStats));
    streamingBatchGet(keys, new StreamingCallback<K, V>() {
      @Override
      public void onRecordReceived(K key, V value) {
        if (value != null) {
          /**
           * {@link java.util.concurrent.ConcurrentHashMap#put} won't take 'null' as the value.
           */
          resultMap.put(key, value);
        } else {
          nonExistingKeyList.add(key);
        }
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        if (exception.isPresent()) {
          resultFuture.completeExceptionally(exception.get());
        } else {
          boolean isFullResponse = (resultMap.size() + nonExistingKeyList.size() == keys.size());
          resultFuture.complete(new VeniceResponseMapImpl(resultMap, nonExistingKeyList, isFullResponse));
        }
      }
    });
    return resultFuture;
  }

  public void recordRetryCount(RequestType requestType) {
    if (requestType == RequestType.SINGLE_GET) {
      singleGetStats.recordRequestRetryCount();
    } else if (requestType == RequestType.MULTI_GET) {
      multiGetStats.recordRequestRetryCount();
    } else if (requestType == RequestType.COMPUTE) {
      computeStats.recordRequestRetryCount();
    }
  }

  private static class StatTrackingStreamingCallback<K, V> extends TrackingStreamingCallback<K, V> {
    private final ClientStats stats;
    private final int keyCntForP50;
    private final int keyCntForP90;
    private final int keyCntForP95;
    private final int keyCntForP99;
    private final long preRequestTimeInNS;
    private final AtomicInteger receivedKeyCnt = new AtomicInteger(0);

    public StatTrackingStreamingCallback(
        StreamingCallback<K, V> callback,
        ClientStats stats,
        int keyCnt,
        long preRequestTimeInNS) {
      super(callback);
      this.stats = stats;
      this.keyCntForP50 = keyCnt / 2;
      this.keyCntForP90 = keyCnt * 9 / 10;
      this.keyCntForP95 = keyCnt * 95 / 100;
      this.keyCntForP99 = keyCnt * 99 / 100;
      this.preRequestTimeInNS = preRequestTimeInNS;
    }

    @Override
    public ClientStats getStats() {
      return stats;
    }

    @Override
    public void onRecordDeserialized() {
      int currentKeyCnt = receivedKeyCnt.incrementAndGet();
      /**
       * Here is not short-circuiting because the key cnt for each percentile could be same if the total key count
       * is very small.
       */
      if (currentKeyCnt == 1) {
        stats.recordStreamingResponseTimeToReceiveFirstRecord(LatencyUtils.getLatencyInMS(preRequestTimeInNS));
      }
      if (currentKeyCnt == keyCntForP50) {
        stats.recordStreamingResponseTimeToReceive50PctRecord(LatencyUtils.getLatencyInMS(preRequestTimeInNS));
      }
      if (currentKeyCnt == keyCntForP90) {
        stats.recordStreamingResponseTimeToReceive90PctRecord(LatencyUtils.getLatencyInMS(preRequestTimeInNS));
      }
      if (currentKeyCnt == keyCntForP95) {
        stats.recordStreamingResponseTimeToReceive95PctRecord(LatencyUtils.getLatencyInMS(preRequestTimeInNS));
      }
      if (currentKeyCnt == keyCntForP99) {
        stats.recordStreamingResponseTimeToReceive99PctRecord(LatencyUtils.getLatencyInMS(preRequestTimeInNS));
      }
    }

    @Override
    public void onDeserializationCompletion(
        Optional<VeniceClientException> veniceException,
        int successKeyCnt,
        int duplicateEntryCnt) {
      handleMetricTrackingForStreamingCallback(
          stats,
          preRequestTimeInNS,
          veniceException,
          successKeyCnt,
          duplicateEntryCnt);
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

  private static void handleMetricTrackingForStreamingCallback(
      ClientStats clientStats,
      long startTimeInNS,
      Optional<VeniceClientException> veniceException,
      int successKeyCnt,
      int duplicateEntryCnt) {
    double latency = LatencyUtils.getLatencyInMS(startTimeInNS);
    if (veniceException.isPresent()) {
      clientStats.recordUnhealthyRequest();
      clientStats.recordUnhealthyLatency(latency);

      if (veniceException.get() instanceof VeniceClientHttpException) {
        VeniceClientHttpException httpException = (VeniceClientHttpException) veniceException.get();
        clientStats.recordHttpRequest(httpException.getHttpStatus());
      } else {
        // Http related exception logging is being taken care by underlying transporting layer,
        // and here will dump other kinds of exceptions
        LOGGER.error("Received exception in streaming callback", veniceException.get());
      }
    } else {
      emitRequestHealthyMetrics(clientStats, latency);
    }
    clientStats.recordSuccessRequestKeyCount(successKeyCnt);
    clientStats.recordSuccessDuplicateRequestKeyCount(duplicateEntryCnt);
  }

  @Override
  public ComputeRequestBuilder<K> compute() throws VeniceClientException {
    long startTimeInNS = System.nanoTime();

    /**
     * Here, we have to use {@link #compute(Optional, InternalAvroStoreClient, long)}
     * to pass {@link StatTrackingStoreClient}, so that {@link #compute(ComputeRequestWrapper, Set, Schema, Optional, long)}
     * will be invoked when serving 'compute' request.
     */
    return super.compute(Optional.of(computeStats), Optional.of(computeStreamingStats), this, startTimeInNS);
  }

  private static void emitRequestHealthyMetrics(ClientStats clientStats, double latency) {
    clientStats.recordHealthyRequest();
    clientStats.recordHealthyLatency(latency);
  }

  public static <T> BiFunction<? super T, Throwable, ? extends T> getStatCallback(
      ClientStats clientStats,
      long startTimeInNS) {
    return (T value, Throwable throwable) -> {
      double latency = LatencyUtils.getLatencyInMS(startTimeInNS);
      if (throwable != null) {
        clientStats.recordUnhealthyRequest();
        clientStats.recordUnhealthyLatency(latency);
        if (throwable instanceof VeniceClientHttpException) {
          VeniceClientHttpException httpException = (VeniceClientHttpException) throwable;
          clientStats.recordHttpRequest(httpException.getHttpStatus());
        }
        handleStoreExceptionInternally(throwable);
      }
      emitRequestHealthyMetrics(clientStats, latency);

      if (value == null) {
        clientStats.recordSuccessRequestKeyCount(0);
      } else if (value instanceof Map) {
        clientStats.recordSuccessRequestKeyCount(((Map) value).size());
      } else {
        clientStats.recordSuccessRequestKeyCount(1);
      }
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
