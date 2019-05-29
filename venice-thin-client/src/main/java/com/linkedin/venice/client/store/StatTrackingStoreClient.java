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
import io.tehuti.utils.Time;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * This class is used to handle all the metric related logic.
 * @param <V>
 */
public class StatTrackingStoreClient<K, V> extends DelegatingStoreClient<K, V> {
  private static String STAT_VENICE_CLIENT_NAME = "venice_client";
  private static String STAT_SCHEMA_READER = "schema_reader";

  //TODO: do we want it to be configurable?
  //TODO: we should use a different timeout for multi-get
  private static final int TIMEOUT_IN_SECOND = 5;

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
    this.singleGetStats = new ClientStats(metricsRepository, getStoreName(), RequestType.SINGLE_GET);
    this.multiGetStats = new ClientStats(metricsRepository, getStoreName(), RequestType.MULTI_GET);
    this.multiGetStreamingStats = new ClientStats(metricsRepository, getStoreName(), RequestType.MULTI_GET_STREAMING);
    this.schemaReaderStats =
        new ClientStats(metricsRepository, getStoreName() + "_" + STAT_SCHEMA_READER, RequestType.SINGLE_GET);
    this.computeStats = new ClientStats(metricsRepository, getStoreName(), RequestType.COMPUTE);
    this.computeStreamingStats = new ClientStats(metricsRepository, getStoreName(), RequestType.COMPUTE_STREAMING);
  }

  @Override
  public CompletableFuture<V> get(K key) {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<V> innerFuture = super.get(key, Optional.of(singleGetStats), startTimeInNS);
    singleGetStats.recordRequestKeyCount(1);
    CompletableFuture<V> statFuture = innerFuture.handle(
        (BiFunction<? super V, Throwable, ? extends V>) getStatCallback(singleGetStats, startTimeInNS));
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
    long startTimeInNS = System.nanoTime();
    CompletableFuture<Map<K, V>> innerFuture = super.batchGet(keys, Optional.of(multiGetStats), startTimeInNS);
    multiGetStats.recordRequestKeyCount(keys.size());
    CompletableFuture<Map<K, V>> statFuture = innerFuture.handle(
        (BiFunction<? super Map<K, V>, Throwable, ? extends Map<K, V>>) getStatCallback(multiGetStats, startTimeInNS));
    return AppTimeOutTrackingCompletableFuture.track(statFuture, multiGetStats);
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
          resultFuture.complete(new VeniceResponseMapImpl(resultMap, nonExistingKeyList, true));
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

    public StatTrackingStreamingCallback(StreamingCallback<K, V> callback, ClientStats stats, int keyCnt, long preRequestTimeInNS) {
      super(callback);
      this.stats = stats;
      this.keyCntForP50 = keyCnt / 2;
      this.keyCntForP90 = keyCnt * 9 / 10;
      this.keyCntForP95 = keyCnt * 95 / 100;
      this.keyCntForP99 = keyCnt * 99 / 100;
      this.preRequestTimeInNS = preRequestTimeInNS;
    }

    @Override
    public void onRecordDeserialized() {
      int currentKeyCnt = receivedKeyCnt.incrementAndGet();
      /**
       * Here is not short-circuiting because the key cnt for each percentile could be same if the total key count
       * is very small.
       */
      if (1 == currentKeyCnt) {
        stats.recordStreamingResponseTimeToReceiveFirstRecord(LatencyUtils.getLatencyInMS(preRequestTimeInNS));
      }
      if (keyCntForP50 == currentKeyCnt) {
        stats.recordStreamingResponseTimeToReceive50PctRecord(LatencyUtils.getLatencyInMS(preRequestTimeInNS));
      }
      if (keyCntForP90 == currentKeyCnt) {
        stats.recordStreamingResponseTimeToReceive90PctRecord(LatencyUtils.getLatencyInMS(preRequestTimeInNS));
      }
      if (keyCntForP95 == currentKeyCnt) {
        stats.recordStreamingResponseTimeToReceive95PctRecord(LatencyUtils.getLatencyInMS(preRequestTimeInNS));
      }
      if (keyCntForP99 == currentKeyCnt) {
        stats.recordStreamingResponseTimeToReceive99PctRecord(LatencyUtils.getLatencyInMS(preRequestTimeInNS));
      }
    }

    @Override
    public void onDeserializationCompletion(Optional<VeniceClientException> veniceException, int resultCnt,
        int duplicateEntryCnt) {
      handleMetricTrackingForStreamingCallback(stats, preRequestTimeInNS, veniceException, resultCnt, duplicateEntryCnt);
    }
  }

  @Override
  public void streamingBatchGet(Set<K> keys, StreamingCallback<K, V> callback) throws VeniceClientException {
    long preRequestTimeInNS = System.nanoTime();
    multiGetStreamingStats.recordRequestKeyCount(keys.size());
    super.streamingBatchGet(keys, new StatTrackingStreamingCallback<>(callback, multiGetStreamingStats, keys.size(), preRequestTimeInNS));
  }

  @Override
  public void compute(ComputeRequestWrapper computeRequestWrapper, Set<K> keys, Schema resultSchema,
      StreamingCallback<K, GenericRecord> callback, final long preRequestTimeInNS) throws VeniceClientException {
    computeStreamingStats.recordRequestKeyCount(keys.size());
    super.compute(computeRequestWrapper, keys, resultSchema,
        new StatTrackingStreamingCallback<>(callback, computeStreamingStats, keys.size(), preRequestTimeInNS),
        preRequestTimeInNS);
  }

  private static void handleMetricTrackingForStreamingCallback(ClientStats clientStats, long startTimeInNS,
      Optional<VeniceClientException> veniceException, int resultCnt, int duplicateEntryCnt) {
    double latency = LatencyUtils.getLatencyInMS(startTimeInNS);
    if (veniceException.isPresent()) {
      clientStats.recordUnhealthyRequest();
      clientStats.recordUnhealthyLatency(latency);

      if (veniceException.get() instanceof VeniceClientHttpException) {
        VeniceClientHttpException httpException = (VeniceClientHttpException)veniceException.get();
        clientStats.recordHttpRequest(httpException.getHttpStatus());
      }
    } else {
      emitRequestHealthyMetrics(clientStats, latency);
    }
    clientStats.recordSuccessRequestKeyCount(resultCnt);
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


  @Override
  public CompletableFuture<Map<K, GenericRecord>> compute(ComputeRequestWrapper computeRequestWrapper, Set<K> keys,
      Schema resultSchema, Optional<ClientStats> stats, final long preRequestTimeInNS) throws VeniceClientException {
    CompletableFuture<Map<K, GenericRecord>> innerFuture = super.compute(computeRequestWrapper, keys, resultSchema,
        stats, preRequestTimeInNS);
    computeStats.recordRequestKeyCount(keys.size());
    CompletableFuture<Map<K, GenericRecord>> statFuture = innerFuture.handle(
        (BiFunction<? super Map<K, GenericRecord>, Throwable, ? extends Map<K, GenericRecord>>) getStatCallback(computeStats, preRequestTimeInNS));
    return statFuture;
  }

  private static void emitRequestHealthyMetrics(ClientStats clientStats, double latency) {
    if (latency > TIMEOUT_IN_SECOND * Time.MS_PER_SECOND) {
      clientStats.recordUnhealthyRequest();
      clientStats.recordUnhealthyLatency(latency);
    } else {
      clientStats.recordHealthyRequest();
      clientStats.recordHealthyLatency(latency);
    }
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
      emitRequestHealthyMetrics(clientStats, latency);

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
