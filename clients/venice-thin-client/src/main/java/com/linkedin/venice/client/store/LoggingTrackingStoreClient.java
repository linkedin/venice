package com.linkedin.venice.client.store;

import static com.linkedin.venice.client.stats.BasicClientStats.getUnhealthyRequestHttpStatus;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.streaming.DelegatingTrackingCallback;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseCompletableFuture;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.client.store.streaming.VeniceResponseMapImpl;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
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
 * This class is a client wrapper that adds logging and tracking capabilities to store client operations.
 * The class is intended to be used when {@link ClientConfig#isStatTrackingEnabled()} is set to false. It will try to
 * log exceptions and high latency requests with rate limiting. It is not intended to be used in production environments
 * where performance and metric monitoring is critical.
 * <p>
 * This class extends {@link DelegatingStoreClient} to encapsulate an internal store client and
 * override its key operations such as {@code get}, {@code getRaw}, {@code batchGet}, and
 * {@code streamingBatchGet} with additional logging and tracking functionality. It captures
 * the latency of operations and logs warnings when the latency exceeds predefined thresholds.
 * <p>
 * It also handles exceptions by logging unhealthy requests and rethrowing the exceptions for
 * further handling.
 *
 * @param <K> the type of keys used by the store client
 * @param <V> the type of values returned by the store client
 */
public class LoggingTrackingStoreClient<K, V> extends DelegatingStoreClient<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(LoggingTrackingStoreClient.class);
  private static final int DEFAULT_SINGLE_GET_LOGGING_THRESHOLD_MS = 50;
  private static final int DEFAULT_BATCH_GET_LOGGING_THRESHOLD_MS = 500;
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  public LoggingTrackingStoreClient(InternalAvroStoreClient<K, V> innerStoreClient) {
    super(innerStoreClient);
  }

  @Override
  public CompletableFuture<V> get(K key) {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<V> innerFuture = super.get(key, Optional.empty(), startTimeInNS);
    return innerFuture.handle(
        (BiFunction<? super V, Throwable, ? extends V>) getLoggingCallback(super.getStoreName(), startTimeInNS));
  }

  @Override
  public CompletableFuture<byte[]> getRaw(String requestPath) {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<byte[]> innerFuture = super.getRaw(requestPath, Optional.empty(), startTimeInNS);
    return innerFuture.handle(
        (BiFunction<? super byte[], Throwable, ? extends byte[]>) getLoggingCallback(
            super.getStoreName(),
            startTimeInNS));
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) throws VeniceClientException {
    return internalBatchGet(keys);
  }

  @Override
  public void streamingBatchGet(Set<K> keys, StreamingCallback<K, V> callback) throws VeniceClientException {
    long preRequestTimeInNS = System.nanoTime();
    super.streamingBatchGet(keys, new LoggingStreamingCallback<>(getStoreName(), callback, preRequestTimeInNS));
  }

  @Override
  public CompletableFuture<VeniceResponseMap<K, V>> streamingBatchGet(Set<K> keys) throws VeniceClientException {
    Map<K, V> resultMap = new VeniceConcurrentHashMap<>(keys.size());
    Queue<K> nonExistingKeyList = new ConcurrentLinkedQueue<>();

    VeniceResponseCompletableFuture<VeniceResponseMap<K, V>> resultFuture = new VeniceResponseCompletableFuture<>(
        () -> new VeniceResponseMapImpl<>(resultMap, nonExistingKeyList, false),
        keys.size(),
        Optional.empty());
    streamingBatchGet(keys, super.getStreamingCallback(keys, resultMap, nonExistingKeyList, resultFuture));
    return resultFuture;
  }

  @Override
  public void compute(
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    super.compute(
        computeRequestWrapper,
        keys,
        resultSchema,
        new LoggingStreamingCallback<>(getStoreName(), callback, preRequestTimeInNS),
        preRequestTimeInNS);
  }

  public static <T> BiFunction<? super T, Throwable, ? extends T> getLoggingCallback(
      String storeName,
      long startTimeInNS) {
    return (T value, Throwable throwable) -> {
      double latency = LatencyUtils.getElapsedTimeFromNSToMS(startTimeInNS);
      if (throwable != null) {
        handleUnhealthyRequest(storeName, throwable, latency);
        handleStoreExceptionInternally(throwable);
      } else {
        if (latency > DEFAULT_SINGLE_GET_LOGGING_THRESHOLD_MS) {
          String logMessage =
              String.format("Receive high latency single get request for store: %s with latency: {}", storeName);
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(logMessage)) {
            LOGGER.warn(logMessage, latency);
          }
        }
      }
      return value;
    };
  }

  private static void handleUnhealthyRequest(String storeName, Throwable throwable, double latency) {
    int httpStatus = getUnhealthyRequestHttpStatus(throwable);
    String logMessage =
        String.format("Receive unhealthy request %s for store: %s with latency: {}", httpStatus, storeName);
    if (!REDUNDANT_LOGGING_FILTER.isRedundantException(logMessage)) {
      LOGGER.warn(logMessage, latency, throwable);
    }
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

  private static class LoggingStreamingCallback<K, V> extends DelegatingTrackingCallback<K, V> {
    private final long preRequestTimeInNS;
    private final String storeName;

    public LoggingStreamingCallback(String storeName, StreamingCallback<K, V> callback, long preRequestTimeInNS) {
      super(callback);
      this.storeName = storeName;
      this.preRequestTimeInNS = preRequestTimeInNS;
    }

    @Override
    public Optional<ClientStats> getStats() {
      return Optional.empty();
    }

    @Override
    public void onDeserializationCompletion(
        Optional<Exception> exception,
        int successKeyCount,
        int duplicateEntryCount) {
      handleLoggingTrackingForStreamingCallback(
          storeName,
          preRequestTimeInNS,
          exception,
          successKeyCount,
          duplicateEntryCount);
    }
  }

  private static void handleLoggingTrackingForStreamingCallback(
      String storeName,
      long startTimeInNS,
      Optional<Exception> exception,
      int successKeyCnt,
      int duplicateEntryCnt) {
    double latency = LatencyUtils.getElapsedTimeFromNSToMS(startTimeInNS);
    if (exception.isPresent()) {
      handleUnhealthyRequest(storeName, exception.get(), latency);
    } else {
      if (latency > DEFAULT_BATCH_GET_LOGGING_THRESHOLD_MS) {
        String logMessage = String.format(
            "Receive high latency streaming batch-get request for store: %s with latency: {} for success key count: {} and duplicate entry count: {}",
            storeName);
        if (!REDUNDANT_LOGGING_FILTER.isRedundantException(logMessage)) {
          LOGGER.warn(logMessage, latency, successKeyCnt, duplicateEntryCnt);
        }
      }
    }
  }
}
