package com.linkedin.venice.fastclient;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is mostly used to trigger retry in the following scenarios:
 * 1. The original request latency exceeds the retry threshold.
 * 2. The original request fails.
 *
 * Currently, it only supports single-get.
 *
 * TODO:
 * 1. Limit the retry volume.
 * 2. Leverage some smart logic to avoid useless retry, such as retry triggered by heavy GC.
 * 3. Batch-get retry support.
 */
public class RetriableAvroGenericStoreClient<K, V> extends DelegatingAvroStoreClient<K, V> {
  private final boolean longTailRetryEnabledForSingleGet;
  private final boolean longTailRetryEnabledForBatchGet;
  private final int longTailRetryThresholdForSingleGetInMicroseconds;
  private final int longTailRetryThresholdForBatchGetInMicroseconds;
  private TimeoutProcessor timeoutProcessor;
  private static final Logger LOGGER = LogManager.getLogger(RetriableAvroGenericStoreClient.class);

  public RetriableAvroGenericStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig clientConfig) {
    super(delegate);
    if (!(clientConfig.isLongTailRetryEnabledForSingleGet() || clientConfig.isLongTailRetryEnabledForBatchGet())) {
      throw new VeniceException("Long tail retry is not enabled");
    }
    this.longTailRetryEnabledForSingleGet = clientConfig.isLongTailRetryEnabledForSingleGet();
    this.longTailRetryEnabledForBatchGet = clientConfig.isLongTailRetryEnabledForBatchGet();
    this.longTailRetryThresholdForSingleGetInMicroseconds =
        clientConfig.getLongTailRetryThresholdForSingleGetInMicroSeconds();
    this.longTailRetryThresholdForBatchGetInMicroseconds =
        clientConfig.getLongTailRetryThresholdForBatchGetInMicroSeconds();
  }

  enum RetryType {
    LONG_TAIL_RETRY, ERROR_RETRY
  }

  class RetryRunnable implements Runnable {
    private final GetRequestContext requestContext;
    private final RetryType retryType;
    private final Runnable retryTask;

    RetryRunnable(GetRequestContext requestContext, RetryType retryType, Runnable retryTask) {
      this.requestContext = requestContext;
      this.retryType = retryType;
      this.retryTask = retryTask;
    }

    @Override
    public void run() {
      switch (retryType) {
        case LONG_TAIL_RETRY:
          requestContext.longTailRetryRequestTriggered = true;
          break;
        case ERROR_RETRY:
          requestContext.errorRetryRequestTriggered = true;
          break;
        default:
          throw new VeniceClientException("Unknown retry type: " + retryType);
      }
      retryTask.run();
    }
  }

  /**
   * TODO:
   * Limit the retry volume: Even though retry for a single request is being scheduled at max twice (once
   * via scheduler (LONG_TAIL_RETRY) and once instant (ERROR_RETRY) if originalRequestFuture fails), but there
   * is no way to control the total allowed retry per node. It would be good to design some mechanism to make
   * it configurable, such as retry at most the slowest 5% of traffic, otherwise, too many retry requests
   * could cause cascading failure.
   */
  @Override
  protected CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException {
    if (!longTailRetryEnabledForSingleGet) {
      return super.get(requestContext, key);
    }
    final CompletableFuture<V> originalRequestFuture = super.get(requestContext, key);
    if (timeoutProcessor == null) {
      /**
       * Reuse the {@link TimeoutProcessor} from {@link com.linkedin.venice.fastclient.meta.InstanceHealthMonitor} to
       * reduce the thread usage.
       */
      timeoutProcessor = requestContext.instanceHealthMonitor.getTimeoutProcessor();
    }
    final CompletableFuture<V> retryFuture = new CompletableFuture<>();
    final CompletableFuture<V> finalFuture = new CompletableFuture<>();

    Runnable retryTask = () -> {
      super.get(requestContext, key).whenComplete((value, throwable) -> {
        if (throwable != null) {
          retryFuture.completeExceptionally(throwable);
        } else {
          retryFuture.complete(value);
          if (finalFuture.complete(value)) {
            requestContext.retryWin = true;
          }
        }
      });
    };
    // Setup long-tail retry task
    TimeoutProcessor.TimeoutFuture timeoutFuture = timeoutProcessor.schedule(
        new RetryRunnable(requestContext, RetryType.LONG_TAIL_RETRY, retryTask),
        longTailRetryThresholdForSingleGetInMicroseconds,
        TimeUnit.MICROSECONDS);

    originalRequestFuture.whenComplete((value, throwable) -> {
      if (throwable == null) {
        if (!timeoutFuture.isDone()) {
          timeoutFuture.cancel();
        }
        if (finalFuture.complete(value)) {
          // original request is faster
          requestContext.retryWin = false;
        }
      } else {
        // Trigger the retry right away when receiving any error
        if (!timeoutFuture.isDone()) {
          timeoutFuture.cancel();
          new RetryRunnable(requestContext, RetryType.ERROR_RETRY, retryTask).run();
        }
      }
    });

    CompletableFuture.allOf(originalRequestFuture, retryFuture).whenComplete((value, throwable) -> {
      /**
       * If any of the futures completes with a successful result, {@link finalFuture} should
       * have been completed with the successful result, so don't have to do anything else here.
       */
      if (originalRequestFuture.isCompletedExceptionally() && retryFuture.isCompletedExceptionally()) {
        /**
         * If none of the futures completes with a successful result, {@link finalFuture} must haven't completed
         * yet, so {@link finalFuture} will be completed with an exception thrown by either future.
         */
        finalFuture.completeExceptionally(throwable);
      }
    });

    return finalFuture;
  }

  @Override
  public void streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys,
      StreamingCallback<K, V> callback) throws VeniceClientException {
    if (!longTailRetryEnabledForBatchGet) {
      super.streamingBatchGet(requestContext, keys, callback);
    }
    /** Track the final completion of the request. It will be completed normally if
     1. the original requests calls onComplete with no exception
     2. the retry request calls onComplete with no exception
     3. all the keys have already been completed */
    CompletableFuture<Void> finalRequestCompletion = new CompletableFuture<>();
    /** Save the exception from onComplete of original or retry request. The final request would return exception only
       if both the original and retry request return an exception. */
    AtomicReference<Exception> savedException = new AtomicReference<>();
    /** Track all keys with a future. We remove the key when we receive value from either the original or the retry
     callback. Removal is thread safe so we will do it only once. We can then complete the future for that key */
    VeniceConcurrentHashMap<K, CompletableFuture<V>> pendingKeys = new VeniceConcurrentHashMap<>();
    for (K key: keys) {
      CompletableFuture<V> originalCompletion = new CompletableFuture<V>();
      originalCompletion.whenComplete((value, throwable) -> {
        callback.onRecordReceived(key, value);
      });
      pendingKeys.put(key, originalCompletion);
    }

    super.streamingBatchGet(
        requestContext,
        keys,
        getStreamingCallback(
            finalRequestCompletion,
            savedException,
            pendingKeys,
            requestContext.numberOfKeysCompletedInOriginalRequest));

    if (timeoutProcessor == null) {
      /** Reuse the {@link TimeoutProcessor} from {@link com.linkedin.venice.fastclient.meta.InstanceHealthMonitor} to
      reduce  thread usage */
      timeoutProcessor = requestContext.instanceHealthMonitor.getTimeoutProcessor();
    }

    Runnable retryTask = () -> { // Look at the remaining keys and setup completion
      if (!pendingKeys.isEmpty()) {
        requestContext.longTailRetryTriggered = true;
        requestContext.numberOfKeysSentInRetryRequest = pendingKeys.size();
        LOGGER.debug("Retrying {} incomplete keys ", pendingKeys.size());
        // Prepare the retry context and track excluded routes on a per partition basis
        BatchGetRequestContext<K, V> retryContext = new BatchGetRequestContext<>();
        retryContext.setRoutesForPartitionMapping(requestContext.getRoutesForPartitionMapping());
        super.streamingBatchGet(
            retryContext,
            Collections.unmodifiableSet(pendingKeys.keySet()),
            getStreamingCallback(
                finalRequestCompletion,
                savedException,
                pendingKeys,
                requestContext.numberOfKeysCompletedInRetryRequest));
      } else {
        /** If there are no keys pending at this point , the onCompletion callback of the original
         request will be triggered. So no need to do anything.*/
        LOGGER.debug("Retry triggered with no incomplete keys. Ignoring.");
      }
    };

    TimeoutProcessor.TimeoutFuture scheduledRetryTask =
        timeoutProcessor.schedule(retryTask, longTailRetryThresholdForBatchGetInMicroseconds, TimeUnit.MICROSECONDS);

    finalRequestCompletion.whenComplete((ignore, finalException) -> {
      if (!scheduledRetryTask.isDone()) {
        scheduledRetryTask.cancel();
      }
      if (finalException == null) {
        callback.onCompletion(Optional.empty());
      } else {
        callback.onCompletion(Optional.of(new VeniceClientException("Request failed with exception ", finalException)));
      }
    });
  }

  private StreamingCallback<K, V> getStreamingCallback(
      CompletableFuture<Void> finalRequestCompletion,
      AtomicReference<Exception> savedException,
      VeniceConcurrentHashMap<K, CompletableFuture<V>> pendingKeys,
      AtomicInteger successfulKeysCounter) {
    return new StreamingCallback<K, V>() {
      @Override
      public void onRecordReceived(K key, V value) {
        // Remove the key and if successful , mark it as complete
        CompletableFuture<V> removed = pendingKeys.remove(key);
        if (removed != null) {
          removed.complete(value);
          successfulKeysCounter.incrementAndGet();
        }
        if (pendingKeys.isEmpty() && !finalRequestCompletion.isDone()) { // No more pending keys so complete the
                                                                         // finalRequest
          finalRequestCompletion.complete(null);
        }
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        /* If the finalRequest is already complete. Ignore this.
        Otherwise check exception. If there is an exception, we still cannot complete the final request because other
        routes might still be pending. We just save the exception and move on.
        If there is no exception then we are surely done because this request was for all original keys.
         */
        if (!finalRequestCompletion.isDone()) {
          if (!exception.isPresent()) {
            finalRequestCompletion.complete(null);
          } else {
            // If we are able to set a exception , that means the other request did not have exception and we continue.
            if (!savedException.compareAndSet(null, exception.get())) {
              /* We are not able to set the exception , means there is already a saved exception.
               Since there was a saved exception and this request has also returned exception we can conclude that
               the parent request can be marked with exception. We select the original exception. */
              finalRequestCompletion.completeExceptionally(exception.get());
            }
          }
        }
      }
    };
  }
}
