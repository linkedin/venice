package com.linkedin.venice.fastclient;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.RetryManager;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is mostly used to trigger retry in the following scenarios:
 * 1. The original request latency exceeds the retry threshold.
 * 2. The original request fails.
 *
 * TODO:
 * 1. Limit the retry volume.
 * 2. Leverage some smart logic to avoid useless retry, such as retry triggered by heavy GC.
 */
public class RetriableAvroGenericStoreClient<K, V> extends DelegatingAvroStoreClient<K, V> {
  private static final String FAST_CLIENT_RETRY_MANAGER_THREAD_PREFIX = "Fast-client-retry-manager-thread";
  private final boolean longTailRetryEnabledForSingleGet;
  private final boolean longTailRetryEnabledForBatchGet;
  private final boolean longTailRetryEnabledForCompute;
  private final int longTailRetryThresholdForSingleGetInMicroSeconds;
  private final int longTailRetryThresholdForBatchGetInMicroSeconds;
  private final int longTailRetryThresholdForComputeInMicroSeconds;
  private final TimeoutProcessor timeoutProcessor;
  private final ScheduledExecutorService retryManagerExecutorService =
      Executors.newScheduledThreadPool(1, new DaemonThreadFactory(FAST_CLIENT_RETRY_MANAGER_THREAD_PREFIX));
  /**
   * The long tail retry budget is only applied to long tail retries. If there were any exception that's not a 429 the
   * retry will be triggered without going through the long tail {@link com.linkedin.venice.meta.RetryManager}. If the retry budget is exhausted
   * then the retry task will do nothing and the request will either complete eventually (original future) or time out.
   */
  private RetryManager singleKeyLongTailRetryManager = null;
  private RetryManager multiKeyLongTailRetryManager = null;
  private static final Logger LOGGER = LogManager.getLogger(RetriableAvroGenericStoreClient.class);
  public static final String SINGLE_KEY_LONG_TAIL_RETRY_STATS_PREFIX = "single-key-long-tail-retry-manager-";
  public static final String MULTI_KEY_LONG_TAIL_RETRY_STATS_PREFIX = "multi-key-long-tail-retry-manager-";

  public RetriableAvroGenericStoreClient(
      InternalAvroStoreClient<K, V> delegate,
      ClientConfig clientConfig,
      TimeoutProcessor timeoutProcessor) {
    super(delegate, clientConfig);
    if (!(clientConfig.isLongTailRetryEnabledForSingleGet() || clientConfig.isLongTailRetryEnabledForBatchGet()
        || clientConfig.isLongTailRetryEnabledForCompute())) {
      throw new VeniceException("Long tail retry is not enabled");
    }
    this.longTailRetryEnabledForSingleGet = clientConfig.isLongTailRetryEnabledForSingleGet();
    this.longTailRetryEnabledForBatchGet = clientConfig.isLongTailRetryEnabledForBatchGet();
    this.longTailRetryEnabledForCompute = clientConfig.isLongTailRetryEnabledForCompute();
    this.longTailRetryThresholdForSingleGetInMicroSeconds =
        clientConfig.getLongTailRetryThresholdForSingleGetInMicroSeconds();
    this.longTailRetryThresholdForBatchGetInMicroSeconds =
        clientConfig.getLongTailRetryThresholdForBatchGetInMicroSeconds();
    this.longTailRetryThresholdForComputeInMicroSeconds =
        clientConfig.getLongTailRetryThresholdForComputeInMicroSeconds();
    this.timeoutProcessor = timeoutProcessor;
    if (longTailRetryEnabledForSingleGet && clientConfig.isRetryBudgetEnabled()) {
      this.singleKeyLongTailRetryManager = new RetryManager(
          clientConfig.getClusterStats().getMetricsRepository(),
          SINGLE_KEY_LONG_TAIL_RETRY_STATS_PREFIX + clientConfig.getStoreName(),
          clientConfig.getLongTailRetryBudgetEnforcementWindowInMs(),
          clientConfig.getRetryBudgetPercentage(),
          retryManagerExecutorService);
    }
    if (longTailRetryEnabledForBatchGet && clientConfig.isRetryBudgetEnabled()) {
      this.multiKeyLongTailRetryManager = new RetryManager(
          clientConfig.getClusterStats().getMetricsRepository(),
          MULTI_KEY_LONG_TAIL_RETRY_STATS_PREFIX + clientConfig.getStoreName(),
          clientConfig.getLongTailRetryBudgetEnforcementWindowInMs(),
          clientConfig.getRetryBudgetPercentage(),
          retryManagerExecutorService);
    }
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
      requestContext.retryContext = new GetRequestContext.RetryContext();
      switch (retryType) {
        case LONG_TAIL_RETRY:
          requestContext.retryContext.longTailRetryRequestTriggered = true;
          break;
        case ERROR_RETRY:
          requestContext.retryContext.errorRetryRequestTriggered = true;
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
   * via scheduler (LONG_TAIL_RETRY) and once instant (ERROR_RETRY) if originalRequestFuture fails), there
   * is no way to control the total allowed retry per node. It would be good to design some mechanism to make
   * it configurable, such as retry at most the slowest 5% of traffic, otherwise, too many retry requests
   * could cause cascading failure.
   */
  @Override
  protected CompletableFuture<V> get(GetRequestContext<K> requestContext, K key) throws VeniceClientException {
    final CompletableFuture<V> originalRequestFuture = super.get(requestContext, key);
    if (!longTailRetryEnabledForSingleGet) {
      // if longTailRetry is not enabled for single get, simply return the original future
      return originalRequestFuture;
    }
    if (singleKeyLongTailRetryManager != null) {
      singleKeyLongTailRetryManager.recordRequest();
    }
    final CompletableFuture<V> retryFuture = new CompletableFuture<>();
    final CompletableFuture<V> finalFuture = new CompletableFuture<>();

    AtomicReference<Throwable> savedException = new AtomicReference<>();
    // create a retry task
    Runnable retryTask = () -> {
      if (savedException.get() != null && isExceptionCausedByTooManyRequests(savedException.get())) {
        // Defensive code, abort retry if original request failed due to 429
        retryFuture.completeExceptionally(savedException.get());
        return;
      }
      if (savedException.get() != null || singleKeyLongTailRetryManager == null
          || singleKeyLongTailRetryManager.isRetryAllowed()) {
        GetRequestContext<K> retryRequestContext = requestContext.createRetryRequestContext();

        super.get(retryRequestContext, key).whenComplete((value, throwable) -> {
          if (throwable != null) {
            retryFuture.completeExceptionally(throwable);
          } else {
            retryFuture.complete(value);
            if (finalFuture.isDone() == false) {
              /**
               * Setting flag before completing {@link finalFuture} for the counters to be incremented properly.
               */
              requestContext.retryContext.retryWin = true;
              finalFuture.complete(value);
            }
          }
        });
      }
    };

    // Schedule the created task for long-tail retry
    TimeoutProcessor.TimeoutFuture timeoutFuture = timeoutProcessor.schedule(
        new RetryRunnable(requestContext, RetryType.LONG_TAIL_RETRY, retryTask),
        longTailRetryThresholdForSingleGetInMicroSeconds,
        TimeUnit.MICROSECONDS);

    originalRequestFuture.whenComplete((value, throwable) -> {
      if (throwable == null) {
        if (!timeoutFuture.isDone()) {
          timeoutFuture.cancel();
        }
        if (finalFuture.complete(value)) {
          // original request is faster: Resetting it even though the default is false to be accurate as
          // retryWin is set to true in the above block before completing the future, so there can be a race.
          requestContext.retryContext.retryWin = false;
        }
      } else {
        // Trigger the retry right away when receiving any error that's not a 429 otherwise try to cancel any scheduled
        // retry
        savedException.set(throwable);
        if (!timeoutFuture.isDone()) {
          timeoutFuture.cancel();
          if (!isExceptionCausedByTooManyRequests(throwable)) {
            new RetryRunnable(requestContext, RetryType.ERROR_RETRY, retryTask).run();
          }
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
  protected void streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys,
      StreamingCallback<K, V> callback) throws VeniceClientException {
    if (!longTailRetryEnabledForBatchGet) {
      // if longTailRetry is not enabled for batch get, simply return
      super.streamingBatchGet(requestContext, keys, callback);
      return;
    }

    retryStreamingMultiKeyRequest(
        requestContext,
        keys,
        callback,
        longTailRetryThresholdForBatchGetInMicroSeconds,
        (numKeysInRequest) -> requestContext.createRetryRequestContext(numKeysInRequest),
        super::streamingBatchGet);
  }

  @Override
  protected void compute(
      ComputeRequestContext<K, V> requestContext,
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    if (!longTailRetryEnabledForCompute) {
      // if longTailRetry is not enabled for compute, simply return
      super.compute(requestContext, computeRequestWrapper, keys, resultSchema, callback, preRequestTimeInNS);
      return;
    }

    retryStreamingMultiKeyRequest(
        requestContext,
        keys,
        callback,
        longTailRetryThresholdForComputeInMicroSeconds,
        (numKeysInRequest) -> requestContext.createRetryRequestContext(numKeysInRequest),
        (requestContextInternal, internalKeys, internalCallback) -> {
          super.compute(
              requestContextInternal,
              computeRequestWrapper,
              internalKeys,
              resultSchema,
              internalCallback,
              preRequestTimeInNS);
        });
  }

  @Override
  public void close() {
    retryManagerExecutorService.shutdownNow();
    super.close();
  }

  private <R extends MultiKeyRequestContext<K, V>, RESPONSE> void retryStreamingMultiKeyRequest(
      R requestContext,
      Set<K> keys,
      StreamingCallback<K, RESPONSE> callback,
      int longTailRetryThresholdInMicroSeconds,
      RequestContextConstructor<K, V, R> requestContextConstructor,
      StreamingRequestExecutor<K, V, R, RESPONSE> streamingRequestExecutor) throws VeniceClientException {
    requestContext.retryContext = new MultiKeyRequestContext.RetryContext<K, V>();

    /** Track the final completion of the request. It will be completed normally if
     1. the original requests calls onCompletion with no exception
     2. the retry request calls onCompletion with no exception
     3. all the keys have already been completed */
    CompletableFuture<Void> finalRequestCompletionFuture = new CompletableFuture<>();
    /** Save the exception from onCompletion of original or retry request. The final request would return exception only
     if both the original and retry request return an exception. */
    AtomicReference<Throwable> savedException = new AtomicReference<>();
    /** Track all keys with a future. We remove the key when we receive value from either the original or the retry
     callback. Removal is thread safe, so we will do it only once. We can then complete the future for that key */
    VeniceConcurrentHashMap<K, CompletableFuture<RESPONSE>> pendingKeysFuture = new VeniceConcurrentHashMap<>();
    for (K key: keys) {
      CompletableFuture<RESPONSE> originalCompletion = new CompletableFuture<>();
      originalCompletion.whenComplete((value, throwable) -> {
        callback.onRecordReceived(key, value);
      });
      pendingKeysFuture.put(key, originalCompletion);
    }

    Runnable retryTask = () -> { // Look at the remaining keys and setup completion
      if (!pendingKeysFuture.isEmpty()) {
        Throwable throwable = savedException.get();
        if (isExceptionCausedByTooManyRequests(throwable)) {
          // Defensive code, do not trigger retry and complete the final request completion
          // future if we encountered 429.
          finalRequestCompletionFuture.completeExceptionally(throwable);
          return;
        }
        if (throwable != null || multiKeyLongTailRetryManager == null
            || multiKeyLongTailRetryManager.isRetryAllowed(pendingKeysFuture.keySet().size())) {
          Set<K> pendingKeys = Collections.unmodifiableSet(pendingKeysFuture.keySet());
          // Prepare the retry context and track excluded routes on a per-partition basis
          R retryRequestContext = requestContextConstructor.construct(pendingKeys.size());

          requestContext.retryContext.retryRequestContext = retryRequestContext;
          LOGGER.debug("Retrying {} incomplete keys", retryRequestContext.numKeysInRequest);

          streamingRequestExecutor.trigger(
              retryRequestContext,
              pendingKeys,
              getStreamingCallback(
                  retryRequestContext,
                  finalRequestCompletionFuture,
                  savedException,
                  pendingKeysFuture,
                  null));
        }
      } else {
        /** If there are no keys pending at this point , the onCompletion callback of the original
         request will be triggered. So no need to do anything.*/
        LOGGER.debug("Retry triggered with no incomplete keys. Ignoring.");
      }
    };

    TimeoutProcessor.TimeoutFuture scheduledRetryTask =
        timeoutProcessor.schedule(retryTask, longTailRetryThresholdInMicroSeconds, TimeUnit.MICROSECONDS);

    /**
     * Retry for streaming multi-key request is done at the request level. This mean we will perform one retry for the
     * entire request for both errors and long tail to avoid retry storm. There are two behaviors with retry:
     * 1. If any of the route returned a too many requests 429 exception we will try our best to cancel all scheduled
     * retry and complete the final future. This means some routes could still be in progress, so we will assume those
     * will also soon fail with a 429.
     * 2. If no 429 exceptions are caught after longTailRetryThresholdInMicroSeconds when the retry task is running then
     * all incomplete keys whether due to long tail or errors (e.g. mis-routed) are retried.
     */
    streamingRequestExecutor.trigger(
        requestContext,
        keys,
        getStreamingCallback(
            requestContext,
            finalRequestCompletionFuture,
            savedException,
            pendingKeysFuture,
            scheduledRetryTask));
    if (multiKeyLongTailRetryManager != null) {
      multiKeyLongTailRetryManager.recordRequests(requestContext.numKeysInRequest);
    }

    finalRequestCompletionFuture.whenComplete((ignore, finalException) -> {
      if (!scheduledRetryTask.isDone()) {
        scheduledRetryTask.cancel();
      }
      requestContext.complete();

      // check and update the partial response exception before completing the callback
      // for the metrics to be updated accordingly
      if (finalException == null) {
        requestContext.setPartialResponseException(null);
        callback.onCompletion(Optional.empty());
      } else {
        R retryRequestContext = (R) requestContext.retryContext.retryRequestContext;
        if (requestContext.isCompletedAcceptably()
            && (retryRequestContext == null || retryRequestContext.isCompletedAcceptably())) {
          requestContext.setPartialResponseExceptionIfNull(finalException);
          callback.onCompletion(Optional.empty());
        } else {
          requestContext.setPartialResponseExceptionIfNull(finalException);
          callback
              .onCompletion(Optional.of(new VeniceClientException("Request failed with exception", finalException)));
        }
      }
    });
  }

  private <RESPONSE> StreamingCallback<K, RESPONSE> getStreamingCallback(
      MultiKeyRequestContext<K, V> requestContext,
      CompletableFuture<Void> finalRequestCompletionFuture,
      AtomicReference<Throwable> savedException,
      VeniceConcurrentHashMap<K, CompletableFuture<RESPONSE>> pendingKeysFuture,
      TimeoutProcessor.TimeoutFuture scheduledRetryTask) {
    return new StreamingCallback<K, RESPONSE>() {
      @Override
      public void onRecordReceived(K key, RESPONSE value) {
        // Remove the key and if successful , mark it as complete
        CompletableFuture<RESPONSE> removed = pendingKeysFuture.remove(key);
        if (removed != null) {
          removed.complete(value); // This will invoke the onRecordReceived callback of the original request
          requestContext.numKeysCompleted.incrementAndGet();
        }
        if (pendingKeysFuture.isEmpty() && !finalRequestCompletionFuture.isDone()) {
          // No more pending keys, so complete the finalRequest
          finalRequestCompletionFuture.complete(null);
        }
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        /* If the finalRequest is already complete. Ignore this.
        Otherwise, check exception. If there is an exception, we still cannot complete the final request because other
        routes might still be pending. We just save the exception and move on.
        If there is no exception then we are surely done because this request was for all original keys.
         */
        if (!finalRequestCompletionFuture.isDone()) {
          exception.ifPresent(requestContext::setPartialResponseExceptionIfNull);
          Optional<Throwable> exceptionToSave = requestContext.getPartialResponseException();
          if (!exceptionToSave.isPresent()) {
            finalRequestCompletionFuture.complete(null);
          } else {
            /* If we are able to set an exception, that means the other request did not have exception, so we continue.
               If we are not able to set the exception , means there is already a saved exception.
               Since there was a saved exception and this request has also returned exception we can conclude that
               the parent request can be marked with exception. We select the original exception. This future is
               internal to this class and is allowed to complete exceptionally even for streaming APIs. */
            boolean shouldCompleteRequestFuture = !savedException.compareAndSet(null, exceptionToSave.get());
            if (scheduledRetryTask != null && isExceptionCausedByTooManyRequests(exceptionToSave.get())) {
              // Check if the exception is 429, if so cancel the retry if the retry task exists
              if (!scheduledRetryTask.isDone()) {
                scheduledRetryTask.cancel();
                shouldCompleteRequestFuture = true;
              }
            }

            if (shouldCompleteRequestFuture) {
              finalRequestCompletionFuture.completeExceptionally(exceptionToSave.get());
            }
          }
        }
      }
    };
  }

  private boolean isExceptionCausedByTooManyRequests(Throwable e) {
    return ExceptionUtils.recursiveClassEquals(e, VeniceClientRateExceededException.class);
  }

  interface RequestContextConstructor<K, V, R extends MultiKeyRequestContext<K, V>> {
    R construct(int numKeysInRequest);
  }

  interface StreamingRequestExecutor<K, V, R extends MultiKeyRequestContext<K, V>, RESPONSE> {
    void trigger(R retryRequestContext, Set<K> pendingKeys, StreamingCallback<K, RESPONSE> streamingCallback);
  }
}
