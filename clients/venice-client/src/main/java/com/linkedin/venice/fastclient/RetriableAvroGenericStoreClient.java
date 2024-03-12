package com.linkedin.venice.fastclient;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
  private final boolean longTailRetryEnabledForSingleGet;
  private final boolean longTailRetryEnabledForBatchGet;
  private final boolean longTailRetryEnabledForCompute;
  private final int longTailRetryThresholdForSingleGetInMicroSeconds;
  private final int longTailRetryThresholdForBatchGetInMicroSeconds;
  private final int longTailRetryThresholdForComputeInMicroSeconds;
  private final TimeoutProcessor timeoutProcessor;
  private static final Logger LOGGER = LogManager.getLogger(RetriableAvroGenericStoreClient.class);

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
  protected CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException {
    final CompletableFuture<V> originalRequestFuture = super.get(requestContext, key);
    if (!longTailRetryEnabledForSingleGet) {
      // if longTailRetry is not enabled for single get, simply return the original future
      return originalRequestFuture;
    }
    final CompletableFuture<V> retryFuture = new CompletableFuture<>();
    final CompletableFuture<V> finalFuture = new CompletableFuture<>();

    // create a retry task
    Runnable retryTask = () -> {
      super.get(requestContext, key).whenComplete((value, throwable) -> {
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
          // original request is faster
          requestContext.retryContext.retryWin = false;
        }
      } else {
        // Trigger the retry right away when receiving any error that's not a 429 otherwise try to cancel any scheduled
        // retry
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
  public void streamingBatchGet(
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
        BatchGetRequestContext::new,
        super::streamingBatchGet);
  }

  @Override
  public void compute(
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
        ComputeRequestContext::new,
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

  private <R extends MultiKeyRequestContext<K, V>, RESPONSE> void retryStreamingMultiKeyRequest(
      R requestContext,
      Set<K> keys,
      StreamingCallback<K, RESPONSE> callback,
      int longTailRetryThresholdInMicroSeconds,
      RequestContextConstructor<K, V, R> requestContextConstructor,
      StreamingRequestExecutor<K, V, R, RESPONSE> streamingRequestExecutor) throws VeniceClientException {
    R originalRequestContext = requestContextConstructor.construct(keys.size(), requestContext.isPartialSuccessAllowed);

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
          // Defensive code, do not trigger retry and complete the final request completion future if we encountered
          // 429.
          finalRequestCompletionFuture.completeExceptionally(throwable);
          return;
        }
        Set<K> pendingKeys = Collections.unmodifiableSet(pendingKeysFuture.keySet());
        R retryRequestContext =
            requestContextConstructor.construct(pendingKeys.size(), requestContext.isPartialSuccessAllowed);

        requestContext.retryContext.retryRequestContext = retryRequestContext;
        LOGGER.debug("Retrying {} incomplete keys", retryRequestContext.numKeysInRequest);
        // Prepare the retry context and track excluded routes on a per-partition basis
        retryRequestContext.setRoutesForPartitionMapping(originalRequestContext.getRoutesForPartitionMapping());

        streamingRequestExecutor.trigger(
            retryRequestContext,
            pendingKeys,
            getStreamingCallback(
                retryRequestContext,
                finalRequestCompletionFuture,
                savedException,
                pendingKeysFuture,
                null));
      } else {
        /** If there are no keys pending at this point , the onCompletion callback of the original
         request will be triggered. So no need to do anything.*/
        LOGGER.debug("Retry triggered with no incomplete keys. Ignoring.");
      }
    };

    TimeoutProcessor.TimeoutFuture scheduledRetryTask =
        timeoutProcessor.schedule(retryTask, longTailRetryThresholdInMicroSeconds, TimeUnit.MICROSECONDS);

    streamingRequestExecutor.trigger(
        originalRequestContext,
        keys,
        getStreamingCallback(
            originalRequestContext,
            finalRequestCompletionFuture,
            savedException,
            pendingKeysFuture,
            scheduledRetryTask));

    finalRequestCompletionFuture.whenComplete((ignore, finalException) -> {
      if (!scheduledRetryTask.isDone()) {
        scheduledRetryTask.cancel();
      }
      requestContext.complete();
      if (finalException == null) {
        callback.onCompletion(Optional.empty());
      } else {
        requestContext.setPartialResponseException(finalException);
        if (requestContext.isCompletedAcceptably()) {
          callback.onCompletion(Optional.empty());
        } else {
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
          exception.ifPresent(requestContext::setPartialResponseException);
          Optional<Throwable> exceptionToSave = requestContext.getPartialResponseException();
          if (!exceptionToSave.isPresent()) {
            finalRequestCompletionFuture.complete(null);
          } else {
            boolean shouldCompleteRequestFuture = false;
            if (scheduledRetryTask != null && isExceptionCausedByTooManyRequests(exceptionToSave.get())) {
              // Check if the exception is 429, if so cancel the retry if the retry task exists
              if (!scheduledRetryTask.isDone()) {
                scheduledRetryTask.cancel();
                shouldCompleteRequestFuture = true;
              }
            }
            // If we are able to set an exception, that means the other request did not have exception, so we continue.
            if (!savedException.compareAndSet(null, exceptionToSave.get())) {
              /* We are not able to set the exception , means there is already a saved exception.
               Since there was a saved exception and this request has also returned exception we can conclude that
               the parent request can be marked with exception. We select the original exception. This future is
               internal to this class and is allowed to complete exceptionally even for streaming APIs. */
              shouldCompleteRequestFuture = true;
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
    if (e instanceof VeniceClientHttpException) {
      VeniceClientHttpException clientHttpException = (VeniceClientHttpException) e;
      return clientHttpException.getHttpStatus() == VeniceClientRateExceededException.HTTP_TOO_MANY_REQUESTS;
    }
    return false;
  }

  interface RequestContextConstructor<K, V, R extends MultiKeyRequestContext<K, V>> {
    R construct(int numKeysInRequest, boolean isPartialSuccessAllowed);
  }

  interface StreamingRequestExecutor<K, V, R extends MultiKeyRequestContext<K, V>, RESPONSE> {
    void trigger(R retryRequestContext, Set<K> pendingKeys, StreamingCallback<K, RESPONSE> streamingCallback);
  }
}
