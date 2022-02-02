package com.linkedin.venice.fastclient;

import com.linkedin.ddsstorage.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


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
  private final int longTailRetryThresholdForSingleGetInMicroseconds;
  private TimeoutProcessor timeoutProcessor;

  public RetriableAvroGenericStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig clientConfig) {
    super(delegate);
    if (!clientConfig.isLongTailRetryEnabledForSingleGet()) {
      throw new VeniceException("Long tail retry is not enabled");
    }
    this.longTailRetryThresholdForSingleGetInMicroseconds = clientConfig.getLongTailRetryThresholdForSingletGetInMicroSeconds();
  }

  enum RetryType {
    LONG_TAIL_RETRY,
    ERROR_RETRY;
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
   * 1. Limit the retry volume.
   */
  @Override
  protected CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException {
    final CompletableFuture<V> originalRequestFuture = super.get(requestContext, key);
    if (timeoutProcessor == null) {
      /**
       * Reuse the {@link TimeoutProcessor} from {@link com.linkedin.venice.fastclient.meta.InstanceHealthMonitor} to
       * reduce the thread usage.
       */
      timeoutProcessor = requestContext.instanceHealthMonitor.getTimeoutProcessor();
    }
    final CompletableFuture<V> retryFuture = new CompletableFuture();
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
      if (originalRequestFuture.isCompletedExceptionally() && retryFuture.isCompletedExceptionally()) {
        /**
         * If any of the futures completes with a successful result, {@link finalFuture} should be completed
         * with the successful result, and the following statement will have no effect.
         * If non of the futures completes with a successful result, {@link finalFuture} must haven't completed
         * yet, so {@link finalFuture} will be completed with an exception thrown by either future.
         */
        finalFuture.completeExceptionally(throwable);
      }
    });

    return finalFuture;
  }

}
