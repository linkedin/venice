package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.httpclient.HttpStatus;


/**
 * TODO: make retry work for compute request.
 */
public class RetriableStoreClient<K, V> extends DelegatingStoreClient<K, V> {
  /**
   * We need to execute the retry in a different thread pool than the one being used by the response callback.
   * If we don't use another thread pool, the de-serialization thread pool will be used to initiate
   * the retry request, which needs to submit the deserialization request for the retry response to the same thread
   * pool, which can cause a deadlock issue.
   * This issue is similar to the one around {@link AbstractAvroStoreClient#DESERIALIZATION_EXECUTOR}.
   */
  private static Executor RETRY_EXECUTOR;

  private final InternalAvroStoreClient innerStoreClient;
  private final boolean retryOnAllErrors;
  private final long retryBackOffInMs;
  private final int retryCount;
  private final boolean enableStatTracking;
  private final Executor retryExecutor;

  public static synchronized Executor getDefaultRetryExecutor() {
    if (RETRY_EXECUTOR == null) {
      // Half of process number of threads should be good enough, minimum 2
      int threadNum = Math.max(Runtime.getRuntime().availableProcessors() / 2, 2);

      RETRY_EXECUTOR = Executors.newFixedThreadPool(threadNum, new DaemonThreadFactory("Venice-Request-Retry"));
    }

    return RETRY_EXECUTOR;
  }

  public RetriableStoreClient(InternalAvroStoreClient<K, V> innerStoreClient, ClientConfig clientConfig) {
    super(innerStoreClient);
    this.innerStoreClient = innerStoreClient;
    this.retryOnAllErrors = clientConfig.isRetryOnAllErrorsEnabled();
    this.retryBackOffInMs = clientConfig.getRetryBackOffInMs();
    this.retryCount = clientConfig.getRetryCount();
    this.enableStatTracking = clientConfig.isStatTrackingEnabled();
    Executor configuredRetryExecutor = clientConfig.getRetryExecutor();
    if (clientConfig.getDeserializationExecutor() != null && configuredRetryExecutor != null
        && clientConfig.getDeserializationExecutor() == configuredRetryExecutor) {
      throw new VeniceClientException(
          "RetryExecutor needs to be different from DeserializationExecutor to avoid deadlock issues");
    }
    this.retryExecutor = configuredRetryExecutor != null ? configuredRetryExecutor : getDefaultRetryExecutor();
  }

  /**
   * Adding retry logic on router error as this method returning the completion stage value.
   */
  @Override
  public CompletableFuture<V> get(K key) throws VeniceClientException {
    Supplier<CompletableFuture<V>> supplier = () -> super.get(key);
    return executeWithRetry(supplier, RequestType.SINGLE_GET);
  }

  /**
   *  Adding retry logic on router error as this method returning the completion stage value.
   */
  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) throws VeniceClientException {
    Supplier<CompletableFuture<Map<K, V>>> supplier = () -> super.batchGet(keys);
    return executeWithRetry(supplier, RequestType.MULTI_GET);
  }

  private <T> CompletableFuture<T> executeWithRetry(Supplier<CompletableFuture<T>> supplier, RequestType requestType) {
    CompletableFuture<T> retryFuture = new CompletableFuture<>();

    supplier.get().whenCompleteAsync((T val, Throwable throwable) -> {
      if (throwable != null) {
        int attempt = 0;
        Throwable retryThrowable = throwable;
        while (!retryFuture.isDone() && attempt < retryCount && isRetriableException(retryThrowable)) {
          attempt++;
          if (enableStatTracking) {
            ((StatTrackingStoreClient) innerStoreClient).recordRetryCount(requestType);
          }
          if (retryBackOffInMs > 0) {
            try {
              Thread.sleep(retryBackOffInMs);
            } catch (InterruptedException e) {
              retryFuture.completeExceptionally(e);
              return;
            }
          }
          try {
            T retryVal = supplier.get().get(10, TimeUnit.SECONDS);
            retryFuture.complete(retryVal);
            retryThrowable = null;
            break;
          } catch (Throwable t) {
            retryThrowable = t;
          }
        }
        if (retryThrowable != null) {
          retryFuture.completeExceptionally(retryThrowable);
        }
      } else {
        retryFuture.complete(val);
      }
    }, this.retryExecutor);
    return retryFuture;
  }

  /**
   * Checks if the exception can be retried. Currently it checks for Router health error(Http code: SC_SERVICE_UNAVAILABLE)
   * or always return true if ClientConfig is configured to consider all errors retriable (except InterruptedException).
   * @param throwable type of Throwable that can be retried.
   * @return true if it can be retried.
   */
  private boolean isRetriableException(Throwable throwable) {
    if (throwable instanceof InterruptedException) {
      // do not retry InterruptedException
      return false;
    }
    if (retryOnAllErrors) {
      return true;
    }
    Throwable innerException;
    if (throwable instanceof CompletionException || throwable instanceof ExecutionException) {
      innerException = throwable.getCause();
    } else {
      innerException = throwable;
    }
    if (innerException instanceof VeniceClientHttpException) {
      VeniceClientHttpException exception = (VeniceClientHttpException) innerException;
      return exception.getHttpStatus() == HttpStatus.SC_SERVICE_UNAVAILABLE;
    }
    return false;
  }
}
