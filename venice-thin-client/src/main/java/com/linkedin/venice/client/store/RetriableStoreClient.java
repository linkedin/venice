package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.read.RequestType;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.httpclient.HttpStatus;


public class RetriableStoreClient<K, V> extends StatTrackingStoreClient<K, V> {
  private final boolean retryOnAllErrors;
  private final long retryBackOffInMs;
  private final int retryCount;

  public RetriableStoreClient(InternalAvroStoreClient<K, V> innerStoreClient, ClientConfig clientConfig) {
    super(innerStoreClient, clientConfig);
    retryOnAllErrors = clientConfig.isRetryOnAllErrorsEnabled();
    retryBackOffInMs = clientConfig.getRetryBackOffInMs();
    retryCount = clientConfig.getRetryCount();
  }

  /**
   * Adding retry logic on router error as this method returning the completion stage value.
   */
  @Override
  public CompletableFuture<V> get(K key) throws VeniceClientException {
    CompletableFuture<V> innerFuture = super.get(key);
    Supplier<CompletableFuture<V>> supplier = () -> super.get(key);

    return retryOnError(innerFuture, supplier, RequestType.SINGLE_GET);
  }

  /**
   * Skip retrying getRaw for now as it ues R2 thread pool, doing retry might cause deadlock.

  *@Override
  *public CompletableFuture<byte[]> getRaw(String requestPath, Optional<ClientStats> stats, long preRequestTimeInNS) {
   * return super.getRaw(requestPath, stats, preRequestTimeInNS);
   * Supplier<CompletableFuture<byte[]>> supplier = () -> super.getRaw(requestPath, stats, preRequestTimeInNS);
   * return innerFuture.handle((BiFunction<? super byte[], Throwable, ? extends byte[]>)retryOnRouterError(supplier));
  }
   */

  @Override
  public CompletableFuture<Map<K, GenericRecord>> compute(ComputeRequestWrapper computeRequestWrapper, Set<K> keys,
      Schema resultSchema, Optional<ClientStats> stats, long preRequestTimeInNS) throws VeniceClientException {
    CompletableFuture<Map<K, GenericRecord>> innerFuture = super.compute(computeRequestWrapper, keys, resultSchema,
        stats, preRequestTimeInNS);
    Supplier<CompletableFuture<Map<K, GenericRecord>>> supplier = () ->  super.compute(computeRequestWrapper, keys, resultSchema,
        stats, preRequestTimeInNS);

    return retryOnError(innerFuture, supplier, RequestType.COMPUTE);
  }

  /**
   *  Adding retry logic on router error as this method returning the completion stage value.
   */
  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) throws VeniceClientException {
    CompletableFuture<Map<K, V>> innerFuture = super.batchGet(keys);
    Supplier<CompletableFuture<Map<K,V>>> supplier = () -> super.batchGet(keys);

    return retryOnError(innerFuture, supplier, RequestType.MULTI_GET);
  }

  private <T> CompletableFuture<T> retryOnError(CompletableFuture<T> origFuture, Supplier<CompletableFuture<T>> supplier, RequestType requestType) {
    CompletableFuture<T> retryFuture = new CompletableFuture<>();

    origFuture.whenComplete((T val, Throwable throwable) -> {
      if (null != throwable) {
        int attempt = 0;
        Throwable retryThrowable = throwable;
        while (!retryFuture.isDone() && attempt < retryCount
            && isRetriableException(retryThrowable)) {
          attempt++;
          recordRetryCount(requestType);
          if (retryBackOffInMs > 0) {
            try {
              Thread.sleep(retryBackOffInMs);
            } catch (InterruptedException e) {
              retryFuture.completeExceptionally(e);
              return;
            }
          }
          try {
            T retryVal = supplier.get().get();
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
    });
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
