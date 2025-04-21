package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.fastclient.meta.StoreLoadController;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;


/**
 * This class is used to control the load on the store from the client perspective.
 */
public class LoadControlledAvroGenericStoreClient<K, V> extends DelegatingAvroStoreClient<K, V> {
  public static final VeniceClientRateExceededException RATE_EXCEEDED_EXCEPTION =
      new VeniceClientRateExceededException("Store is overloaded and request got rejected by client");

  private final StoreLoadController loadController;

  public LoadControlledAvroGenericStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig clientConfig) {
    super(delegate, clientConfig);
    this.loadController = new StoreLoadController(clientConfig);
  }

  @Override
  protected CompletableFuture<V> get(GetRequestContext<K> requestContext, K key) throws VeniceClientException {
    requestContext.requestRejectionRatio = loadController.getRejectionRatio();
    if (loadController.shouldRejectRequest()) {
      loadController.recordRejectedRequest();
      requestContext.requestRejectedByLoadController = true;
      CompletableFuture<V> future = new CompletableFuture<>();
      future.completeExceptionally(RATE_EXCEEDED_EXCEPTION);
      return future;
    }

    CompletableFuture<V> future = super.get(requestContext, key);
    future.whenComplete((ignored, throwable) -> loadController.recordResponse(throwable));

    return future;
  }

  @Override
  protected void streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys,
      StreamingCallback<K, V> callback) throws VeniceClientException {
    requestContext.requestRejectionRatio = loadController.getRejectionRatio();

    if (loadController.shouldRejectRequest()) {
      loadController.recordRejectedRequest();
      requestContext.requestRejectedByLoadController = true;
      callback.onCompletion(Optional.of(RATE_EXCEEDED_EXCEPTION));
      return;
    }

    super.streamingBatchGet(requestContext, keys, new StreamingCallback<K, V>() {
      @Override
      public void onCompletion(Optional<Exception> throwable) {
        loadController.recordResponse(throwable.orElse(null));
        callback.onCompletion(throwable);
      }

      @Override
      public void onRecordReceived(K key, V value) {
        callback.onRecordReceived(key, value);
      }
    });
  }

  @Override
  protected void compute(
      ComputeRequestContext<K, V> requestContext,
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    requestContext.requestRejectionRatio = loadController.getRejectionRatio();

    if (loadController.shouldRejectRequest()) {
      loadController.recordRejectedRequest();
      requestContext.requestRejectedByLoadController = true;
      callback.onCompletion(Optional.of(RATE_EXCEEDED_EXCEPTION));
      return;
    }
    super.compute(
        requestContext,
        computeRequestWrapper,
        keys,
        resultSchema,
        new StreamingCallback<K, ComputeGenericRecord>() {
          @Override
          public void onCompletion(Optional<Exception> throwable) {
            loadController.recordResponse(throwable.orElse(null));
            callback.onCompletion(throwable);
          }

          @Override
          public void onRecordReceived(K key, ComputeGenericRecord value) {
            callback.onRecordReceived(key, value);
          }
        },
        preRequestTimeInNS);
  }

}
