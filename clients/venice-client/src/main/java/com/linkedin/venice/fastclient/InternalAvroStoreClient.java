package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericReadComputeStoreClient;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseCompletableFuture;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.client.store.streaming.VeniceResponseMapImpl;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.exception.VeniceKeyCountLimitException;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * All the internal implementations of different tiers should extend this class.
 * This class adds in {@link RequestContext} object for the communication among different tiers.
 */

public abstract class InternalAvroStoreClient<K, V> implements AvroGenericReadComputeStoreClient<K, V> {
  public abstract ClientConfig getClientConfig();

  @Override
  public final boolean isProjectionFieldValidationEnabled() {
    return getClientConfig().isProjectionFieldValidationEnabled();
  }

  @Override
  public CompletableFuture<V> get(K key) throws VeniceClientException {
    return get(new GetRequestContext(), key);
  }

  protected abstract CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException;

  @Override
  public final CompletableFuture<Map<K, V>> batchGet(Set<K> keys) throws VeniceClientException {
    BatchGetRequestContext<K, V> batchGetRequestContext = new BatchGetRequestContext<>();
    // Since user has invoked batchGet directly, then we do not want to allow partial success
    batchGetRequestContext.isPartialSuccessAllowed = false;
    return batchGet(batchGetRequestContext, keys);
  }

  protected CompletableFuture<Map<K, V>> batchGet(BatchGetRequestContext<K, V> requestContext, Set<K> keys)
      throws VeniceClientException {
    // Since user has invoked batchGet directly, then we do not want to allow partial success
    requestContext.isPartialSuccessAllowed = false;
    return getClientConfig().useStreamingBatchGetAsDefault()
        ? batchGetUsingStreamingBatchGet(requestContext, keys)
        : batchGetUsingSingleGet(keys);
  }

  protected CompletableFuture<Map<K, V>> batchGetUsingStreamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys) throws VeniceClientException {
    CompletableFuture<Map<K, V>> resultFuture = new CompletableFuture<>();
    CompletableFuture<VeniceResponseMap<K, V>> streamingResultFuture = streamingBatchGet(requestContext, keys);

    streamingResultFuture.whenComplete((response, throwable) -> {
      if (throwable != null) {
        resultFuture.completeExceptionally(throwable);
      } else {
        resultFuture.complete(response);
      }
    });
    return resultFuture;
  }

  /**
   *  Leverage single-get implementation here:
   *  1. Looping through all keys and call get() for each of the keys
   *  2. Collect the replies and pass it to the caller
   *
   *  Transient change to support {@link ClientConfig#useStreamingBatchGetAsDefault()}
   */
  private CompletableFuture<Map<K, V>> batchGetUsingSingleGet(Set<K> keys) throws VeniceClientException {
    int maxAllowedKeyCntInBatchGetReq = getClientConfig().getMaxAllowedKeyCntInBatchGetReq();
    if (keys.isEmpty()) {
      return CompletableFuture.completedFuture(Collections.emptyMap());
    }
    int keyCnt = keys.size();
    if (keyCnt > maxAllowedKeyCntInBatchGetReq) {
      throw new VeniceKeyCountLimitException(
          getStoreName(),
          RequestType.MULTI_GET,
          keyCnt,
          maxAllowedKeyCntInBatchGetReq);
    }
    CompletableFuture<Map<K, V>> resultFuture = new CompletableFuture<>();
    Map<K, CompletableFuture<V>> valueFutures = new HashMap<>();
    keys.forEach(k -> {
      GetRequestContext getRequestContext = new GetRequestContext();
      getRequestContext.isTriggeredByBatchGet = true;
      valueFutures.put(k, (get(getRequestContext, k)));
    });
    CompletableFuture.allOf(valueFutures.values().toArray(new CompletableFuture[keyCnt]))
        .whenComplete(((aVoid, throwable) -> {
          if (throwable != null) {
            resultFuture.completeExceptionally(throwable);
          }
          Map<K, V> resultMap = new HashMap<>();
          valueFutures.forEach((k, f) -> {
            try {
              resultMap.put(k, f.get());
            } catch (Exception e) {
              resultFuture.completeExceptionally(
                  new VeniceClientException("Failed to complete future for key: " + k.toString(), e));
            }
          });
          resultFuture.complete(resultMap);
        }));

    return resultFuture;
  }

  @Override
  public final void streamingBatchGet(Set<K> keys, StreamingCallback<K, V> callback) throws VeniceClientException {
    BatchGetRequestContext<K, V> requestContext = new BatchGetRequestContext<>();
    streamingBatchGet(requestContext, keys, callback);
  }

  @Override
  public final CompletableFuture<VeniceResponseMap<K, V>> streamingBatchGet(Set<K> keys) throws VeniceClientException {
    BatchGetRequestContext<K, V> requestContext = new BatchGetRequestContext<>();
    return streamingBatchGet(requestContext, keys);
  }

  protected final CompletableFuture<VeniceResponseMap<K, V>> streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys) {
    int keySize = keys.size();
    // keys that do not exist in the storage nodes
    Queue<K> nonExistingKeys = new ConcurrentLinkedQueue<>();
    VeniceConcurrentHashMap<K, V> valueMap = new VeniceConcurrentHashMap<>();
    CompletableFuture<VeniceResponseMap<K, V>> streamingResponseFuture = new VeniceResponseCompletableFuture<>(
        () -> new VeniceResponseMapImpl<K, V>(valueMap, nonExistingKeys, false),
        keySize,
        Optional.empty());
    streamingBatchGet(requestContext, keys, new StreamingCallback<K, V>() {
      @Override
      public void onRecordReceived(K key, V value) {
        if (value == null) {
          nonExistingKeys.add(key);
        } else {
          valueMap.put(key, value);
        }
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        requestContext.complete();
        if (exception.isPresent()) {
          streamingResponseFuture.completeExceptionally(exception.get());
        } else {
          boolean isFullResponse = ((valueMap.size() + nonExistingKeys.size()) == keySize);
          streamingResponseFuture.complete(new VeniceResponseMapImpl<>(valueMap, nonExistingKeys, isFullResponse));
        }
      }
    });
    return streamingResponseFuture;
  }

  protected abstract void streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys,
      StreamingCallback<K, V> callback);

  @Override
  public final void compute(
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    ComputeRequestContext<K, V> requestContext = new ComputeRequestContext<>();
    if (!computeRequestWrapper.isRequestOriginallyStreaming()) {
      requestContext.isPartialSuccessAllowed = false;
    }
    compute(requestContext, computeRequestWrapper, keys, resultSchema, callback, preRequestTimeInNS);
  }

  protected abstract void compute(
      ComputeRequestContext<K, V> requestContext,
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException;

  @Override
  public final void computeWithKeyPrefixFilter(
      byte[] keyPrefix,
      ComputeRequestWrapper computeRequestWrapper,
      StreamingCallback<GenericRecord, GenericRecord> callback) {
    throw new VeniceClientException("'computeWithKeyPrefixFilter' is not supported by Venice Avro Store Client");
  }
}
