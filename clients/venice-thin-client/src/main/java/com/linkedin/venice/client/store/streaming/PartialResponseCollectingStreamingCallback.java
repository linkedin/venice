package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;


public class PartialResponseCollectingStreamingCallback<K, V> implements StreamingCallback<K, V> {
  private final int numKeys;
  private final Map<K, V> resultMap;
  private final Queue<K> nonExistingKeyList;
  private final VeniceResponseCompletableFuture<VeniceResponseMap<K, V>> resultFuture;

  public PartialResponseCollectingStreamingCallback(Set<K> keys, ClientStats clientStats) {
    numKeys = keys.size();
    resultMap = new VeniceConcurrentHashMap<>(numKeys);
    nonExistingKeyList = new ConcurrentLinkedQueue<>();
    resultFuture = new VeniceResponseCompletableFuture<>(
        () -> new VeniceResponseMapImpl(resultMap, nonExistingKeyList, false),
        keys.size(),
        clientStats);
  }

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
      boolean isFullResponse = (resultMap.size() + nonExistingKeyList.size() == numKeys);
      resultFuture.complete(new VeniceResponseMapImpl(resultMap, nonExistingKeyList, isFullResponse));
    }
  }

  public VeniceResponseCompletableFuture<VeniceResponseMap<K, V>> getResultFuture() {
    return resultFuture;
  }
}
