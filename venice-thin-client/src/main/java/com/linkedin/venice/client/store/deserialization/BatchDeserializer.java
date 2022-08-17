package com.linkedin.venice.client.store.deserialization;

import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.stats.Reporter;
import com.linkedin.venice.client.store.ClientConfig;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;


/**
 * This API controls the behavior of the user payload deserialization phase of the
 * batch get response handling.
 */
public abstract class BatchDeserializer<E, K, V> {
  protected final Executor deserializationExecutor;
  protected final ClientConfig clientConfig;

  BatchDeserializer(final Executor deserializationExecutor, ClientConfig clientConfig) {
    this.deserializationExecutor = deserializationExecutor;
    this.clientConfig = clientConfig;
  }

  public abstract void deserialize(
      CompletableFuture<Map<K, V>> valueFuture,
      Iterable<E> records,
      List<K> keyList,
      BiConsumer<Map<K, V>, E> envelopeProcessor,
      Reporter responseDeserializationComplete,
      Optional<ClientStats> stats,
      long preResponseEnvelopeDeserialization);
}
