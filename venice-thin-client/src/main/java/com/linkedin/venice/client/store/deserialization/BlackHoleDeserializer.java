package com.linkedin.venice.client.store.deserialization;

import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.stats.Reporter;
import com.linkedin.venice.client.store.ClientConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;


/**
 * This {@link BatchDeserializer} does nothing, and always returns an empty list.
 *
 * The intent is to use this serializer in order to stress the Venice backend services
 * with more requests, while doing as little work as possible in the client. This can
 * also be used to test the transport-only portion of the client, without any
 * deserialization costs.
 */
public class BlackHoleDeserializer<E, K, V> extends BatchDeserializer<E, K, V> {
  public BlackHoleDeserializer(Executor deserializationExecutor, ClientConfig clientConfig) {
    super(deserializationExecutor, clientConfig);
  }

  @Override
  public void deserialize(
      CompletableFuture<Map<K, V>> valueFuture,
      Iterable<E> envelopes,
      List<K> keyList,
      BiConsumer<Map<K, V>, E> envelopeProcessor,
      Reporter responseDeserializationComplete,
      Optional<ClientStats> stats,
      long preResponseEnvelopeDeserialization) {
    valueFuture.complete(new HashMap<>(1));
    responseDeserializationComplete.report();
  }
}
