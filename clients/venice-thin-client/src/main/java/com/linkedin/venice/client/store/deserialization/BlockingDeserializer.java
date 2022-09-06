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
 * This {@link BatchDeserializer} does not do anything asynchronously. It simply deserializes
 * all records sequentially on the same thread that called it.
 */
public class BlockingDeserializer<E, K, V> extends BatchDeserializer<E, K, V> {
  public BlockingDeserializer(Executor deserializationExecutor, ClientConfig clientConfig) {
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
    Map<K, V> resultMap = new HashMap<>(keyList.size());
    for (E envelope: envelopes) {
      envelopeProcessor.accept(resultMap, envelope);
    }
    valueFuture.complete(resultMap);
    responseDeserializationComplete.report();
  }
}
