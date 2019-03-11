package com.linkedin.venice.client.store.deserialization;

import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.stats.Reporter;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;


/**
 * This {@link BatchDeserializer} forks off one new future for each record.
 */
public class OneFuturePerRecordDeserializer<E, K, V> extends BatchDeserializer<E, K, V> {
  OneFuturePerRecordDeserializer(Executor deserializationExecutor, ClientConfig clientConfig) {
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
    List<CompletableFuture<Void>> futureList = new LinkedList<>();
    Map<K, V> resultMap = new ConcurrentHashMap<>(keyList.size());
    AtomicLong earliestPreResponseRecordsDeserialization = new AtomicLong(Long.MAX_VALUE);
    long preResponseRecordsDeserializationSubmision = System.nanoTime();
    for (E envelope : envelopes) {
      futureList.add(CompletableFuture.runAsync(() -> {
        stats.ifPresent(clientStats -> clientStats.recordResponseRecordsDeserializationSubmissionToStartTime(
            LatencyUtils.getLatencyInMS(preResponseRecordsDeserializationSubmision)));
        long preResponseRecordsDeserialization = System.nanoTime();
        envelopeProcessor.accept(resultMap, envelope);
        earliestPreResponseRecordsDeserialization.accumulateAndGet(preResponseRecordsDeserialization,
            (left, right) -> Math.min(left, right));
      }, deserializationExecutor));
    }
    stats.ifPresent(clientStats -> clientStats.recordResponseEnvelopeDeserializationTime(LatencyUtils.getLatencyInMS(preResponseEnvelopeDeserialization)));
    CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()])).handle((v, throwable1) -> {
      if (null != throwable1) {
        valueFuture.completeExceptionally(throwable1);
      }
      valueFuture.complete(resultMap);
      responseDeserializationComplete.report();
      stats.ifPresent(clientStats -> clientStats.recordResponseRecordsDeserializationTime(LatencyUtils.getLatencyInMS(earliestPreResponseRecordsDeserialization.get())));
      return null;
    });
  }
}
